/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command gateway is the Round Table invoke gateway: a synchronous HTTP API
// and MCP endpoint in front of the knights' NATS task queues.
package main

import (
	"context"
	"errors"
	"flag"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		listen, natsURL, tokenFile, namespace, allowed, bucket, defaultImage string
		defaultTimeout, maxTimeout, taskTTL, runTTL                          time.Duration
		maxInFlight, maxStreams                                              int
		missionsEnabled, allowSkillGeneration, runsEnabled                   bool
		maxBudgetUSD                                                         float64
		maxMissionTTL, maxMissionTimeout                                     int
	)
	flag.StringVar(&listen, "listen", ":8080", "Address the HTTP API and MCP endpoint listen on.")
	flag.StringVar(&natsURL, "nats-url", envOr("NATS_URL", "nats://nats.database.svc:4222"), "NATS server URL.")
	flag.StringVar(&tokenFile, "token-file", "/var/run/secrets/roundtable/gateway/token",
		"File holding the accepted bearer token(s), one per line.")
	flag.StringVar(&namespace, "namespace", envOr("POD_NAMESPACE", "default"), "Default namespace for knights.")
	flag.StringVar(&allowed, "allowed-namespaces", "",
		"Comma-separated extra namespaces callers may target (\"*\" for any). The default namespace is always allowed.")
	flag.StringVar(&bucket, "task-bucket", "roundtable_gateway_tasks", "NATS KV bucket for task records.")
	flag.DurationVar(&taskTTL, "task-ttl", 7*24*time.Hour, "How long task records are kept.")
	flag.DurationVar(&defaultTimeout, "default-timeout", 60*time.Second,
		"How long invoke waits when the caller doesn't say.")
	flag.DurationVar(&maxTimeout, "max-timeout", 30*time.Minute, "Upper bound on the wait a caller may request.")
	flag.IntVar(&maxInFlight, "max-in-flight", 64, "Maximum concurrently waiting invocations (0 = unlimited).")
	flag.IntVar(&maxStreams, "max-streams", 64, "Maximum concurrently open event streams (0 = unlimited).")
	flag.StringVar(&defaultImage, "default-knight-image", os.Getenv("DEFAULT_KNIGHT_IMAGE"),
		"The operator's DEFAULT_KNIGHT_IMAGE, used to infer knight runtimes for model validation.")
	flag.BoolVar(&missionsEnabled, "missions", true,
		"Serve the mission endpoints (needs RBAC to create, read and delete Missions).")
	flag.Float64Var(&maxBudgetUSD, "mission-max-budget-usd", 10, "Upper bound on a mission's costBudgetUSD.")
	flag.IntVar(&maxMissionTTL, "mission-max-ttl-seconds", 86400, "Upper bound on a mission's ttlSeconds.")
	flag.IntVar(&maxMissionTimeout, "mission-max-timeout-seconds", 21600, "Upper bound on a mission's timeoutSeconds.")
	flag.BoolVar(&allowSkillGeneration, "mission-allow-skill-generation", false,
		"Let callers set planner.allowSkillGeneration on missions.")
	flag.BoolVar(&runsEnabled, "runs", true,
		"Serve the chain-run endpoints (needs RBAC to create, read and delete Chains).")
	flag.DurationVar(&runTTL, "run-ttl", 24*time.Hour, "How long a finished chain run started by the gateway is kept.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("gateway")

	auth, err := gateway.NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		log.Error(err, "Bearer token is required; refusing to start without one")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error(err, "Unable to build scheme")
		os.Exit(1)
	}
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		log.Error(err, "Unable to build scheme")
		os.Exit(1)
	}
	k8s, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		log.Error(err, "Unable to create Kubernetes client")
		os.Exit(1)
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("roundtable-gateway"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Error(err, "Unable to connect to NATS", "url", natsURL)
		os.Exit(1)
	}
	defer nc.Close()
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	bus, err := gateway.NewNATSBus(setupCtx, nc, bucket, taskTTL)
	cancelSetup()
	if err != nil {
		log.Error(err, "Unable to set up NATS JetStream")
		os.Exit(1)
	}

	var allowedNS []string
	for ns := range strings.SplitSeq(allowed, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			allowedNS = append(allowedNS, ns)
		}
	}
	svc := gateway.NewService(&gateway.Resolver{Client: k8s}, bus, gateway.Options{
		Namespace:          namespace,
		AllowedNamespaces:  allowedNS,
		DefaultTimeout:     defaultTimeout,
		MaxTimeout:         maxTimeout,
		MaxInFlight:        maxInFlight,
		MaxStreams:         maxStreams,
		DefaultKnightImage: defaultImage,
		Missions: gateway.MissionOptions{
			MaxBudgetUSD:         maxBudgetUSD,
			MaxTTLSeconds:        int32(max(min(maxMissionTTL, math.MaxInt32), 0)),
			MaxTimeoutSeconds:    int32(max(min(maxMissionTimeout, math.MaxInt32), 0)),
			AllowSkillGeneration: allowSkillGeneration,
		},
		Runs: gateway.RunOptions{TTL: runTTL},
	}, log)
	if missionsEnabled {
		svc.Missions = k8s
	}
	if runsEnabled {
		svc.Runs = k8s
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !nc.IsConnected() {
			http.Error(w, "NATS not connected", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", gateway.Handler(svc, auth))
	srv := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// No ReadTimeout or WriteTimeout: invoke, run and MCP calls
		// legitimately stay open for up to --max-timeout. ReadHeaderTimeout
		// only bounds reading the request headers and IdleTimeout only the
		// gap between requests on a kept-alive connection, so neither cuts a
		// long response.
		IdleTimeout: 2 * time.Minute,
	}
	// Open event streams never end by themselves: close them first or
	// Shutdown waits for them until its deadline.
	srv.RegisterOnShutdown(svc.CloseStreams)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if runsEnabled {
		// Delete the gateway's finished run copies once they are past --run-ttl.
		go svc.RunJanitor(ctx, min(max(runTTL/4, time.Minute), 10*time.Minute))
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("Starting gateway", "listen", listen, "missions", missionsEnabled, "runs", runsEnabled,
		"namespace", namespace, "nats", natsURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(err, "Gateway server failed")
		os.Exit(1)
	}
}
