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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitList splits a comma-separated flag, dropping empty items.
func splitList(v string) []string {
	var out []string
	for item := range strings.SplitSeq(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func main() {
	var (
		listen, metricsListen, natsURL, tokenFile, namespace, allowed, bucket, defaultImage, apiVersion string
		defaultTimeout, maxTimeout, taskTTL, runTTL, scheduleRetention                                  time.Duration
		maxInFlight, maxStreams                                                                         int
		missionsEnabled, allowSkillGeneration, runsEnabled, hooksEnabled, saAuth                        bool
		schedulesEnabled, scheduleRunsEnabled                                                           bool
		saAudience, saNamespaces                                                                        string
		maxBudgetUSD, hookRate                                                                          float64
		maxMissionTTL, maxMissionTimeout, hookBurst                                                     int
		hookMaxBody                                                                                     int64
	)
	flag.StringVar(&listen, "listen", ":8080", "Address the HTTP API and MCP endpoint listen on.")
	flag.StringVar(&metricsListen, "metrics-listen", "",
		"Address of the unauthenticated Prometheus /metrics endpoint (e.g. :8081). Empty disables it.")
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
	flag.StringVar(&apiVersion, "api-version", gateway.DefaultAPIVersion,
		"Version reported by the OpenAPI document (the chart's appVersion).")
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
	flag.BoolVar(&schedulesEnabled, "schedules", true,
		"Serve the schedule endpoints (needs RBAC to get, list, watch and patch Triggers).")
	flag.BoolVar(&scheduleRunsEnabled, "schedule-runs", true,
		"Serve a schedule's run history and results, GET /v1/schedules/{namespace}/{name}/runs "+
			"(needs RBAC to get Triggers, get and list Chains).")
	flag.DurationVar(&scheduleRetention, "schedule-result-retention", gateway.DefaultScheduleResultRetention,
		"How long the results of a schedule's runs stay readable: the task record TTL is raised to it.")
	flag.BoolVar(&hooksEnabled, "hooks", true,
		"Serve POST /v1/hooks/{namespace}/{trigger} for webhook Triggers (needs RBAC to get Triggers and "+
			"Secrets labelled ai.roundtable.io/webhook-secret=true).")
	flag.Int64Var(&hookMaxBody, "hook-max-body-bytes", 1<<20, "Largest webhook body accepted.")
	flag.Float64Var(&hookRate, "hook-rate", 2, "Webhook deliveries per second each Trigger accepts on average.")
	flag.IntVar(&hookBurst, "hook-burst", 20, "Webhook deliveries each Trigger accepts at once.")
	flag.BoolVar(&saAuth, "service-account-auth", false,
		"Also accept Kubernetes ServiceAccount tokens, validated with a TokenReview (needs create on "+
			"tokenreviews.authentication.k8s.io).")
	flag.StringVar(&saAudience, "service-account-audience", gateway.DefaultServiceAccountAudience,
		"Audience a ServiceAccount token must be issued for.")
	flag.StringVar(&saNamespaces, "service-account-namespaces", "*",
		"Comma-separated namespaces whose ServiceAccounts may call the gateway (\"*\" for any).")
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
	auth.Log = log.WithName("auth")

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

	if saAuth {
		auth.ServiceAccounts = &gateway.ServiceAccountAuth{
			Reviewer:          gateway.KubeTokenReviewer{Client: k8s},
			Audience:          saAudience,
			AllowedNamespaces: splitList(saNamespaces),
		}
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
	bus, err := gateway.NewNATSBus(setupCtx, nc, bucket, gateway.EffectiveTaskTTL(taskTTL, scheduleRetention))
	cancelSetup()
	if err != nil {
		log.Error(err, "Unable to set up NATS JetStream")
		os.Exit(1)
	}

	allowedNS := splitList(allowed)
	svc := gateway.NewService(&gateway.Resolver{Client: k8s}, bus, gateway.Options{
		Namespace:          namespace,
		AllowedNamespaces:  allowedNS,
		DefaultTimeout:     defaultTimeout,
		MaxTimeout:         maxTimeout,
		MaxInFlight:        maxInFlight,
		MaxStreams:         maxStreams,
		DefaultKnightImage: defaultImage,
		Version:            apiVersion,
		Missions: gateway.MissionOptions{
			MaxBudgetUSD:         maxBudgetUSD,
			MaxTTLSeconds:        int32(max(min(maxMissionTTL, math.MaxInt32), 0)),
			MaxTimeoutSeconds:    int32(max(min(maxMissionTimeout, math.MaxInt32), 0)),
			AllowSkillGeneration: allowSkillGeneration,
		},
		Runs:  gateway.RunOptions{TTL: runTTL},
		Hooks: gateway.HookOptions{MaxBodyBytes: hookMaxBody, Rate: hookRate, Burst: hookBurst},
	}, log)
	if missionsEnabled {
		svc.Missions = k8s
	}
	if runsEnabled {
		svc.Runs = k8s
	}
	if schedulesEnabled {
		svc.Schedules = k8s
	}
	if scheduleRunsEnabled {
		svc.ScheduleReader = k8s
	}
	if hooksEnabled {
		svc.Hooks = k8s
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
	if metricsListen != "" {
		// A separate port: /metrics carries no bearer token, so it stays off
		// the API listener.
		mm := http.NewServeMux()
		mm.Handle("GET /metrics", promhttp.HandlerFor(crmetrics.Registry, promhttp.HandlerOpts{}))
		msrv := &http.Server{Addr: metricsListen, Handler: mm, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error(err, "Metrics server failed")
				os.Exit(1)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = msrv.Shutdown(shutdownCtx)
		}()
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("Starting gateway", "listen", listen, "missions", missionsEnabled, "runs", runsEnabled, "hooks", hooksEnabled,
		"scheduleRuns", scheduleRunsEnabled,
		"serviceAccountAuth", saAuth, "serviceAccountAudience", saAudience,
		"schedules", schedulesEnabled,
		"namespace", namespace, "nats", natsURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(err, "Gateway server failed")
		os.Exit(1)
	}
}
