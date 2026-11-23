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
		defaultTimeout, maxTimeout, taskTTL                                  time.Duration
		maxInFlight                                                          int
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
	flag.DurationVar(&maxTimeout, "max-timeout", 10*time.Minute, "Upper bound on the wait a caller may request.")
	flag.IntVar(&maxInFlight, "max-in-flight", 64, "Maximum concurrently waiting invocations (0 = unlimited).")
	flag.StringVar(&defaultImage, "default-knight-image", os.Getenv("DEFAULT_KNIGHT_IMAGE"),
		"The operator's DEFAULT_KNIGHT_IMAGE, used to infer knight runtimes for model validation.")
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
	bus, err := gateway.NewNATSBus(nc, bucket, taskTTL)
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
		DefaultKnightImage: defaultImage,
	}, log)

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
		// No WriteTimeout: invoke and MCP calls legitimately stay open for
		// up to --max-timeout.
		IdleTimeout: 2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("Starting gateway", "listen", listen, "namespace", namespace, "nats", natsURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(err, "Gateway server failed")
		os.Exit(1)
	}
}
