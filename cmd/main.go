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

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	_ "time/tzdata" // embed IANA tz database: the distroless base image has none

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/roundtable-k8s/roundtable/internal/telemetry"

	// Register custom Prometheus metrics
	_ "github.com/roundtable-k8s/roundtable/pkg/metrics"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/controller"
	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	"github.com/roundtable-k8s/roundtable/internal/firetrigger"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
	"github.com/roundtable-k8s/roundtable/internal/github"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	"github.com/roundtable-k8s/roundtable/internal/mission"
	notifypkg "github.com/roundtable-k8s/roundtable/internal/notify"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
	// +kubebuilder:scaffold:imports
)

// version is the operator version reported as service.version; set with -ldflags "-X main.version=...".
var version = "dev"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(aiv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	// manager fire-trigger ...: the one gateway call a schedule Trigger's
	// CronJob pod makes. No manager, no Kubernetes client.
	if len(os.Args) > 1 && os.Args[1] == firetrigger.Command {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		code := firetrigger.Run(ctx, os.Args[2:], os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}

	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var leTimings leaderElectionTimings
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var sandboxBackendMode string
	var kedaMode string
	var natsMonitoringEndpoint string
	var githubAPIURL string
	var providerMaxRetries int
	var chainMaxConcurrentReconciles int
	var maxConcurrentRollouts int
	var missionResultsTTL time.Duration
	var triggerTaskBucket string
	var triggerTaskTTL, scheduleResultRetention time.Duration
	var triggerSchedule controller.TriggerScheduleConfig
	var triggerPullPolicy, triggerPullSecrets, triggerNodeSelector, triggerTolerations string
	var chainScheduleMode string
	flag.StringVar(&chainScheduleMode, "chain-schedule-mode", string(controller.ChainScheduleModeInProcess),
		"How Chain.spec.schedule runs: inprocess (the operator's cron runs the Chain itself) or trigger "+
			"(an owned schedule Trigger <chain>-schedule and its CronJob start a copy of the Chain per fire).")
	flag.StringVar(&triggerSchedule.Image, "trigger-image", os.Getenv("OPERATOR_IMAGE"),
		"Image of the pod a scheduled Trigger's CronJob runs (the operator's own image; it runs "+
			"`/manager fire-trigger`). Empty leaves scheduled Triggers not Ready.")
	flag.StringVar(&triggerPullPolicy, "trigger-image-pull-policy", "",
		"Image pull policy of a scheduled Trigger's firing pod (empty = the cluster default).")
	flag.StringVar(&triggerPullSecrets, "trigger-image-pull-secrets", "",
		"Comma-separated imagePullSecrets names of a scheduled Trigger's firing pod; must exist in the Trigger's namespace.")
	flag.StringVar(&triggerNodeSelector, "trigger-node-selector", "",
		"JSON object: nodeSelector of a scheduled Trigger's firing pod.")
	flag.StringVar(&triggerTolerations, "trigger-tolerations", "",
		"JSON array of tolerations of a scheduled Trigger's firing pod.")
	flag.StringVar(&triggerSchedule.PriorityClassName, "trigger-priority-class-name", "",
		"priorityClassName of a scheduled Trigger's firing pod.")
	flag.StringVar(&triggerSchedule.GatewayURL, "trigger-gateway-url", "",
		"Gateway base URL a scheduled Trigger's firing pod calls. Empty leaves scheduled Triggers not Ready.")
	flag.StringVar(&triggerSchedule.Audience, "trigger-gateway-audience", controller.DefaultTriggerGatewayAudience,
		"Audience of the projected ServiceAccount token a scheduled Trigger's firing pod sends to the gateway.")
	flag.StringVar(&triggerTaskBucket, "trigger-task-bucket", controller.DefaultGatewayTaskBucket,
		"NATS KV bucket of the gateway's task records; Triggers with a knight target record their tasks there.")
	flag.DurationVar(&triggerTaskTTL, "trigger-task-ttl", controller.DefaultGatewayTaskTTL,
		"How long the gateway keeps task records (used if the operator creates the bucket first).")
	flag.DurationVar(&scheduleResultRetention, "schedule-result-retention", gateway.DefaultScheduleResultRetention,
		"How long the results of schedule Triggers' runs stay readable: finished runs are kept at least this "+
			"long and the task record TTL is raised to it.")
	flag.DurationVar(&missionResultsTTL, "mission-results-ttl", natspkg.DefaultResultsTTL,
		"How long retained mission results are kept in the mission-results NATS KV bucket. "+
			"An existing bucket with a different TTL is updated.")
	flag.IntVar(&chainMaxConcurrentReconciles, "chain-max-concurrent-reconciles", 1,
		"How many Chains reconcile in parallel. 1 keeps the single worker.")
	flag.IntVar(&maxConcurrentRollouts, "max-concurrent-rollouts", 0,
		"Maximum number of knights per namespace rolling out a changed pod template at once. "+
			"0 means unlimited.")
	flag.IntVar(&providerMaxRetries, "provider-overload-max-retries", 4,
		"Retries (1m, 2m, 4m, 8m backoff) of a chain step whose task failed with a transient "+
			"model provider error (HTTP 529/503/502/500, overloaded). 0 disables.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.DurationVar(&leTimings.LeaseDuration, "leader-election-lease-duration", 60*time.Second,
		"How long a non-leader waits before taking over an unrenewed lease.")
	flag.DurationVar(&leTimings.RenewDeadline, "leader-election-renew-deadline", 40*time.Second,
		"How long the leader keeps retrying to renew the lease before giving up and exiting. "+
			"Must be less than --leader-election-lease-duration.")
	flag.DurationVar(&leTimings.RetryPeriod, "leader-election-retry-period", 5*time.Second,
		"How often the leader election client retries acquiring or renewing the lease.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&sandboxBackendMode, "enable-sandbox-backend", string(rtruntime.SandboxModeAuto),
		"Enable the agent-sandbox runtime backend: auto (only if agents.x-k8s.io/v1beta1 Sandbox is served, "+
			"i.e. agent-sandbox v1.0.0 or later), "+
			"true or false. When disabled, knights with runtime: sandbox report RuntimeAvailable=False.")
	flag.StringVar(&kedaMode, "enable-keda", string(rtruntime.KEDAModeAuto),
		"Render KEDA ScaledObjects for autoscaled knights (maxReplicas > 1 or suspendPolicy: auto): "+
			"auto (only if keda.sh/v1alpha1 ScaledObject is served), true or false. "+
			"When disabled, such knights run spec.replicas pods and report Autoscaled=False.")
	flag.StringVar(&natsMonitoringEndpoint, "nats-monitoring-endpoint", os.Getenv("NATS_MONITORING_ENDPOINT"),
		"NATS HTTP monitoring endpoint (host:port) for KEDA's JetStream scaler. "+
			"Empty derives it from each knight's spec.nats.url host on port 8222.")
	flag.StringVar(&githubAPIURL, "github-api-url", "https://api.github.com",
		"GitHub REST API URL used by RoundTable spec.issuePickup and github-source Triggers.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	triggerSchedule.ImagePullPolicy = corev1.PullPolicy(triggerPullPolicy)

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	scheduleMode, err := controller.ParseChainScheduleMode(chainScheduleMode)
	if err != nil {
		setupLog.Error(err, "Invalid --chain-schedule-mode")
		os.Exit(1)
	}
	setupLog.Info("Chain schedule mode", "mode", scheduleMode)

	for n := range strings.SplitSeq(triggerPullSecrets, ",") {
		if n = strings.TrimSpace(n); n != "" {
			triggerSchedule.ImagePullSecrets = append(triggerSchedule.ImagePullSecrets, corev1.LocalObjectReference{Name: n})
		}
	}
	if triggerNodeSelector != "" {
		if err := json.Unmarshal([]byte(triggerNodeSelector), &triggerSchedule.NodeSelector); err != nil {
			setupLog.Error(err, "invalid --trigger-node-selector")
			os.Exit(1)
		}
	}
	if triggerTolerations != "" {
		if err := json.Unmarshal([]byte(triggerTolerations), &triggerSchedule.Tolerations); err != nil {
			setupLog.Error(err, "invalid --trigger-tolerations")
			os.Exit(1)
		}
	}

	// Register the W3C trace-context + baggage propagator unless something
	// else already installed one, so trace context crosses NATS.
	if len(otel.GetTextMapPropagator().Fields()) == 0 {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))
	}

	shutdownTelemetry, err := telemetry.Setup(context.Background(), version)
	if err != nil {
		setupLog.Error(err, "Failed to set up OpenTelemetry; continuing without tracing")
		shutdownTelemetry = func(context.Context) error { return nil }
	}
	if telemetry.Enabled() {
		setupLog.Info("OpenTelemetry tracing enabled", "endpoint", telemetry.Endpoint())
	} else {
		setupLog.Info("OpenTelemetry tracing disabled (no OTLP endpoint configured or OTEL_SDK_DISABLED=true)")
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	sandboxMode, err := rtruntime.ParseSandboxMode(sandboxBackendMode)
	if err != nil {
		setupLog.Error(err, "Invalid --enable-sandbox-backend")
		os.Exit(1)
	}
	parsedKEDAMode, err := rtruntime.ParseKEDAMode(kedaMode)
	if err != nil {
		setupLog.Error(err, "Invalid --enable-keda")
		os.Exit(1)
	}

	restConfig := ctrl.GetConfigOrDie()

	// The sandbox backend watches agents.x-k8s.io/v1beta1 Sandbox objects. If
	// that kind isn't served, the watch never syncs and the manager exits after
	// the cache-sync timeout, taking every Knight down with it (#186). Probe
	// discovery once at startup and only wire the backend in when it's served.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "Failed to create discovery client")
		os.Exit(1)
	}
	sandboxEnabled, sandboxReason, err := rtruntime.ResolveSandboxBackend(sandboxMode, discoveryClient)
	if err != nil {
		setupLog.Error(err, "Failed to detect the agent-sandbox API")
		os.Exit(1)
	}
	if sandboxEnabled {
		setupLog.Info("Runtime backends enabled", "backends", []string{"deployment", "sandbox"}, "reason", sandboxReason)
	} else {
		setupLog.Info("Runtime backends enabled", "backends", []string{"deployment"},
			"reason", sandboxReason+"; knights with runtime: sandbox will report RuntimeAvailable=False")
	}

	// KEDA is optional too: probe once, the same way as the sandbox backend.
	// The operator only touches ScaledObjects when the kind is served.
	kedaEnabled, kedaReason, err := rtruntime.ResolveKEDA(parsedKEDAMode, discoveryClient)
	if err != nil {
		setupLog.Error(err, "Failed to detect the KEDA API")
		os.Exit(1)
	}
	if kedaEnabled {
		setupLog.Info("Knight autoscaling enabled", "reason", kedaReason)
	} else {
		setupLog.Info("Knight autoscaling disabled",
			"reason", kedaReason+"; knights asking for autoscaling run spec.replicas pods and report Autoscaled=False")
	}

	// Cilium FQDN policy for knights with spec.egress: probe once. Without it
	// the operator renders a NetworkPolicy without hostname rules.
	ciliumEnabled, err := rtruntime.CiliumNetworkPolicyAPIServed(discoveryClient)
	if err != nil {
		setupLog.Error(err, "Failed to detect the CiliumNetworkPolicy API")
		os.Exit(1)
	}
	setupLog.Info("Knight egress policy backend", "ciliumNetworkPolicy", ciliumEnabled)

	// Workspace snapshots need the external-snapshotter VolumeSnapshot API.
	snapshotsEnabled, err := rtruntime.VolumeSnapshotAPIServed(discoveryClient)
	if err != nil {
		setupLog.Error(err, "Failed to detect the VolumeSnapshot API")
		os.Exit(1)
	}
	setupLog.Info("Knight workspace snapshots", "volumeSnapshotAPI", snapshotsEnabled)

	if enableLeaderElection {
		if err := leTimings.Validate(); err != nil {
			setupLog.Error(err, "Invalid leader election configuration")
			os.Exit(1)
		}
	}

	mgr, err := ctrl.NewManager(restConfig, managerOptions(scheme, metricsServerOptions, webhookServer,
		probeAddr, enableLeaderElection, leTimings))
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Create shared NATS provider
	natsConfig := natspkg.DefaultConfig()
	// Allow override via environment variable
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		natsConfig.URL = natsURL
	}
	natsConfig.ResultsTTL = missionResultsTTL
	natsProvider := natspkg.NewProvider(natsConfig, ctrl.Log.WithName("nats"))
	setupLog.Info("NATS provider initialized", "url", natsConfig.URL)

	// Completion webhook notifier (spec.notify on Chains/Missions). The URL
	// allowlist is the SSRF guard — with no prefixes configured, every
	// notification is rejected and the feature is effectively disabled.
	var notifyPrefixes []string
	for p := range strings.SplitSeq(os.Getenv("NOTIFY_ALLOWED_URL_PREFIXES"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			notifyPrefixes = append(notifyPrefixes, p)
		}
	}
	notifier := notifypkg.NewNotifier(notifyPrefixes)
	setupLog.Info("Completion webhook notifier initialized",
		"allowedURLPrefixes", notifyPrefixes, "enabled", len(notifyPrefixes) > 0)

	// Ensure cleanup on shutdown
	defer func() {
		if err := natsProvider.Close(); err != nil {
			setupLog.Error(err, "Failed to close NATS provider")
		}
	}()
	// Dead-letter watchers (#396), started per RoundTable by its controller.
	// Stopped explicitly after mgr.Start returns (StopAllContext below, bounded
	// by shutdownTimeout), before the deferred NATS provider Close runs.
	deadLetters := deadletter.NewManager(ctrl.Log.WithName("dead-letter"))

	defaultImage := os.Getenv("DEFAULT_KNIGHT_IMAGE")
	cardsBucket := natspkg.CardsBucketFromEnv()
	knightSecurity := knightpkg.PodSecurityFromEnv()
	egressNetPol := mission.NetPolSettingsFromEnv()
	isolation := knightpkg.IsolationSettingsFromEnv()
	setupLog.Info("Knight isolation defaults", "runtimeClassName", isolation.RuntimeClassName,
		"egressDeny", isolation.EgressDeny, "baseHosts", isolation.BaseHosts)
	// Note: GetEventRecorderFor is deprecated (SA1019); GetEventRecorder returns a different
	// events API type and needs controller changes, so the migration is deferred.
	knightReconciler := &controller.KnightReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		APIReader:      mgr.GetAPIReader(),
		Recorder:       mgr.GetEventRecorderFor("knight-controller"), //nolint:staticcheck // SA1019: see note above
		DefaultImage:   defaultImage,
		BrowserImage:   knightpkg.BrowserImageFromEnv(),
		KnightSecurity: knightSecurity,
		CardsBucket:    cardsBucket,
		SandboxEnabled: sandboxEnabled,

		MaxConcurrentRollouts:  maxConcurrentRollouts,
		KEDAEnabled:            kedaEnabled,
		NATSMonitoringEndpoint: natsMonitoringEndpoint,
		NATS:                   natsProvider,
		CiliumEnabled:          ciliumEnabled,
		SnapshotsEnabled:       snapshotsEnabled,
		SnapshotAPIProbe:       func() (bool, error) { return rtruntime.VolumeSnapshotAPIServed(discoveryClient) },
		EgressNetPol:           &egressNetPol,
		Isolation:              isolation,
	}

	// Create runtime backends
	deploymentBackend := knightReconciler.NewDeploymentBackend()

	// Register backends by runtime type name
	knightReconciler.RuntimeBackend = deploymentBackend // default
	knightReconciler.RuntimeBackends = map[string]rtruntime.RuntimeBackend{
		"deployment": deploymentBackend,
	}
	if sandboxEnabled {
		knightReconciler.RuntimeBackends["sandbox"] = rtruntime.NewSandboxBackend(
			mgr.GetClient(),
			mgr.GetScheme(),
			defaultImage,
			knightReconciler.BuildDrainablePodSpec,
		)
	}

	if err := knightReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Knight")
		os.Exit(1)
	}
	if err := (&controller.ChainReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("chain-controller"), //nolint:staticcheck // SA1019: see note above
		NATS:     natsProvider,
		Notify:   notifier,

		DefaultKnightImage: defaultImage,
		ProviderMaxRetries: providerMaxRetries,
		ScheduleMode:       scheduleMode,

		MaxConcurrentReconciles: chainMaxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Chain")
		os.Exit(1)
	}
	plannerDefaults, err := controller.PlannerDefaultsFromEnv()
	if err != nil {
		setupLog.Error(err, "Invalid table planner defaults")
		os.Exit(1)
	}
	if err := (&controller.RoundTableReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("roundtable-controller"), //nolint:staticcheck // SA1019: see note above
		NATS:            natsProvider,
		DeadLetters:     deadLetters,
		CardsBucket:     cardsBucket,
		PlannerDefaults: plannerDefaults,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "RoundTable")
		os.Exit(1)
	}
	githubFactory := github.NewClientFactory(githubAPIURL)
	// Triggers (#402) start Chain runs from NATS events, webhooks, cron
	// schedules and GitHub polls.
	if err := (&controller.TriggerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("trigger-controller"), //nolint:staticcheck // SA1019: see note above
		NATS:     natsProvider,
		GitHub:   githubFactory,

		TaskBucket:         triggerTaskBucket,
		TaskTTL:            gateway.EffectiveTaskTTL(triggerTaskTTL, scheduleResultRetention),
		ScheduleRetention:  scheduleResultRetention,
		DefaultKnightImage: defaultImage,
		Schedule:           triggerSchedule,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Trigger")
		os.Exit(1)
	}
	// Issue pickup (#224) only acts on RoundTables that set spec.issuePickup.
	if err := (&controller.IssuePickupReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Recorder:    mgr.GetEventRecorderFor("issuepickup-controller"), //nolint:staticcheck // SA1019: see note above
		GitHub:      githubFactory,
		DeadLetters: deadLetters,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "IssuePickup")
		os.Exit(1)
	}
	if err := (&controller.PRFeedbackReconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorderFor("prfeedback-controller"), //nolint:staticcheck // SA1019: see note above
		GitHub:   githubFactory,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "PRFeedback")
		os.Exit(1)
	}
	missionPlanner := &mission.Planner{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		NATS:     natsProvider,
		Recorder: mgr.GetEventRecorderFor("mission-controller"), //nolint:staticcheck // SA1019: see note above
	}
	if err := (&controller.MissionReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("mission-controller"), //nolint:staticcheck // SA1019: see note above
		NATS:     natsProvider,
		Notify:   notifier,
		Planner:  missionPlanner,
		Assembler: &mission.KnightAssembler{
			Client:     mgr.GetClient(),
			Scheme:     mgr.GetScheme(),
			EgressDeny: isolation.EgressDeny,
		},
		Cleaner:     &mission.Cleaner{Client: mgr.GetClient(), NATS: natsProvider},
		CostTracker: &mission.CostTracker{Client: mgr.GetClient()},
		Briefer: &mission.Briefer{
			Client:   mgr.GetClient(),
			Recorder: mgr.GetEventRecorderFor("mission-controller"), //nolint:staticcheck // SA1019: see note above
			NATS:     natsProvider,
		},
		Chains: &mission.ChainManager{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Mission")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	if enableLeaderElection {
		logLeaseHolder(restConfig, leTimings)
		// Log once when this pod wins the lease; Elected() closes at once
		// when leader election is off, so only wait for it when it is on.
		go func() {
			<-mgr.Elected()
			setupLog.Info("Acquired the leader lease; controllers starting",
				"leaseDuration", leTimings.LeaseDuration.String(),
				"identity", os.Getenv("HOSTNAME"))
		}()
	} else {
		setupLog.Info("Leader election disabled; controllers starting")
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		if isLeaderElectionLost(err) {
			setupLog.Error(err, "Leader election lease lost: could not renew the lease within the renew deadline "+
				"(the Kubernetes API was slow or unreachable); exiting so a fresh leader can start",
				"leaseDuration", leTimings.LeaseDuration.String(),
				"renewDeadline", leTimings.RenewDeadline.String(),
				"retryPeriod", leTimings.RetryPeriod.String())
			os.Exit(1)
		}
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}

	// The lease is released when mgr.Start returns (ReleaseOnCancel), so
	// everything below must finish well inside terminationGracePeriodSeconds
	// (10s): one shared 5s budget for the dead-letter watchers and telemetry.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := deadLetters.StopAllContext(shutdownCtx); err != nil {
		setupLog.Error(err, "Dead-letter watchers did not stop in time")
	}
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		setupLog.Error(err, "Failed to flush OpenTelemetry spans")
	}
}

// shutdownTimeout bounds all work after the manager stops (watcher stop,
// telemetry flush), so the process exits inside the pod's termination grace
// period after the lease has been released.
//
// The manager's own graceful shutdown (draining runnables, releasing the
// lease) happens first, bounded by managerShutdownTimeout.
const shutdownTimeout = 5 * time.Second

// managerShutdownTimeout is ctrl.Options.GracefulShutdownTimeout. With the
// 5s shutdownTimeout for post-manager work it totals 9s, below the chart's
// terminationGracePeriodSeconds of 10s.
const managerShutdownTimeout = 4 * time.Second

// leaseReadTimeout bounds the best-effort start-up read of the Lease.
const leaseReadTimeout = 3 * time.Second

// logLeaseHolder logs the current Lease holderIdentity and leaderTransitions
// so the start-up log shows whether the previous holder released the lease
// (empty holder) or this pod must wait it out. Best effort: any error is
// logged and ignored.
func logLeaseHolder(cfg *rest.Config, t leaderElectionTimings) {
	nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		setupLog.Info("Lease holder unknown: namespace not detected", "error", err.Error())
		return
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Info("Lease holder unknown: client setup failed", "error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), leaseReadTimeout)
	defer cancel()
	ns := strings.TrimSpace(string(nsBytes))
	l, err := cs.CoordinationV1().Leases(ns).Get(ctx, leaderElectionID, metav1.GetOptions{})
	logLeaseState(l, err, t)
}

// logLeaseState logs the outcome of the Lease read; it never fails.
func logLeaseState(l *coordinationv1.Lease, err error, t leaderElectionTimings) {
	if err != nil {
		setupLog.Info("Lease not readable; continuing", "error", err.Error())
		return
	}
	holder := ""
	if l.Spec.HolderIdentity != nil {
		holder = *l.Spec.HolderIdentity
	}
	var transitions int32
	if l.Spec.LeaseTransitions != nil {
		transitions = *l.Spec.LeaseTransitions
	}
	setupLog.Info("Leader lease state at start-up",
		"holderIdentity", holder, "released", holder == "",
		"leaderTransitions", transitions,
		"leaseDuration", t.LeaseDuration.String())
}

const leaderElectionID = "dcc66784.roundtable.io"

// managerOptions builds the manager options. LeaderElectionReleaseOnCancel is
// on: after mgr.Start returns, main only does work bounded by shutdownTimeout
// (stopping the dead-letter watchers, flushing telemetry, closing NATS), which
// is shorter than terminationGracePeriodSeconds, so releasing the lease cannot
// let a second leader run alongside leftover work. A new pod can then take over
// at once instead of waiting leaseDuration; a crash still waits it out.
func managerOptions(s *runtime.Scheme, metrics metricsserver.Options, webhooks webhook.Server,
	probeAddr string, leaderElect bool, t leaderElectionTimings) ctrl.Options {
	return ctrl.Options{
		Scheme: s,
		// Secrets are read uncached (no cluster-wide Secret informer).
		Client:                        controller.ManagerClientOptions(),
		Metrics:                       metrics,
		WebhookServer:                 webhooks,
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                leaderElect,
		LeaderElectionID:              leaderElectionID,
		GracefulShutdownTimeout:       new(managerShutdownTimeout),
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 &t.LeaseDuration,
		RenewDeadline:                 &t.RenewDeadline,
		RetryPeriod:                   &t.RetryPeriod,
	}
}

// leaderElectionTimings are the lease timings handed to controller-runtime.
type leaderElectionTimings struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Validate mirrors client-go's leader election constraints so a bad
// combination fails at start-up with a clear message rather than a late,
// cryptic error from the election loop. client-go requires
// leaseDuration > renewDeadline > retryPeriod * 1.2 (JitterFactor).
func (t leaderElectionTimings) Validate() error {
	if t.LeaseDuration <= 0 || t.RenewDeadline <= 0 || t.RetryPeriod <= 0 {
		return fmt.Errorf("leader election timings must be positive: lease-duration=%s renew-deadline=%s retry-period=%s",
			t.LeaseDuration, t.RenewDeadline, t.RetryPeriod)
	}
	if t.RenewDeadline >= t.LeaseDuration {
		return fmt.Errorf("leader-election-renew-deadline (%s) must be less than leader-election-lease-duration (%s)",
			t.RenewDeadline, t.LeaseDuration)
	}
	if float64(t.RenewDeadline) <= 1.2*float64(t.RetryPeriod) {
		return fmt.Errorf("leader-election-renew-deadline (%s) must be greater than 1.2 x leader-election-retry-period (%s)",
			t.RenewDeadline, t.RetryPeriod)
	}
	return nil
}

// isLeaderElectionLost reports whether a manager.Start error means the lease
// could not be renewed in time (controller-runtime returns "leader election lost").
func isLeaderElectionLost(err error) bool {
	return err != nil && strings.Contains(err.Error(), "leader election lost")
}
