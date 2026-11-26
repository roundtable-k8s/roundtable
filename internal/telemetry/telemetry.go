// Package telemetry bootstraps the OpenTelemetry SDK for the operator from the
// standard OTEL_* environment variables.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DefaultServiceName is used when OTEL_SERVICE_NAME is not set.
const DefaultServiceName = "roundtable-operator"

// Enabled reports whether tracing export is configured: the SDK is not
// disabled and an OTLP traces endpoint is set.
func Enabled() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return false
	}
	return Endpoint() != ""
}

// Endpoint returns the configured OTLP traces endpoint ("" when unset).
func Endpoint() string {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); v != "" {
		return v
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}

// Setup configures the global tracer provider and propagator from the standard
// OTEL_* environment. When tracing is disabled or no endpoint is configured it
// leaves the global no-op provider in place and returns a no-op shutdown.
// The returned shutdown flushes and stops the exporter.
func Setup(ctx context.Context, version string) (func(context.Context) error, error) {
	return SetupService(ctx, DefaultServiceName, version)
}

// SetupService is Setup for a component other than the operator: serviceName
// is the service.name used when OTEL_SERVICE_NAME is not set (which still wins).
func SetupService(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if !Enabled() {
		return noop, nil
	}

	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return noop, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	// Defaults first; WithFromEnv applies OTEL_SERVICE_NAME and
	// OTEL_RESOURCE_ATTRIBUTES on top.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("service.version", version),
		),
		resource.WithFromEnv(),
	)
	if err != nil {
		_ = exp.Shutdown(ctx)
		return noop, fmt.Errorf("build OTel resource: %w", err)
	}

	// No explicit sampler: the SDK reads OTEL_TRACES_SAMPLER(_ARG) and
	// defaults to parentbased_always_on. The IDGenerator keeps the stored
	// IDs of spans emitted after the fact (Chain run and step spans); with the
	// SDK's random generator they would be dropped.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithIDGenerator(NewIDGenerator()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}
