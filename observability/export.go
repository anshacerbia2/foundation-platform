package observability

// Export starts the OTLP exporters a composition root hands to New.
//
// STD-GLB-003 requires every signal to flow through an OpenTelemetry Collector, and New starts no
// exporter by design: the lifecycle belongs to the process. Until this existed no deployable
// started one either, so every metric and span went to the global no-op providers and nothing
// measured was ever seen. This is the one implementation, so each deployable does not grow its own.
//
// The endpoint is passed in rather than read here. This package reads no configuration; the
// composition root reads OTEL_EXPORTER_OTLP_ENDPOINT and decides what an empty value means.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ExportConfig is what the composition root supplies.
type ExportConfig struct {
	// Endpoint is the Collector's OTLP/HTTP base URL, such as http://collector:4318.
	Endpoint string

	Deployable string
	System     string

	// Interval is how often metrics are pushed. Defaults to 30s: below every alert's evaluation
	// window in the estate, so a threshold is never judged on a value older than its own "for".
	Interval time.Duration
}

// Exported holds the providers and the shutdown that flushes them.
type Exported struct {
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
	shutdown       []func(context.Context) error
}

// Shutdown flushes and stops both exporters. Call it on the way out, with a bounded context, so the
// last interval's metrics are not lost to a routine restart.
func (e *Exported) Shutdown(ctx context.Context) error {
	var errs []error
	for _, stop := range e.shutdown {
		errs = append(errs, stop(ctx))
	}
	return errors.Join(errs...)
}

// Export builds OTLP/HTTP metric and trace exporters for cfg.Endpoint.
//
// Nothing is sent at construction: an unreachable Collector is reported per export by the SDK and
// never stops the process. Telemetry failing must not become an outage (foundation-operations
// runbook), and a Collector that is down is what the absent-telemetry alert is for.
func Export(ctx context.Context, cfg ExportConfig) (*Exported, error) {
	endpoint := strings.TrimSuffix(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return nil, errors.New("observability: an OTLP endpoint is required to export")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("observability: %q is not an http(s) OTLP endpoint", endpoint)
	}
	if strings.TrimSpace(cfg.Deployable) == "" || strings.TrimSpace(cfg.System) == "" {
		return nil, errors.New("observability: deployable and system are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}

	// The two attributes STD-GLB-003 and TDD-foundation-platform-002 require on every signal, as
	// resource attributes, so a Collector can promote them onto every series.
	res := resource.NewSchemaless(
		attribute.String("service.name", cfg.Deployable),
		attribute.String("deployable", cfg.Deployable),
		attribute.String("system", cfg.System),
	)

	metricExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"))
	if err != nil {
		return nil, fmt.Errorf("observability: metric exporter: %w", err)
	}
	meters := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(cfg.Interval))),
	)

	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	if err != nil {
		_ = meters.Shutdown(ctx)
		return nil, fmt.Errorf("observability: trace exporter: %w", err)
	}
	tracers := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter))

	return &Exported{
		MeterProvider:  meters,
		TracerProvider: tracers,
		shutdown:       []func(context.Context) error{meters.Shutdown, tracers.Shutdown},
	}, nil
}
