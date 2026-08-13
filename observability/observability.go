// Package observability provides vendor-neutral tracing, metrics, structured logging,
// and correlation propagation for every consuming deployable.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/anshacerbia2/foundation-platform/id"
	"github.com/anshacerbia2/foundation-platform/redact"
)

const instrumentationName = "github.com/anshacerbia2/foundation-platform"

type contextKey uint8

const (
	correlationKey contextKey = iota + 1
	causationKey
)

// Config identifies one deployment and supplies process-owned OpenTelemetry providers.
type Config struct {
	Deployable     string
	System         string
	Logger         *slog.Logger
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
}

// Telemetry binds the identity every signal must carry to its instruments.
type Telemetry struct {
	deployable string
	system     string
	logger     *slog.Logger
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	shedRequests    metric.Int64Counter
	panics          metric.Int64Counter
}

// New constructs telemetry without starting an exporter. Exporter lifecycle remains at
// the composition root that owns the supplied providers.
func New(cfg Config) (*Telemetry, error) {
	cfg.Deployable = strings.TrimSpace(cfg.Deployable)
	cfg.System = strings.TrimSpace(cfg.System)
	if cfg.Deployable == "" {
		return nil, errors.New("observability: deployable is required")
	}
	if cfg.System == "" {
		return nil, errors.New("observability: system is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}

	meter := cfg.MeterProvider.Meter(instrumentationName)
	requests, err := meter.Int64Counter("http.server.requests")
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("http.server.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	shed, err := meter.Int64Counter("http.server.shed_requests")
	if err != nil {
		return nil, err
	}
	panics, err := meter.Int64Counter("http.server.panics")
	if err != nil {
		return nil, err
	}

	baseLogger := slog.New(redact.NewHandler(cfg.Logger.Handler())).With(
		slog.String("deployable", cfg.Deployable),
		slog.String("system", cfg.System),
	)
	return &Telemetry{
		deployable:      cfg.Deployable,
		system:          cfg.System,
		logger:          baseLogger,
		tracer:          cfg.TracerProvider.Tracer(instrumentationName),
		propagator:      cfg.Propagator,
		requests:        requests,
		requestDuration: duration,
		shedRequests:    shed,
		panics:          panics,
	}, nil
}

// EnsureCorrelation propagates a valid inbound identifier or generates a new UUIDv7.
// A malformed inbound value is replaced rather than reflected back to a caller.
func EnsureCorrelation(ctx context.Context, inbound string) (context.Context, id.UUID, error) {
	correlationID, err := id.Parse(strings.TrimSpace(inbound))
	if err != nil {
		correlationID, err = id.NewV7()
		if err != nil {
			return ctx, id.Nil, err
		}
	}
	return WithCorrelationID(ctx, correlationID), correlationID, nil
}

// WithCorrelationID stores a non-nil correlation identifier in ctx.
func WithCorrelationID(ctx context.Context, correlationID id.UUID) context.Context {
	if correlationID.IsNil() {
		return ctx
	}
	return context.WithValue(ctx, correlationKey, correlationID)
}

// CorrelationID returns the correlation identifier carried by ctx.
func CorrelationID(ctx context.Context) (id.UUID, bool) {
	value, ok := ctx.Value(correlationKey).(id.UUID)
	return value, ok && !value.IsNil()
}

// WithCausationID stores the request or event that caused the current operation.
func WithCausationID(ctx context.Context, causationID id.UUID) context.Context {
	if causationID.IsNil() {
		return ctx
	}
	return context.WithValue(ctx, causationKey, causationID)
}

// CausationID returns the causation identifier carried by ctx.
func CausationID(ctx context.Context) (id.UUID, bool) {
	value, ok := ctx.Value(causationKey).(id.UUID)
	return value, ok && !value.IsNil()
}

// Metadata is embedded by event payloads that cross the broker boundary.
type Metadata struct {
	CorrelationID id.UUID `json:"correlation_id"`
	CausationID   id.UUID `json:"causation_id"`
}

// MetadataFromContext extracts correlation and causation identifiers for an event.
func MetadataFromContext(ctx context.Context) Metadata {
	correlationID, _ := CorrelationID(ctx)
	causationID, ok := CausationID(ctx)
	if !ok {
		causationID = correlationID
	}
	return Metadata{CorrelationID: correlationID, CausationID: causationID}
}

// ContextWithMetadata restores broker-carried metadata before consumer work starts.
func ContextWithMetadata(ctx context.Context, metadata Metadata) context.Context {
	ctx = WithCorrelationID(ctx, metadata.CorrelationID)
	return WithCausationID(ctx, metadata.CausationID)
}

// Logger returns a logger carrying the deployment and current correlation dimensions.
func (t *Telemetry) Logger(ctx context.Context) *slog.Logger {
	logger := t.logger
	if correlationID, ok := CorrelationID(ctx); ok {
		logger = logger.With(slog.String("correlation_id", correlationID.String()))
	}
	if causationID, ok := CausationID(ctx); ok {
		logger = logger.With(slog.String("causation_id", causationID.String()))
	}
	return logger
}

// Start begins a span carrying every mandatory telemetry dimension.
func (t *Telemetry) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts = append(opts, trace.WithAttributes(t.attributes(ctx)...))
	return t.tracer.Start(ctx, name, opts...)
}

// Inject writes the active trace context to an outbound broker carrier.
func (t *Telemetry) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	t.propagator.Inject(ctx, carrier)
}

// StartConsumer extracts a producer context and links it to a new consumer span.
func (t *Telemetry) StartConsumer(ctx context.Context, name string, carrier propagation.TextMapCarrier) (context.Context, trace.Span) {
	remote := t.propagator.Extract(context.Background(), carrier)
	spanContext := trace.SpanContextFromContext(remote)
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}
	if spanContext.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: spanContext}))
	}
	return t.Start(ctx, name, opts...)
}

// RecordRequest records the outcome of one inbound request.
func (t *Telemetry) RecordRequest(ctx context.Context, method string, status int, duration time.Duration) {
	attrs := append(t.attributes(ctx),
		attribute.String("http.request.method", method),
		attribute.Int("http.response.status_code", status),
	)
	t.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.requestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordShed records a request rejected before authentication.
func (t *Telemetry) RecordShed(ctx context.Context) {
	t.shedRequests.Add(ctx, 1, metric.WithAttributes(t.attributes(ctx)...))
}

// RecordPanic records a panic recovered at the HTTP boundary.
func (t *Telemetry) RecordPanic(ctx context.Context) {
	t.panics.Add(ctx, 1, metric.WithAttributes(t.attributes(ctx)...))
}

func (t *Telemetry) attributes(ctx context.Context) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("deployable", t.deployable),
		attribute.String("system", t.system),
	}
	if correlationID, ok := CorrelationID(ctx); ok {
		attrs = append(attrs, attribute.String("correlation_id", correlationID.String()))
	}
	if causationID, ok := CausationID(ctx); ok {
		attrs = append(attrs, attribute.String("causation_id", causationID.String()))
	}
	return attrs
}
