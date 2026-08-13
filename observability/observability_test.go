package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/anshacerbia2/foundation-platform/id"
)

func newTelemetry(t *testing.T, output *bytes.Buffer) *Telemetry {
	t.Helper()
	telemetry, err := New(Config{
		Deployable: "control-test",
		System:     "SAD-test",
		Logger:     slog.New(slog.NewJSONHandler(output, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return telemetry
}

func TestEnsureCorrelationPropagatesAValidIdentifier(t *testing.T) {
	want := id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
	ctx, got, err := EnsureCorrelation(context.Background(), want.String())
	if err != nil {
		t.Fatalf("EnsureCorrelation: %v", err)
	}
	stored, ok := CorrelationID(ctx)
	if got != want || !ok || stored != want {
		t.Errorf("correlation = %s, stored = %s, present = %v", got, stored, ok)
	}
}

func TestEnsureCorrelationReplacesInvalidInput(t *testing.T) {
	_, got, err := EnsureCorrelation(context.Background(), "not-a-uuid")
	if err != nil {
		t.Fatalf("EnsureCorrelation: %v", err)
	}
	if got.IsNil() || got.Version() != 7 {
		t.Errorf("generated correlation = %s", got)
	}
}

func TestMetadataCrossesAContextBoundary(t *testing.T) {
	correlationID := id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
	causationID := id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d72")
	producer := WithCausationID(WithCorrelationID(context.Background(), correlationID), causationID)

	consumer := ContextWithMetadata(context.Background(), MetadataFromContext(producer))
	gotCorrelation, _ := CorrelationID(consumer)
	gotCausation, _ := CausationID(consumer)
	if gotCorrelation != correlationID || gotCausation != causationID {
		t.Errorf("metadata = %s, %s", gotCorrelation, gotCausation)
	}
}

func TestRootRequestUsesCorrelationAsCausation(t *testing.T) {
	correlationID := id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
	metadata := MetadataFromContext(WithCorrelationID(context.Background(), correlationID))
	if metadata.CorrelationID != correlationID || metadata.CausationID != correlationID {
		t.Errorf("root metadata = %+v", metadata)
	}
}

func TestLoggerCarriesDimensionsAndRedacts(t *testing.T) {
	var output bytes.Buffer
	telemetry := newTelemetry(t, &output)
	ctx, correlationID, err := EnsureCorrelation(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	telemetry.Logger(ctx).Error("broker failed password=visible", slog.String("token", "visible"))

	got := output.String()
	for _, want := range []string{"control-test", "SAD-test", correlationID.String(), "[REDACTED]"} {
		if !strings.Contains(got, want) {
			t.Errorf("log omits %q: %s", want, got)
		}
	}
	if strings.Contains(got, "visible") {
		t.Errorf("log leaked credential material: %s", got)
	}
}

func TestNewRequiresSignalDimensions(t *testing.T) {
	if _, err := New(Config{System: "SAD-test"}); err == nil {
		t.Error("New accepted an empty deployable")
	}
	if _, err := New(Config{Deployable: "control-test"}); err == nil {
		t.Error("New accepted an empty system")
	}
}

func TestSpanCarriesDimensionsAndConsumerLinksProducer(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	telemetry, err := New(Config{
		Deployable:     "control-test",
		System:         "SAD-test",
		TracerProvider: provider,
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, correlationID, err := EnsureCorrelation(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, requestSpan := telemetry.Start(ctx, "request")
	requestSpan.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d", len(ended))
	}
	attributes := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attributes[string(attr.Key)] = attr.Value.AsString()
	}
	for key, want := range map[string]string{
		"deployable":     "control-test",
		"system":         "SAD-test",
		"correlation_id": correlationID.String(),
	} {
		if attributes[key] != want {
			t.Errorf("span attribute %s = %q, want %q", key, attributes[key], want)
		}
	}

	producer := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	telemetry.Inject(trace.ContextWithSpanContext(context.Background(), producer), carrier)
	_, consumerSpan := telemetry.StartConsumer(ctx, "consume", carrier)
	consumerSpan.End()

	ended = recorder.Ended()
	links := ended[len(ended)-1].Links()
	if len(links) != 1 || links[0].SpanContext.TraceID() != producer.TraceID() || links[0].SpanContext.SpanID() != producer.SpanID() {
		t.Errorf("consumer links = %+v, want producer %s/%s", links, producer.TraceID(), producer.SpanID())
	}
}
