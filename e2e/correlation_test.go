package e2e

// The Week 2 exit criterion, asserted in one test: a correlation identifier survives from an
// inbound HTTP request through a domain transaction, into an outbox row, across the broker, and
// into the consumer's span.
//
// Each hop is tested in its own package already. httpapi propagates the inbound header,
// observability carries Metadata across a context boundary and puts correlation_id on a span,
// and httpdelivery posts an envelope. What none of those tests can say is that the hops join:
// that the identifier the handler sees is the one the outbox row holds, and that the consumer
// span carries the value the client sent rather than a fresh one minted on the way. A hop that
// silently regenerated the identifier would leave every per-package test green.
//
// The broker is the Direct Durable Delivery transport this module ships (outbox/httpdelivery),
// driven by the real dispatcher against a real PostgreSQL. Nothing here is a long-running
// service: both HTTP ends are httptest servers and the database is created for this test and
// dropped after it.
//
// The identifier crosses the broker inside the event payload, as observability.Metadata, and the
// consumer restores it from there, which is what every consumer has to do: a broker carries no
// X-Correlation-Id header. This transport also sets that header, for the logs on both sides. The
// dispatcher publishes on its own context, which carries no correlation, so httpdelivery takes the
// value from the payload (TDD-foundation-platform-001 §HTTP Delivery), and the header is asserted
// too.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/httpapi"
	"github.com/anshacerbia2/foundation-platform/id"
	"github.com/anshacerbia2/foundation-platform/inbox"
	"github.com/anshacerbia2/foundation-platform/migrations"
	"github.com/anshacerbia2/foundation-platform/observability"
	"github.com/anshacerbia2/foundation-platform/outbox"
	"github.com/anshacerbia2/foundation-platform/outbox/httpdelivery"
)

const (
	source    = "/systems/e2e-producer"
	eventType = "com.scnehaux.e2e.subject.lifecycle.changed"
	consumer  = "e2e-consumer"
	applySpan = "subject.change.apply"
)

// change is the event payload, deliberately meaning nothing: this module holds no domain
// concept, in a test or anywhere else. Metadata is embedded, which is how observability
// documents that correlation crosses the broker: inside the payload the publishing domain owns.
type change struct {
	SubjectID id.UUID `json:"subject_id"`
	observability.Metadata
}

func TestTheCorrelationIdentifierSurvivesFromRequestToConsumerSpan(t *testing.T) {
	pool := throwawayDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE TABLE subject_change
			(subject_id UUID PRIMARY KEY, correlation_id UUID NOT NULL)`); err != nil {
			return err
		}
		return outbox.Subscribe(ctx, tx, consumer, []event.Type{event.MustParseType(eventType)})
	}); err != nil {
		t.Fatalf("preparing the domain table and the subscription: %v", err)
	}

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	producerTelemetry := telemetry(t, "e2e-producer", provider)
	consumerTelemetry := telemetry(t, "e2e-consumer", provider)

	// The consumer: restore the broker-carried metadata, open the consumer span, and apply the
	// event behind its inbox guard, answering with the applied marker the way a real one does.
	applied := make(chan id.UUID, 1)
	header := make(chan string, 1)
	consumerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var delivered event.Envelope
		if err := json.NewDecoder(r.Body).Decode(&delivered); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var payload change
		if err := delivered.UnmarshalData(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		ctx := observability.ContextWithMetadata(r.Context(), payload.Metadata)
		ctx, span := consumerTelemetry.StartConsumer(ctx, applySpan, propagation.HeaderCarrier(r.Header))
		defer span.End()

		if err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := inbox.Guard(ctx, tx, consumer, delivered.ID, delivered.Type)
			return err
		}); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(outbox.ApplicationReceiptHeader, outbox.ApplicationReceiptApplied)
		w.WriteHeader(http.StatusOK)
		select {
		case applied <- delivered.ID:
			header <- r.Header.Get(httpapi.CorrelationHeader)
		default:
		}
	}))
	t.Cleanup(consumerServer.Close)

	// The producer: the fixed middleware chain in front of a handler that mutates the domain and
	// appends the event in one transaction.
	producerServer := httptest.NewServer(httpapi.Chain(httpapi.Options{Telemetry: producerTelemetry})(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			subjectID, err := id.NewV7()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			err = pool.InTx(r.Context(), func(ctx context.Context, tx db.Tx) error {
				metadata := observability.MetadataFromContext(ctx)
				if _, err := tx.Exec(ctx, `INSERT INTO subject_change (subject_id, correlation_id)
					VALUES ($1, $2)`, subjectID.String(), metadata.CorrelationID.String()); err != nil {
					return err
				}
				changed, err := event.New(event.MustParseSource(source), event.MustParseType(eventType),
					time.Now(), change{SubjectID: subjectID, Metadata: metadata})
				if err != nil {
					return err
				}
				return outbox.Append(ctx, tx, subjectID, changed)
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		})))
	t.Cleanup(producerServer.Close)

	// The inbound request, carrying the identifier the whole chain must preserve.
	correlationID, err := id.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, producerServer.URL+"/changes", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(httpapi.CorrelationHeader, correlationID.String())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("calling the producer: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("producer answered %d: %s", response.StatusCode, body)
	}
	if got := response.Header.Get(httpapi.CorrelationHeader); got != correlationID.String() {
		t.Fatalf("the producer answered with correlation %q, want %s", got, correlationID)
	}

	// Hop two and three: the domain row and the outbox row committed with the inbound value.
	var domainCorrelation, outboxCorrelation, eventID string
	if err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT correlation_id::text FROM subject_change`).
			Scan(&domainCorrelation); err != nil {
			return fmt.Errorf("reading the domain row: %w", err)
		}
		return tx.QueryRow(ctx, `SELECT event_id::text, envelope->'data'->>'correlation_id' FROM platform.outbox`).
			Scan(&eventID, &outboxCorrelation)
	}); err != nil {
		t.Fatalf("reading what the request committed: %v", err)
	}
	if domainCorrelation != correlationID.String() {
		t.Errorf("the domain transaction recorded correlation %s, want %s", domainCorrelation, correlationID)
	}
	if outboxCorrelation != correlationID.String() {
		t.Errorf("the outbox row carries correlation %q, want %s", outboxCorrelation, correlationID)
	}

	// Hop four: the dispatcher publishes across the broker to the consumer.
	publisher, err := httpdelivery.NewPublisher(httpdelivery.Config{
		Endpoint:  consumerServer.URL + "/v1/deliveries",
		Tokens:    httpdelivery.StaticToken("e2e-token"),
		Timeout:   5 * time.Second,
		Telemetry: producerTelemetry,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := outbox.NewDispatcher(pool, publisher, outbox.Config{
		Consumer: consumer, Interval: 20 * time.Millisecond, IdleInterval: 100 * time.Millisecond,
		Workers: 1, PriorityWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- dispatcher.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		<-stopped
	})

	select {
	case delivered := <-applied:
		if delivered.String() != eventID {
			t.Fatalf("the consumer applied %s, want the appended event %s", delivered, eventID)
		}
		if got := <-header; got != correlationID.String() {
			t.Errorf("the delivery carried %s %q, want %s", httpapi.CorrelationHeader, got, correlationID)
		}
	case err := <-stopped:
		t.Fatalf("the dispatcher stopped before delivering: %v", err)
	case <-ctx.Done():
		t.Fatal("the event was never delivered to the consumer")
	}

	// Hop five: the consumer span carries the identifier the client sent.
	var consumerSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == applySpan {
			consumerSpan = span
		}
	}
	if consumerSpan == nil {
		t.Fatalf("no %q span was recorded", applySpan)
	}
	if consumerSpan.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("the consumer span is %s, want consumer", consumerSpan.SpanKind())
	}
	attributes := map[string]string{}
	for _, attribute := range consumerSpan.Attributes() {
		attributes[string(attribute.Key)] = attribute.Value.AsString()
	}
	if got := attributes["correlation_id"]; got != correlationID.String() {
		t.Errorf("the consumer span carries correlation_id %q, want %s, the value the client sent", got, correlationID)
	}
	if got := attributes["deployable"]; got != "e2e-consumer" {
		t.Errorf("the consumer span carries deployable %q, want e2e-consumer", got)
	}
}

func telemetry(t *testing.T, deployable string, provider trace.TracerProvider) *observability.Telemetry {
	t.Helper()
	built, err := observability.New(observability.Config{
		Deployable:     deployable,
		System:         "SAD-e2e",
		TracerProvider: provider,
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatalf("observability.New: %v", err)
	}
	return built
}

// throwawayDatabase creates a fully migrated database for this test alone and drops it after.
//
// A separate database rather than the shared one. `go test ./...` runs package binaries
// concurrently against one TEST_DATABASE_URL, the outbox suite drops and rebuilds the platform
// schema in it, and this test commits, so sharing would let either suite delete what the other
// is reading.
func throwawayDatabase(t *testing.T) *db.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_INTEGRATION") != "" {
			t.Fatal("REQUIRE_INTEGRATION is set and TEST_DATABASE_URL is empty")
		}
		t.Skip("TEST_DATABASE_URL is unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := fmt.Sprintf("platform_e2e_%d", time.Now().UnixNano())
	if err := dbtest.ExecOutsideTransaction(ctx, dsn, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() {
		if err := dbtest.ExecOutsideTransaction(context.Background(), dsn,
			"DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("%v", err)
		}
	})

	pool, err := db.Open(ctx, db.Config{Name: "e2e", DSN: replaceDatabase(dsn, name), MaxConns: 8})
	if err != nil {
		t.Fatalf("opening the throwaway database: %v", err)
	}
	t.Cleanup(pool.Close)

	set, err := migrations.PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}
	for _, migration := range set {
		if err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, migration.SQL)
			return err
		}); err != nil {
			t.Fatalf("applying %s: %v", migration.Name, err)
		}
	}
	return pool
}

// replaceDatabase swaps the database name in a URL-form DSN, keeping its query string.
func replaceDatabase(dsn, name string) string {
	index := strings.LastIndex(dsn, "/")
	if index < 0 {
		return dsn
	}
	rest := ""
	if query := strings.Index(dsn[index:], "?"); query >= 0 {
		rest = dsn[index+query:]
	}
	return dsn[:index+1] + name + rest
}
