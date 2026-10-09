package httpdelivery_test

// The status mapping is the whole contract between this adapter and the dispatcher, and getting it
// wrong is silent in both directions: retrying poison spends the attempt budget of every event
// behind it, and dead-lettering a retryable failure turns a credential rotation into data loss.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/observability"
	"github.com/anshacerbia2/foundation-platform/outbox"

	"github.com/anshacerbia2/foundation-platform/outbox/httpdelivery"
)

func testTelemetry(t *testing.T) *observability.Telemetry {
	t.Helper()
	telemetry, err := observability.New(observability.Config{
		Deployable: "httpdelivery-test",
		System:     "SAD-004",
		Logger:     slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
	})
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}
	return telemetry
}

func envelope(t *testing.T) event.Envelope {
	t.Helper()
	built, err := event.New(
		"//scnehaux.com/organization-control",
		"com.scnehaux.organization.membership.security.revoked",
		time.Now().UTC(),
		map[string]any{"membership_id": "01a05800-0000-7000-8000-000000000001", "version": 3})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	built.StreamPosition = 3
	return built
}

func publisherFor(t *testing.T, handler http.HandlerFunc) *httpdelivery.Publisher {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	publisher, err := httpdelivery.NewPublisher(httpdelivery.Config{
		Endpoint: server.URL + "/v1/deliveries", Tokens: httpdelivery.StaticToken("test-token"),
		Timeout: 2 * time.Second, Telemetry: testTelemetry(t),
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return publisher
}

func TestAnAcceptedDeliveryIsPublished(t *testing.T) {
	publisher := publisherFor(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want the delivery credential", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		w.WriteHeader(http.StatusAccepted)
	})

	if _, err := publisher.Publish(context.Background(), envelope(t)); err != nil {
		t.Errorf("Publish: %v", err)
	}
}

// TestTheConsumersPermanentRefusalsArePoison covers the statuses a retry cannot fix. Retrying them
// delays the dead letter and spends the attempt budget of every event queued behind.
func TestTheConsumersPermanentRefusalsArePoison(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity} {
		publisher := publisherFor(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"this consumer does not project that type"}`))
		})

		_, err := publisher.Publish(context.Background(), envelope(t))
		if !errors.Is(err, outbox.ErrPoison) {
			t.Errorf("status %d produced %v, want ErrPoison", status, err)
		}
	}
}

// TestACredentialFailureIsRetryableRatherThanPoison is the mapping worth arguing about.
//
// 401 and 403 look permanent from inside one request and are not: a withdrawn or expired
// credential is an operator's problem, and dead-lettering the estate's events over it would make a
// credential rotation lose data.
func TestACredentialFailureIsRetryableRatherThanPoison(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		publisher := publisherFor(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})

		_, err := publisher.Publish(context.Background(), envelope(t))
		switch {
		case err == nil:
			t.Errorf("status %d was reported as published", status)
		case errors.Is(err, outbox.ErrPoison):
			t.Errorf("status %d was classified poison; a credential failure must retry", status)
		}
	}
}

func TestAConsumerOutageIsRetryable(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusTooManyRequests} {
		publisher := publisherFor(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})

		_, err := publisher.Publish(context.Background(), envelope(t))
		switch {
		case err == nil:
			t.Errorf("status %d was reported as published", status)
		case errors.Is(err, outbox.ErrPoison):
			t.Errorf("status %d was classified poison", status)
		}
	}
}

// TestATimeoutIsRetryable is the ambiguous delivery: the consumer may have committed and the
// acknowledgement been lost. Retrying is the only available choice, and it is safe only because the
// consumer deduplicates inside the transaction that applies the effect.
func TestATimeoutIsRetryable(t *testing.T) {
	blocked := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-blocked
		w.WriteHeader(http.StatusAccepted)
	}))

	// Cleanup order matters and is LIFO, so the handler is released before the server is closed.
	// Registered the other way round, server.Close waits for the in-flight handler that is still
	// blocked on this channel, and the test hangs rather than failing.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(blocked) })

	publisher, err := httpdelivery.NewPublisher(httpdelivery.Config{
		Endpoint: server.URL + "/v1/deliveries", Tokens: httpdelivery.StaticToken("test-token"),
		Timeout: 50 * time.Millisecond, Telemetry: testTelemetry(t),
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	_, publishErr := publisher.Publish(context.Background(), envelope(t))
	switch {
	case publishErr == nil:
		t.Error("a timed-out delivery was reported as published")
	case errors.Is(publishErr, outbox.ErrPoison):
		t.Error("a timeout was classified poison; the consumer may have committed")
	}
}

func TestTheCorrelationIdentifierTravelsWithTheDelivery(t *testing.T) {
	seen := make(chan string, 1)
	publisher := publisherFor(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-Correlation-Id")
		w.WriteHeader(http.StatusAccepted)
	})

	correlation, err := event.New("//scnehaux.com/test", "com.scnehaux.organization.membership.security.revoked", time.Now(), map[string]any{})
	if err != nil {
		t.Fatalf("minting an identifier: %v", err)
	}
	ctx := observability.WithCorrelationID(context.Background(), correlation.ID)

	if _, err := publisher.Publish(ctx, envelope(t)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := <-seen; got != correlation.ID.String() {
		t.Errorf("X-Correlation-Id = %q, want %s", got, correlation.ID)
	}
}

// correlated builds an envelope whose payload carries correlation the way observability.Metadata
// puts it there.
func correlated(t *testing.T, correlation any) event.Envelope {
	t.Helper()
	built, err := event.New("/systems/organization-control", "com.scnehaux.organization.membership.security.revoked",
		time.Now().UTC(), map[string]any{"membership_id": "01a05800-0000-7000-8000-000000000001", "correlation_id": correlation})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	built.StreamPosition = 3
	return built
}

// headerSeen publishes e on ctx and returns the X-Correlation-Id the consumer received.
func headerSeen(t *testing.T, ctx context.Context, e event.Envelope) (string, bool) {
	t.Helper()
	type seenHeader struct {
		value   string
		present bool
	}
	seen := make(chan seenHeader, 1)
	publisher := publisherFor(t, func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("X-Correlation-Id")
		got := seenHeader{present: len(values) > 0}
		if got.present {
			got.value = values[0]
		}
		seen <- got
		w.WriteHeader(http.StatusAccepted)
	})
	if _, err := publisher.Publish(ctx, e); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := <-seen
	return got.value, got.present
}

// The dispatcher publishes on its own context, which carries no correlation. Before the envelope
// was read, every dispatched delivery therefore went out without the header.
func TestADispatchedDeliveryCarriesTheEnvelopesCorrelation(t *testing.T) {
	want := mintedID(t)
	got, present := headerSeen(t, context.Background(), correlated(t, want))
	if !present || got != want {
		t.Errorf("X-Correlation-Id = %q (present %v), want the envelope's %s", got, present, want)
	}
}

// The context is the caller's statement about this publication, and it was the only source before
// the envelope's was read, so it still wins.
func TestTheContextsCorrelationWinsOverTheEnvelopes(t *testing.T) {
	fromContext, err := event.New("/systems/test", "com.scnehaux.organization.membership.security.revoked", time.Now(), map[string]any{})
	if err != nil {
		t.Fatalf("minting an identifier: %v", err)
	}
	ctx := observability.WithCorrelationID(context.Background(), fromContext.ID)

	got, _ := headerSeen(t, ctx, correlated(t, mintedID(t)))
	if got != fromContext.ID.String() {
		t.Errorf("X-Correlation-Id = %q, want the context's %s", got, fromContext.ID)
	}
}

// A value that is not an identifier is not reflected into a header, and it does not fail the
// delivery: the event is not at fault.
func TestAMalformedEnvelopeCorrelationSendsNoHeaderAndStillDelivers(t *testing.T) {
	for name, value := range map[string]any{
		"not an identifier": "not-a-uuid\r\nX-Injected: yes",
		"a number":          42,
		"null":              nil,
	} {
		if got, present := headerSeen(t, context.Background(), correlated(t, value)); present {
			t.Errorf("%s: X-Correlation-Id = %q, want no header", name, got)
		}
	}
	if got, present := headerSeen(t, context.Background(), envelope(t)); present {
		t.Errorf("a payload without correlation_id: X-Correlation-Id = %q, want no header", got)
	}
}

func mintedID(t *testing.T) string {
	t.Helper()
	minted, err := event.New("/systems/test", "com.scnehaux.organization.membership.security.revoked", time.Now(), map[string]any{})
	if err != nil {
		t.Fatalf("minting an identifier: %v", err)
	}
	return minted.ID.String()
}

func TestConstructionRefusesAnUnauthenticatedPublisher(t *testing.T) {
	for name, cfg := range map[string]httpdelivery.Config{
		"an empty credential": {Endpoint: "http://127.0.0.1:8096/v1/deliveries", Tokens: httpdelivery.StaticToken(""),
			Timeout: time.Second, Telemetry: testTelemetry(t)},
		"no token source": {Endpoint: "http://127.0.0.1:8096/v1/deliveries", Timeout: time.Second, Telemetry: testTelemetry(t)},
		"a relative endpoint": {Endpoint: "not-a-url", Tokens: httpdelivery.StaticToken("token"),
			Timeout: time.Second, Telemetry: testTelemetry(t)},
	} {
		if _, err := httpdelivery.NewPublisher(cfg); err == nil {
			t.Errorf("NewPublisher accepted %s; an anonymous intake is a projection anyone can write", name)
		}
	}
}

// countingTokens is a token source that records what the publisher asked of it.
type countingTokens struct {
	tokens, invalidations int
	err                   error
}

func (c *countingTokens) Token(context.Context) (string, error) {
	c.tokens++
	return "workload-token", c.err
}

func (c *countingTokens) Invalidate() { c.invalidations++ }

// A 401 drops the cached token, so the next attempt presents a fresh one; a 403 does not, because
// the credential was understood and refused. Neither is poison.
func TestA401DropsTheTokenAndA403DoesNot(t *testing.T) {
	for status, invalidations := range map[int]int{http.StatusUnauthorized: 1, http.StatusForbidden: 0} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer workload-token" {
				t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(status)
		}))
		tokens := &countingTokens{}
		publisher, err := httpdelivery.NewPublisher(httpdelivery.Config{
			Endpoint: server.URL, Tokens: tokens, Timeout: time.Second, Telemetry: testTelemetry(t)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = publisher.Publish(context.Background(), envelope(t))
		server.Close()
		if err == nil || errors.Is(err, outbox.ErrPoison) {
			t.Errorf("%d answered %v, want retryable", status, err)
		}
		if tokens.invalidations != invalidations {
			t.Errorf("%d invalidated the token %d times, want %d", status, tokens.invalidations, invalidations)
		}
	}
}

// No credential this attempt is retryable: the event is not at fault, and nothing is sent.
func TestAMissingCredentialIsRetryableAndSendsNothing(t *testing.T) {
	sent := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { sent = true }))
	defer server.Close()
	publisher, err := httpdelivery.NewPublisher(httpdelivery.Config{
		Endpoint: server.URL, Tokens: &countingTokens{err: errors.New("token endpoint down")},
		Timeout: time.Second, Telemetry: testTelemetry(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), envelope(t)); err == nil || errors.Is(err, outbox.ErrPoison) {
		t.Errorf("a missing credential answered %v, want retryable", err)
	}
	if sent {
		t.Error("a delivery was sent without a credential")
	}
}
