// Package httpdelivery is the Direct Durable Delivery adapter (ADR-GLB-016 §5.4): an
// outbox.Publisher that posts each envelope to one consumer's acceptance API.
// TDD-foundation-platform-001 §HTTP Delivery.
//
// It was foundation-reference's internal/dispatch and moved here when the producer took over its
// consumers' dispatchers (ADR-GLB-018 §5.4), so the producer and the reference consumer run one
// implementation of the contract below.
package httpdelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/observability"
	"github.com/anshacerbia2/foundation-platform/outbox"
)

// maxDetail bounds what a consumer's refusal contributes to a dead-letter row. A consumer
// returning a large body should not be able to fill this database through its error path.
const maxDetail = 4 << 10

// TokenSource supplies the credential each delivery carries. clientauth.Tokens is the production
// one: the producer authenticates as its workload (STD-IAM-001 §3).
type TokenSource interface {
	Token(ctx context.Context) (string, error)
	// Invalidate drops a cached token after the consumer refused it with 401.
	Invalidate()
}

// StaticToken is a fixed credential, for a consumer with no workload identity such as a local
// proof. A production producer uses clientauth.Tokens: a shared static secret is a long-lived
// secret where a managed workload identity is available, which STD-IAM-001 §3 prohibits.
type StaticToken string

// Token returns the fixed credential.
func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

// Invalidate does nothing: a fixed credential has nothing to refresh.
func (StaticToken) Invalidate() {}

// Config is one consumer's acceptance API.
type Config struct {
	// Endpoint is the absolute URL deliveries are posted to.
	Endpoint string
	// Tokens supplies the bearer credential. Required: a delivery is never sent unauthenticated.
	Tokens TokenSource
	// Timeout bounds one publication.
	Timeout   time.Duration
	Telemetry *observability.Telemetry
}

// Publisher posts CloudEvents envelopes to one consumer.
type Publisher struct {
	endpoint  string
	tokens    TokenSource
	client    *http.Client
	telemetry *observability.Telemetry
}

// NewPublisher builds the publisher for one consumer.
func NewPublisher(cfg Config) (*Publisher, error) {
	trimmed := strings.TrimSpace(cfg.Endpoint)
	if trimmed == "" {
		return nil, errors.New("httpdelivery: the consumer endpoint is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("httpdelivery: %q is not an absolute URL", cfg.Endpoint)
	}
	if cfg.Tokens == nil {
		// Refused rather than sent unauthenticated. An intake that accepts anonymous deliveries is
		// a projection anyone can write, and a dispatcher that discovered this per request would
		// dead-letter every event before anyone noticed the credential was missing.
		return nil, errors.New("httpdelivery: a delivery credential is required")
	}
	if static, ok := cfg.Tokens.(StaticToken); ok && strings.TrimSpace(string(static)) == "" {
		return nil, errors.New("httpdelivery: a delivery credential is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("httpdelivery: the publication timeout must be positive")
	}
	if cfg.Telemetry == nil {
		return nil, errors.New("httpdelivery: telemetry is required")
	}
	return &Publisher{
		endpoint:  trimmed,
		tokens:    cfg.Tokens,
		client:    &http.Client{Timeout: cfg.Timeout},
		telemetry: cfg.Telemetry,
	}, nil
}

// Publish delivers one envelope.
//
// The status mapping is the whole contract with the dispatcher, and it decides whether a failure
// retries or dead-letters:
//
//   - 2xx: published.
//   - 400, 409, 422: poison. The consumer will refuse this event identically forever — an
//     unregistered type, a payload it cannot read — so retrying only delays the dead letter and
//     spends the attempt budget of every event behind it.
//   - 401: the cached token is dropped and the delivery is retryable. The consumer may have
//     restarted, or the token expired in flight; the next attempt presents a fresh one.
//   - anything else, including a transport failure and a timeout: retryable. That deliberately
//     includes 403: a withdrawn credential is an operator's problem to fix, and dead-lettering the
//     estate's events because of it would turn a credential rotation into data loss.
//
// A timeout is retryable and is also the ambiguous case: the consumer may have committed and the
// acknowledgement been lost. That is safe only because the consumer deduplicates inside the
// transaction that applies the effect, which foundation-reference asserts.
//
// # What a 2xx establishes, and what it does not
//
// A successful publication returns an outbox.Receipt, and its class comes from the consumer rather
// than from this adapter's opinion. A consumer that applied the event within the delivery says so
// in outbox.ApplicationReceiptHeader, and only that header produces applied evidence -- the class
// the dead-letter resolution contract accepts as proof the consumer holds the event.
//
// This adapter cannot claim it on the consumer's behalf. outbox.Receipt's field is unexported, so
// the only route to the strong class is outbox.ReceiptFromMarker with the header the consumer
// returned. When this file is replaced by a broker client, the broker's acknowledgement carries no
// such header, the receipts become transport evidence, and resolution stops finding proof -- which
// is the intended outcome rather than a regression to work around.
func (p *Publisher) Publish(ctx context.Context, envelope event.Envelope) (outbox.Receipt, error) {
	body, err := json.Marshal(envelope)
	if err != nil {
		// The envelope came out of this system's own outbox and failed to marshal, so no retry
		// will fix it. Poison rather than an endless loop over a row nobody can send.
		return outbox.Receipt{}, fmt.Errorf("%w: encoding %s: %v", outbox.ErrPoison, envelope.ID, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return outbox.Receipt{}, fmt.Errorf("httpdelivery: building the delivery for %s: %w", envelope.ID, err)
	}
	token, err := p.tokens.Token(ctx)
	if err != nil {
		// No credential this attempt: retryable, never poison. The event is not at fault.
		return outbox.Receipt{}, fmt.Errorf("httpdelivery: obtaining the delivery credential for %s: %w", envelope.ID, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)

	// The correlation identifier travels with the delivery, so the producer's log line, this
	// publication, and the consumer's refusal all join on one value. Without it the end-to-end
	// delay is two stopwatches nobody can reconcile.
	if correlation, ok := correlationOf(ctx, envelope); ok {
		request.Header.Set("X-Correlation-Id", correlation)
	}

	response, err := p.client.Do(request)
	if err != nil {
		// Includes the timeout, and therefore includes the case where the consumer committed and
		// the response was lost. Retryable on purpose.
		return outbox.Receipt{}, fmt.Errorf("httpdelivery: delivering %s: %w", envelope.ID, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// The header is read before the body is drained, because draining can fail and the
		// evidence class is already decided by this point.
		//
		// A 2xx without the header is not an error and not applied evidence: an older consumer,
		// or a proxy that answered on its behalf, delivered something to somebody. The receipt
		// records what was actually established rather than what the status code suggests.
		marker := response.Header.Get(outbox.ApplicationReceiptHeader)
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxDetail))
		return outbox.ReceiptFromMarker(marker), nil
	}

	detail, _ := io.ReadAll(io.LimitReader(response.Body, maxDetail))
	trimmed := strings.TrimSpace(string(detail))

	switch response.StatusCode {
	case http.StatusUnauthorized:
		p.tokens.Invalidate()
		return outbox.Receipt{}, fmt.Errorf("httpdelivery: the consumer refused the credential for %s with 401: %s",
			envelope.ID, trimmed)
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return outbox.Receipt{}, fmt.Errorf("%w: the consumer refused %s with %d: %s",
			outbox.ErrPoison, envelope.ID, response.StatusCode, trimmed)
	default:
		return outbox.Receipt{}, fmt.Errorf("httpdelivery: the consumer answered %d for %s: %s",
			response.StatusCode, envelope.ID, trimmed)
	}
}

// correlationOf names the correlation identifier a delivery carries (TDD-foundation-platform-001
// §HTTP Delivery).
//
// The context's wins when it carries one: it is the caller's explicit statement about this
// publication, such as a replay run under an operator's incident correlation, and it was the only
// source before the envelope's was read, so a caller that already sets one sees no change.
//
// Otherwise the envelope's data.correlation_id, where observability.Metadata puts it. The
// dispatcher publishes on its own context, which carries none, so without this fallback a
// dispatched delivery never carried the header at all.
//
// A payload that is not an object, or whose value is not a valid identifier, yields no header
// rather than an error. The event is not at fault, and a malformed value must not reach a header.
func correlationOf(ctx context.Context, envelope event.Envelope) (string, bool) {
	if correlation, ok := observability.CorrelationID(ctx); ok {
		return correlation.String(), true
	}
	var data struct {
		CorrelationID json.RawMessage `json:"correlation_id"`
	}
	if len(envelope.Data) == 0 || json.Unmarshal(envelope.Data, &data) != nil || len(data.CorrelationID) == 0 {
		return "", false
	}
	var metadata observability.Metadata
	if json.Unmarshal(data.CorrelationID, &metadata.CorrelationID) != nil || metadata.CorrelationID.IsNil() {
		return "", false
	}
	return metadata.CorrelationID.String(), true
}
