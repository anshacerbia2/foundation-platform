package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/redact"
)

// Subscriptions (ADR-GLB-018 §5.1, TDD-foundation-platform-001 §Per-Consumer Delivery): which event
// types each named consumer is owed. A subscription is replaced rather than edited, and a
// consumer that needs a type it did not receive re-bootstraps from the producer's snapshot.

var (
	// ErrNoConsumer reports a subscription change that names no consumer.
	ErrNoConsumer = errors.New("outbox: a consumer name is required")

	// ErrNoEventTypes reports a subscription to nothing. A consumer owed nothing has no
	// subscription; Unsubscribe is how it says so.
	ErrNoEventTypes = errors.New("outbox: a subscription names at least one event type")

	// ErrStillSubscribed refuses to abandon the deliveries of a consumer that still subscribes:
	// they are owed to a consumer that may receive them.
	ErrStillSubscribed = errors.New("outbox: the consumer still subscribes")

	// ErrNoReason refuses an abandonment that does not say why.
	ErrNoReason = errors.New("outbox: an abandonment must say why")
)

// subscriptionExclusive waits for every transaction already appending to commit, and holds new
// appends until this one commits (see subscriptionLock).
const subscriptionExclusive = `SELECT pg_advisory_xact_lock(hashtextextended('platform.subscription', 0))`

const retireSubscriptionStatement = `UPDATE platform.subscription
SET retired_at = statement_timestamp()
WHERE consumer = $1 AND retired_at IS NULL`

const insertSubscriptionStatement = `INSERT INTO platform.subscription (consumer, event_types)
VALUES ($1, $2)`

// Subscribe replaces the consumer's subscription with these event types, inside the caller's
// transaction. Events appended after it commits owe the consumer a delivery when their type is
// one of these; events committed before it do not, and the consumer bootstraps from a snapshot
// taken after it commits.
func Subscribe(ctx context.Context, tx db.Tx, consumer string, eventTypes []event.Type) error {
	consumer = strings.TrimSpace(consumer)
	switch {
	case db.IsNilTx(tx):
		return ErrNoTransaction
	case consumer == "":
		return ErrNoConsumer
	case len(eventTypes) == 0:
		return ErrNoEventTypes
	}
	types := make([]string, 0, len(eventTypes))
	seen := map[event.Type]bool{}
	for _, t := range eventTypes {
		if _, err := event.ParseType(string(t)); err != nil {
			return fmt.Errorf("outbox: subscribing %s: %w", consumer, err)
		}
		if !seen[t] {
			seen[t] = true
			types = append(types, string(t))
		}
	}
	if _, err := tx.Exec(ctx, subscriptionExclusive); err != nil {
		return fmt.Errorf("outbox: ordering the subscription of %s against appends: %w", consumer, err)
	}
	if _, err := tx.Exec(ctx, retireSubscriptionStatement, consumer); err != nil {
		return fmt.Errorf("outbox: retiring the subscription of %s: %w", consumer, err)
	}
	if _, err := tx.Exec(ctx, insertSubscriptionStatement, consumer, types); err != nil {
		return fmt.Errorf("outbox: subscribing %s: %w", consumer, err)
	}
	return nil
}

// Unsubscribe retires the consumer's subscription. Deliveries already owed stay owed: an event
// committed while the consumer subscribed is still the consumer's to receive or to dead-letter.
func Unsubscribe(ctx context.Context, tx db.Tx, consumer string) error {
	consumer = strings.TrimSpace(consumer)
	switch {
	case db.IsNilTx(tx):
		return ErrNoTransaction
	case consumer == "":
		return ErrNoConsumer
	}
	if _, err := tx.Exec(ctx, subscriptionExclusive); err != nil {
		return fmt.Errorf("outbox: ordering the retirement of %s against appends: %w", consumer, err)
	}
	if _, err := tx.Exec(ctx, retireSubscriptionStatement, consumer); err != nil {
		return fmt.Errorf("outbox: retiring the subscription of %s: %w", consumer, err)
	}
	return nil
}

const activeSubscriptionStatement = `SELECT EXISTS (
    SELECT 1 FROM platform.subscription WHERE consumer = $1 AND retired_at IS NULL)`

// abandonStatement closes what is still owed. published, so retention may drop the day and no
// dispatcher claims it; no published_at, so it never reads as delivered; and no receipt, so no
// closure can cite it. The lease is cleared, so a dispatcher still holding one has its outcome
// discarded by the fence.
const abandonStatement = `UPDATE platform.outbox_delivery
SET published = TRUE, failure_class = $2, last_error = $3,
    next_attempt_at = NULL, lease_id = NULL, leased_until = NULL
WHERE consumer = $1 AND published = FALSE`

// Abandon closes every delivery still owed to a consumer that no longer subscribes, inside the
// caller's transaction, and reports how many it closed (ADR-GLB-018 §5.5).
//
// A retired consumer's dispatcher refuses to start, so nothing delivers these, and left owed they
// would hold retention for every event they belong to. Call it in the transaction that retires the
// consumer, after Unsubscribe. It refuses while the consumer still subscribes, and it takes the
// subscription lock exclusive, so no append writes the consumer a delivery while it runs.
func Abandon(ctx context.Context, tx db.Tx, consumer, reason string) (int64, error) {
	consumer, reason = strings.TrimSpace(consumer), strings.TrimSpace(reason)
	switch {
	case db.IsNilTx(tx):
		return 0, ErrNoTransaction
	case consumer == "":
		return 0, ErrNoConsumer
	case reason == "":
		return 0, ErrNoReason
	}
	if _, err := tx.Exec(ctx, subscriptionExclusive); err != nil {
		return 0, fmt.Errorf("outbox: ordering the abandonment for %s against appends: %w", consumer, err)
	}
	var subscribed bool
	if err := tx.QueryRow(ctx, activeSubscriptionStatement, consumer).Scan(&subscribed); err != nil {
		return 0, fmt.Errorf("outbox: reading the subscription of %s: %w", consumer, err)
	}
	if subscribed {
		return 0, fmt.Errorf("%w: %s; unsubscribe it first", ErrStillSubscribed, consumer)
	}
	tag, err := tx.Exec(ctx, abandonStatement, consumer, string(FailureAbandoned), redact.String(reason))
	if err != nil {
		return 0, fmt.Errorf("outbox: abandoning the deliveries of %s: %w", consumer, err)
	}
	return tag.RowsAffected(), nil
}
