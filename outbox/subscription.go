package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
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
