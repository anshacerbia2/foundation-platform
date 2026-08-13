// Package outbox writes domain events into the transactional outbox that carries them
// between the two control planes.
//
// It is the only supported publication path. There is no broker client in this module,
// and Append requires a transaction handle, so an event cannot be published outside the
// transaction that produced the fact it reports. TDD-foundation-platform-001 states that
// property as structural rather than procedural: a service that mutates state and
// publishes in two transactions does not compile, rather than passing review.
//
// It holds no domain concept. It knows an event has a type, an aggregate, and a payload;
// it does not know what any of them mean.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
)

// Dispatch lanes.
//
// TDD-foundation-platform-001 reserves workers for PriorityHigh so a lifecycle backlog
// cannot delay a revocation. The values are the ones the claim query orders by, ascending,
// so a lower number is dispatched first.
const (
	// PriorityHigh carries security events: revocation, suspension, credential change.
	PriorityHigh int16 = 0

	// PriorityStandard carries lifecycle events and is the default.
	PriorityStandard int16 = 100
)

var (
	// ErrNoTransaction reports Append called with no transaction handle. The handle is
	// the guarantee this package exists to provide, so a nil one is a programming error
	// rather than a runtime condition.
	ErrNoTransaction = errors.New("outbox: a transaction handle is required")

	// ErrNoAggregate reports an absent aggregate identifier. An event that names no
	// subject cannot be triaged from a dead-letter row, which is the moment it is most
	// needed.
	ErrNoAggregate = errors.New("outbox: an aggregate identifier is required")
)

// appendOptions is the resolved set of options for one append. Named apart from Config,
// which configures the dispatcher: one describes a single write, the other a long-running
// process, and a reader should not have to check which.
type appendOptions struct {
	priority int16
}

// Option adjusts how an event is appended.
type Option func(*appendOptions)

// Priority routes the event to the reserved dispatch lane.
//
// It is a function rather than a constant so the call site reads as a decision. An event
// in this lane is never dead-lettered for broker unavailability; it returns to the pool
// with escalating claim backoff, because abandoning a revocation that would have
// published a minute later is not a failure mode the publisher can compensate for.
func Priority() Option {
	return func(o *appendOptions) { o.priority = PriorityHigh }
}

// appendStatement inserts one event.
//
// One nextval supplies both the ordering column and the CloudEvents streamposition
// extension. Keeping the assignment inside this statement makes the row and envelope
// impossible to disagree. created_at, published, attempts, and last_error retain their
// database defaults.
const appendStatement = `WITH positioned AS (
	SELECT nextval('platform.outbox_sequence')::bigint AS stream_position
)
INSERT INTO platform.outbox
	(event_id, sequence, event_type, aggregate_id, priority, payload, envelope)
SELECT $1, positioned.stream_position, $2, $3, $4, $5,
	jsonb_set($6::jsonb, '{streamposition}', to_jsonb(positioned.stream_position), true)
FROM positioned`

// Append writes an event to the outbox inside the caller's transaction.
//
// The event_id column carries the envelope's own identifier rather than a fresh one.
// Consumers deduplicate on the identifier the envelope arrives with, so a generated
// column value would produce a row that no consumer can match, and the effect would be
// applied twice with no error anywhere to show for it. The outbox table defaults
// event_id to gen_random_uuid() for schema convenience; this statement always overrides
// it, and that is not an optimisation to remove.
//
// aggregateID is a required parameter rather than an Option because the column is NOT
// NULL and the value cannot be derived here — it lives inside the payload, whose shape
// only the publishing system knows. An option that must always be supplied is an
// argument wearing the wrong clothes.
func Append(ctx context.Context, tx db.Tx, aggregateID id.UUID, e event.Envelope, opts ...Option) error {
	if db.IsNilTx(tx) {
		return ErrNoTransaction
	}
	if aggregateID.IsNil() {
		return ErrNoAggregate
	}

	// Checked here as a precondition, before any work, so the error names the contract
	// that was broken rather than surfacing as a marshalling failure. Marshalling
	// validates again, because the envelope is about to cross a process boundary.
	if err := e.Validate(); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}

	resolved := appendOptions{priority: PriorityStandard}
	for _, opt := range opts {
		opt(&resolved)
	}

	envelope, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("outbox: marshalling envelope %s: %w", e.ID, err)
	}

	// Identifiers are sent in their canonical string form. The driver accepts a UUID as
	// a string, and the alternative would be for package id to carry a driver type,
	// which would make the identifier format depend on the database client. One
	// allocation per append is not measurable against the round trip it precedes.
	//
	// Data is converted to a plain []byte so the driver plans it as JSON rather than
	// reaching for the json.Marshaler that json.RawMessage also satisfies.
	if _, err := tx.Exec(ctx, appendStatement,
		e.ID.String(),
		string(e.Type),
		aggregateID.String(),
		resolved.priority,
		[]byte(e.Data),
		envelope,
	); err != nil {
		return fmt.Errorf("outbox: appending %s (%s): %w", e.Type, e.ID, err)
	}

	return nil
}
