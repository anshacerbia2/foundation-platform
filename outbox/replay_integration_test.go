package outbox

// Dead-letter replay sufficiency.
//
// A dead letter has to be replayable from its own row. Replaying means appending to
// platform.outbox again, which requires aggregate_id NOT NULL and takes priority to pick
// the lane. Before 0004 platform.dead_letter retained neither, so a replay had to read the
// original outbox row -- and that row lives in a partition with retention. Once it was
// dropped the incident record survived and the ability to act on it did not.
//
// These tests hold that property against the case it fails in: the original row gone.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
)

// retained is everything platform.dead_letter keeps about an abandoned delivery. The field
// set is the point: it is exactly what a replay has to work from.
type retained struct {
	eventType   string
	envelope    []byte
	payload     []byte
	aggregateID *string
	priority    *int16
}

func readDeadLetter(ctx context.Context, t *testing.T, p *db.Pool, eventID string) retained {
	t.Helper()

	var r retained
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT event_type, envelope, payload, aggregate_id::text, priority
			FROM platform.dead_letter WHERE event_id = $1`, eventID,
		).Scan(&r.eventType, &r.envelope, &r.payload, &r.aggregateID, &r.priority)
	}); err != nil {
		t.Fatalf("reading the dead-letter row: %v", err)
	}
	return r
}

// dropOutboxRow removes the original, which is what partition retention eventually does.
// A DELETE rather than a DROP PARTITION so the test does not have to know which daily
// partition the row landed in -- the effect on a replay is the same: the row is not there.
func dropOutboxRow(ctx context.Context, t *testing.T, p *db.Pool, eventID string) {
	t.Helper()

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "DELETE FROM platform.outbox WHERE event_id = $1", eventID)
		return err
	}); err != nil {
		t.Fatalf("deleting the original outbox row: %v", err)
	}
}

// deadLetterOne appends an event, fails it as poison, and returns what was abandoned.
func deadLetterOne(ctx context.Context, t *testing.T, p *db.Pool, opts ...Option) (event.Envelope, id.UUID) {
	t.Helper()

	e, aggregate := appendOne(ctx, t, p, opts...)
	pub := &fakePublisher{err: fmt.Errorf("unregistered type: %w", ErrPoison)}
	d := newTestDispatcher(t, p, pub, Config{})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Fatalf("%d dead-letter rows, want 1", got)
	}
	return e, aggregate
}

// The row must carry the aggregate and the lane, not merely the message. Without them the
// retained envelope describes an event that cannot be sent anywhere.
func TestADeadLetterRetainsWhatAReplayNeeds(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, aggregate := deadLetterOne(ctx, t, p, Priority())

	r := readDeadLetter(ctx, t, p, e.ID.String())

	if r.aggregateID == nil {
		t.Fatal("aggregate_id was not retained, so this row cannot be appended to the outbox again: " +
			"platform.outbox requires it NOT NULL and no other column here supplies it")
	}
	if *r.aggregateID != aggregate.String() {
		t.Errorf("aggregate_id = %s, want %s", *r.aggregateID, aggregate)
	}
	if r.priority == nil {
		t.Fatal("priority was not retained, so a replayed security event would be re-appended " +
			"into the standard lane and lose its reserved workers")
	}
	if *r.priority != PriorityHigh {
		t.Errorf("priority = %d, want %d; the event was appended with Priority()", *r.priority, PriorityHigh)
	}
	if len(r.envelope) == 0 || len(r.payload) == 0 {
		t.Error("envelope or payload is empty on an unresolved row; disposal only clears resolved ones")
	}
}

// The property under the condition that breaks it. The original row is gone, as retention
// eventually makes it, and the replay is driven from the dead-letter row alone.
//
// This is the whole point of 0004: REPLAYED is the only first-hand evidence the resolution
// contract has -- the producer's own dispatcher witnessing the consumer accept the event --
// and before this, an incident old enough to outlive its partition could not produce it.
func TestADeadLetterCanBeReplayedAfterTheOriginalIsGone(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, aggregate := deadLetterOne(ctx, t, p, Priority())
	r := readDeadLetter(ctx, t, p, e.ID.String())

	dropOutboxRow(ctx, t, p, e.ID.String())

	// From here on nothing reads the original. Everything comes out of the dead-letter row,
	// which is what an operator or a resolution endpoint would have.
	var replayed event.Envelope
	if err := json.Unmarshal(r.envelope, &replayed); err != nil {
		t.Fatalf("the retained envelope does not decode, so it cannot be republished: %v", err)
	}

	// Checked rather than dereferenced. With the retention removed these are NULL, and a
	// nil dereference here would abort the whole test binary -- taking the other tests'
	// diagnostics with it and reading like a broken test rather than a caught regression.
	if r.aggregateID == nil || r.priority == nil {
		t.Fatalf("the dead-letter row retained aggregate_id=%v priority=%v, so the abandoned "+
			"event cannot be replayed now that its original outbox row is gone",
			r.aggregateID, r.priority)
	}

	aggregateID, err := id.Parse(*r.aggregateID)
	if err != nil {
		t.Fatalf("the retained aggregate_id does not parse: %v", err)
	}

	var opts []Option
	if *r.priority == PriorityHigh {
		opts = append(opts, Priority())
	}

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, aggregateID, replayed, opts...)
	}); err != nil {
		t.Fatalf("re-appending from the dead-letter row alone: %v", err)
	}

	// And it publishes. A replay that enqueues but cannot be dispatched proves nothing.
	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{})
	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatching the replay: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("the replay published %d envelopes, want 1", pub.count())
	}
	if pub.published[0].ID != e.ID {
		t.Errorf("the replay carried event %s, want the abandoned event %s", pub.published[0].ID, e.ID)
	}

	// The replayed row must be its own row, not a resurrection of the deleted one: the
	// abandoned original was closed with published = TRUE and no published_at, and a replay
	// that reused it would report the same event as both abandoned and delivered.
	row, err := readRow(ctx, p, e.ID.String())
	if err != nil {
		t.Fatalf("reading the replayed row: %v", err)
	}
	if row.aggregateID != aggregate.String() {
		t.Errorf("the replayed row names aggregate %s, want %s", row.aggregateID, aggregate)
	}
	if row.priority != PriorityHigh {
		t.Errorf("the replayed row is in lane %d, want the reserved lane %d", row.priority, PriorityHigh)
	}
}

// The backfill in 0004 is not the same statement as the dispatcher's INSERT, so it gets its
// own check: a row that predates the column must be recovered while its original survives.
func TestTheBackfillRecoversRowsWrittenBeforeTheColumnsExisted(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, aggregate := deadLetterOne(ctx, t, p)

	// Return the row to the state 0003 would have left it in.
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx,
			"UPDATE platform.dead_letter SET aggregate_id = NULL, priority = NULL WHERE event_id = $1",
			e.ID.String())
		return err
	}); err != nil {
		t.Fatalf("clearing the columns: %v", err)
	}

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE platform.dead_letter d
			   SET aggregate_id = o.aggregate_id,
			       priority     = o.priority
			  FROM platform.outbox o
			 WHERE o.event_id = d.event_id
			   AND d.aggregate_id IS NULL`)
		return err
	}); err != nil {
		t.Fatalf("running the backfill: %v", err)
	}

	r := readDeadLetter(ctx, t, p, e.ID.String())
	if r.aggregateID == nil || *r.aggregateID != aggregate.String() {
		t.Errorf("aggregate_id = %v after the backfill, want %s", r.aggregateID, aggregate)
	}
	if r.priority == nil || *r.priority != PriorityStandard {
		t.Errorf("priority = %v after the backfill, want %d", r.priority, PriorityStandard)
	}
}

// And the honest limit of the backfill, stated as a test so nobody reads NULL as a bug.
//
// A row whose original is already gone recovers nothing, and that is correct: the data does
// not exist any more. NULL says "this row cannot replay itself", which an alert can act on.
// Guessing an aggregate_id would be worse than leaving it absent -- it would name a real
// aggregate somewhere, and the replay would deliver a security event attributed to the
// wrong subject.
func TestTheBackfillLeavesUnrecoverableRowsNull(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := deadLetterOne(ctx, t, p)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx,
			"UPDATE platform.dead_letter SET aggregate_id = NULL, priority = NULL WHERE event_id = $1",
			e.ID.String())
		return err
	}); err != nil {
		t.Fatalf("clearing the columns: %v", err)
	}
	dropOutboxRow(ctx, t, p, e.ID.String())

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE platform.dead_letter d
			   SET aggregate_id = o.aggregate_id,
			       priority     = o.priority
			  FROM platform.outbox o
			 WHERE o.event_id = d.event_id
			   AND d.aggregate_id IS NULL`)
		return err
	}); err != nil {
		t.Fatalf("running the backfill: %v", err)
	}

	r := readDeadLetter(ctx, t, p, e.ID.String())
	if r.aggregateID != nil {
		t.Errorf("aggregate_id = %s for a row whose original is gone; the backfill invented a value",
			*r.aggregateID)
	}
	// The incident itself must survive. Losing the record along with the data would remove
	// the alert that says a delivery was abandoned.
	if r.eventType == "" || len(r.envelope) == 0 {
		t.Error("the incident record lost its content when its original was deleted")
	}
}
