package outbox

// Delivery receipts, and why the strong class cannot be claimed casually.
//
// A dead letter may be resolved as REPLAYED when the producer's own dispatcher witnessed the
// consumer accept the event. That is the only evidence in the resolution contract which does
// not rest on the consumer's report about its own progress, and it is worth exactly as much as
// the guarantee that "the consumer accepted it" was not inferred from something weaker.
//
// Today's transport supports the strong claim: the consumer applies the event inside the same
// transaction as its inbox guard and only then answers. A broker acknowledgement supports only
// the weak one. These tests hold the line between them.

import (
	"context"
	"fmt"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
)

type storedReceipt struct {
	consumer  string
	eventType string
	evidence  string
}

func readReceipts(ctx context.Context, t *testing.T, p *db.Pool, eventID string) []storedReceipt {
	t.Helper()

	var out []storedReceipt
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT consumer, event_type, evidence
			FROM platform.delivery_receipt WHERE event_id = $1
			ORDER BY consumer`, eventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r storedReceipt
			if err := rows.Scan(&r.consumer, &r.eventType, &r.evidence); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	}); err != nil {
		t.Fatalf("reading delivery receipts: %v", err)
	}
	return out
}

func clearReceipts(ctx context.Context, t *testing.T, p *db.Pool) {
	t.Helper()
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "TRUNCATE platform.delivery_receipt")
		return err
	}); err != nil {
		t.Fatalf("clearing delivery receipts: %v", err)
	}
}

// The consumer said it applied the event. That is resolution evidence, and it must survive
// into the table under the consumer's own name.
func TestAConsumerMarkerRecordsAppliedEvidence(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{marker: ApplicationReceiptApplied}
	d := newTestDispatcher(t, p, pub, Config{Consumer: "reference-projection"})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	got := readReceipts(ctx, t, p, e.ID.String())
	if len(got) != 1 {
		t.Fatalf("%d receipts, want 1", len(got))
	}
	if got[0].evidence != string(EvidenceConsumerApplied) {
		t.Errorf("evidence = %q, want %q", got[0].evidence, EvidenceConsumerApplied)
	}
	if got[0].consumer != "reference-projection" {
		t.Errorf("consumer = %q; a receipt that does not name its destination establishes nothing",
			got[0].consumer)
	}
	if got[0].eventType != e.Type.String() {
		t.Errorf("event_type = %q, want %q", got[0].eventType, e.Type)
	}
}

// No marker, so nothing established that the consumer applied anything. This is the shape a
// broker will produce, and it must never read as the strong class.
func TestNoMarkerRecordsTransportEvidenceOnly(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{} // no marker
	d := newTestDispatcher(t, p, pub, Config{Consumer: "broker"})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	got := readReceipts(ctx, t, p, e.ID.String())
	if len(got) != 1 {
		t.Fatalf("%d receipts, want 1", len(got))
	}
	if got[0].evidence != string(EvidenceTransportAccepted) {
		t.Errorf("evidence = %q, want %q -- a publication that carried no consumer assertion "+
			"was recorded as though the consumer had applied the event",
			got[0].evidence, EvidenceTransportAccepted)
	}
}

// A marker that is not the agreed value establishes nothing. It is the case where a consumer
// has been changed and the two sides no longer agree, and the safe reading of "I do not
// recognise this" is the weak class.
func TestAnUnrecognisedMarkerIsNotAppliedEvidence(t *testing.T) {
	for _, marker := range []string{"", "APPLIED", "applied ", "true", "yes", "ok"} {
		if got := ReceiptFromMarker(marker).Evidence(); got != EvidenceTransportAccepted {
			t.Errorf("marker %q produced %q, want %q", marker, got, EvidenceTransportAccepted)
		}
	}
	if got := ReceiptFromMarker(ApplicationReceiptApplied).Evidence(); got != EvidenceConsumerApplied {
		t.Errorf("the agreed marker produced %q, want %q", got, EvidenceConsumerApplied)
	}
}

// A publisher that says nothing about what it established must not have that read as the
// stronger claim. The zero Receipt is what an adapter returns when it was written before this
// distinction existed, or when someone forgot.
func TestAnUnsetReceiptIsTransportEvidence(t *testing.T) {
	if got := (Receipt{}).Evidence(); got != EvidenceTransportAccepted {
		t.Errorf("the zero Receipt reports %q, want %q", got, EvidenceTransportAccepted)
	}
	if got := TransportReceipt().Evidence(); got != EvidenceTransportAccepted {
		t.Errorf("TransportReceipt reports %q, want %q", got, EvidenceTransportAccepted)
	}
}

// A failed publication establishes nothing, so it must leave no receipt. A receipt for a
// delivery that failed would resolve an incident that is still open.
func TestAFailedPublicationLeavesNoReceipt(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{
		marker: ApplicationReceiptApplied, // set, and irrelevant: the publish fails
		err:    fmt.Errorf("unregistered type: %w", ErrPoison),
	}
	d := newTestDispatcher(t, p, pub, Config{Consumer: "reference-projection"})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	if got := readReceipts(ctx, t, p, e.ID.String()); len(got) != 0 {
		t.Errorf("%d receipts for a dead-lettered event, want 0: %+v", len(got), got)
	}
	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Errorf("%d dead-letter rows, want 1", got)
	}
}

// A replay of an event the consumer already applied is a successful no-op at the consumer,
// and the first receipt already records what was established. Overwriting it with a later,
// weaker class would let a replay through a broker erase the evidence a direct delivery
// produced -- which is the evidence a resolution depends on.
func TestAReplayDoesNotWeakenAnExistingReceipt(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)

	e, aggregate := appendOne(ctx, t, p)
	strong := &fakePublisher{marker: ApplicationReceiptApplied}
	d := newTestDispatcher(t, p, strong, Config{Consumer: "reference-projection"})
	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}

	// Re-append the same event_id, as a replay from a dead letter would, and deliver it
	// through a transport that cannot carry the consumer's assertion.
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, aggregate, e)
	}); err != nil {
		t.Fatalf("re-appending: %v", err)
	}
	weak := &fakePublisher{}
	replay := newTestDispatcher(t, p, weak, Config{Consumer: "reference-projection"})
	if _, err := replay.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("replay dispatch: %v", err)
	}

	got := readReceipts(ctx, t, p, e.ID.String())
	if len(got) != 1 {
		t.Fatalf("%d receipts, want 1 -- the key is (event_id, consumer)", len(got))
	}
	if got[0].evidence != string(EvidenceConsumerApplied) {
		t.Errorf("evidence = %q after a transport-only replay, want %q retained",
			got[0].evidence, EvidenceConsumerApplied)
	}
}

// The database refuses a class the contract does not define, which is the second copy of the
// rule. Go's unexported field stops an adapter naming one; this stops anything else.
func TestTheDatabaseRefusesAnUndefinedEvidenceClass(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearReceipts(ctx, t, p)

	err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO platform.delivery_receipt
		    (event_id, consumer, event_type, evidence)
		    VALUES (gen_random_uuid(), 'c', 'x.y.z.a.b.c', 'probably_applied')`)
		return err
	})
	if err == nil {
		t.Fatal("the database accepted an evidence class the contract does not define")
	}
}

// A dispatcher with no consumer name cannot write a receipt that establishes anything, so it
// is refused at construction rather than at the first delivery.
func TestADispatcherWithoutAConsumerIsRefused(t *testing.T) {
	p := requireDatabase(t)

	_, err := NewDispatcher(p, &fakePublisher{}, Config{})
	if err == nil {
		t.Fatal("NewDispatcher accepted an empty consumer name")
	}
	if _, err := NewDispatcher(p, &fakePublisher{}, Config{Consumer: "   "}); err == nil {
		t.Fatal("NewDispatcher accepted a blank consumer name")
	}
}

// Compile-time, and the point of the whole design: outside this package there is no way to
// construct applied evidence except by passing the consumer's own marker.
//
// This is not a runtime assertion, so it is written as an assignment the compiler checks.
// `Receipt{evidence: EvidenceConsumerApplied}` does not compile in another package, which is
// what makes the class a mechanism rather than a convention -- a broker adapter cannot fill
// the field in on a 200 because there is no field to fill.
var _ = func() Receipt {
	// Both routes an external adapter has. Neither can name the strong class by itself.
	if r := TransportReceipt(); r.Evidence() == EvidenceConsumerApplied {
		panic("TransportReceipt produced applied evidence")
	}
	return ReceiptFromMarker(ApplicationReceiptApplied)
}()

var _ Publisher = (*fakePublisher)(nil)
var _ = event.Envelope{}
