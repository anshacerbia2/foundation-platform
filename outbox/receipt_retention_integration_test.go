package outbox

// Receipt retention, against real rows.
//
// A receipt is evidence that a closure was justified. Pruning one that a closure cites, or one an
// open incident might still need, removes the reason an incident was closed or could be. Each case
// below is one of those reasons, and the last is the one the others exist to allow: an old receipt
// nothing needs is removed.

import (
	"context"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
)

// seedReceipt writes a receipt recorded at the given age.
func seedReceipt(ctx context.Context, t *testing.T, p *db.Pool, age time.Duration) (eventID, consumer string) {
	t.Helper()
	consumer = "retention-consumer"
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO platform.delivery_receipt (event_id, consumer, event_type, evidence, recorded_at)
			VALUES (gen_random_uuid(), $1, 'com.scnehaux.test.retention.recorded', 'consumer_applied',
			        statement_timestamp() - make_interval(secs => $2))
			RETURNING event_id::text`, consumer, age.Seconds()).Scan(&eventID)
	}); err != nil {
		t.Fatalf("seeding a receipt: %v", err)
	}
	return eventID, consumer
}

// seedIncident writes a dead letter, closed on reference when reference is not empty.
func seedIncident(ctx context.Context, t *testing.T, p *db.Pool, reference string) {
	t.Helper()
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if reference == "" {
			_, err := tx.Exec(ctx, `
				INSERT INTO platform.dead_letter
				    (event_id, event_type, envelope, payload, failure_class, failure_detail, attempts, first_failed_at)
				VALUES (gen_random_uuid(), 'com.scnehaux.test.retention.failed', '{}'::jsonb, '{}'::jsonb,
				        'poison', 'refused', 3, clock_timestamp())`)
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO platform.dead_letter
			    (event_id, event_type, envelope, payload, failure_class, failure_detail, attempts, first_failed_at,
			     resolved_at, resolution_type, resolved_by, resolution_reference)
			VALUES (gen_random_uuid(), 'com.scnehaux.test.retention.failed', '{}'::jsonb, '{}'::jsonb,
			        'poison', 'refused', 3, clock_timestamp(),
			        clock_timestamp(), 'REPLAYED', 'suite', $1)`, reference)
		return err
	}); err != nil {
		t.Fatalf("seeding an incident: %v", err)
	}
}

func clearDeadLetters(ctx context.Context, t *testing.T, p *db.Pool) {
	t.Helper()
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "TRUNCATE platform.dead_letter")
		return err
	}); err != nil {
		t.Fatalf("clearing dead letters: %v", err)
	}
}

func prune(ctx context.Context, t *testing.T, p *db.Pool, before time.Time) int64 {
	t.Helper()
	var pruned int64
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		pruned, err = PruneDeliveryReceipts(ctx, tx, before)
		return err
	}); err != nil {
		t.Fatalf("PruneDeliveryReceipts: %v", err)
	}
	return pruned
}

func receiptExists(ctx context.Context, t *testing.T, p *db.Pool, eventID, consumer string) bool {
	t.Helper()
	return len(readReceipts(ctx, t, p, eventID)) > 0
}

const retentionAge = 90 * 24 * time.Hour

func TestAnOldReceiptNothingNeedsIsPruned(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearReceipts(ctx, t, p)
	clearDeadLetters(ctx, t, p)

	old, oldConsumer := seedReceipt(ctx, t, p, retentionAge+time.Hour)
	recent, recentConsumer := seedReceipt(ctx, t, p, time.Hour)

	if got := prune(ctx, t, p, time.Now().Add(-retentionAge)); got != 1 {
		t.Errorf("pruned %d receipts, want 1", got)
	}
	if receiptExists(ctx, t, p, old, oldConsumer) {
		t.Error("a receipt past retention that nothing cites survived pruning")
	}
	if !receiptExists(ctx, t, p, recent, recentConsumer) {
		t.Error("a receipt inside retention was pruned; a resolution in progress would not find it")
	}
}

// The closure record is permanent, so the receipt it cites is too. A resolution naming a receipt
// that no longer exists is a closure nobody can check.
func TestAReceiptAClosureCitesIsNeverPruned(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearReceipts(ctx, t, p)
	clearDeadLetters(ctx, t, p)

	cited, consumer := seedReceipt(ctx, t, p, retentionAge+time.Hour)
	seedIncident(ctx, t, p, ReceiptReference(cited, consumer))

	if got := prune(ctx, t, p, time.Now().Add(-retentionAge)); got != 0 {
		t.Errorf("pruned %d receipts, want 0", got)
	}
	if !receiptExists(ctx, t, p, cited, consumer) {
		t.Fatal("the receipt a closure rests on was pruned, and the closure now cites nothing")
	}
}

// While any incident is open, nothing goes. Which receipt could close it is the host's rule, and a
// SUPERSEDED closure reads a receipt for a different event this module cannot identify.
func TestNothingIsPrunedWhileAnIncidentIsOpen(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearReceipts(ctx, t, p)
	clearDeadLetters(ctx, t, p)

	old, consumer := seedReceipt(ctx, t, p, retentionAge+time.Hour)
	seedIncident(ctx, t, p, "")

	if got := prune(ctx, t, p, time.Now().Add(-retentionAge)); got != 0 {
		t.Errorf("pruned %d receipts with an incident open, want 0", got)
	}
	if !receiptExists(ctx, t, p, old, consumer) {
		t.Fatal("a receipt was pruned while an incident was open; it may have been the one that closes it")
	}
}

func TestPruningRequiresABoundary(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := PruneDeliveryReceipts(ctx, tx, time.Time{})
		return err
	}); err == nil {
		t.Fatal("PruneDeliveryReceipts ran without a retention boundary")
	}
}

func TestReceiptReferenceNamesTheTableAndKey(t *testing.T) {
	if got := ReceiptReference("e", "c"); got != "platform.delivery_receipt:e:c" {
		t.Errorf("ReceiptReference = %q", got)
	}
}
