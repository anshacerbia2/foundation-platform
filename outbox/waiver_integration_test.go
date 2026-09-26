package outbox

// Waivers, against real rows.
//
// A waiver is an operational exception and never a closure. These cases hold both halves: what a
// waiver may change -- the stale alert, payload disposal, and whether retention pauses -- and the
// schema refusing a waiver that does not say who, why, and until when.

import (
	"context"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
)

// seedWaived writes an unresolved dead letter, dead-lettered age ago, under a waiver that expires
// at until (relative to now; negative means already expired).
func seedWaived(ctx context.Context, t *testing.T, p *db.Pool, age, until time.Duration) string {
	t.Helper()
	var eventID string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO platform.dead_letter
			    (event_id, event_type, envelope, payload, failure_class, failure_detail, attempts,
			     first_failed_at, dead_lettered_at,
			     waived_at, waived_until, waived_by, waiver_reason)
			VALUES (gen_random_uuid(), 'com.scnehaux.test.waiver.failed', '{"a":1}'::jsonb, '{}'::jsonb,
			        'poison', 'refused', 3,
			        now() - make_interval(secs => $1), now() - make_interval(secs => $1),
			        now() - make_interval(secs => $1), now() + make_interval(secs => $2),
			        'operator', 'the consumer was decommissioned')
			RETURNING event_id::text`, age.Seconds(), until.Seconds()).Scan(&eventID)
	}); err != nil {
		t.Fatalf("seeding a waived incident: %v", err)
	}
	return eventID
}

func staleCount(ctx context.Context, t *testing.T, p *db.Pool, olderThan time.Time) int64 {
	t.Helper()
	var n int64
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		n, err = CountStaleUnresolvedDeadLetters(ctx, tx, olderThan)
		return err
	}); err != nil {
		t.Fatalf("CountStaleUnresolvedDeadLetters: %v", err)
	}
	return n
}

// The schema refuses a waiver missing any of its parts, and one that expires before it began.
func TestTheDatabaseRefusesAnIncompleteWaiver(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	for name, columns := range map[string]string{
		"no expiry":       `now(), NULL, 'operator', 'reason'`,
		"no author":       `now(), now() + interval '1 day', NULL, 'reason'`,
		"a blank reason":  `now(), now() + interval '1 day', 'operator', '   '`,
		"expiry in past":  `now(), now() - interval '1 day', 'operator', 'reason'`,
		"expiry at start": `now(), now(), 'operator', 'reason'`,
	} {
		err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO platform.dead_letter
				    (event_id, event_type, envelope, payload, failure_class, failure_detail, attempts,
				     first_failed_at, waived_at, waived_until, waived_by, waiver_reason)
				VALUES (gen_random_uuid(), 'com.scnehaux.test.waiver.failed', '{}'::jsonb, '{}'::jsonb,
				        'poison', 'refused', 3, now(), `+columns+`)`)
			return err
		})
		if err == nil {
			t.Errorf("the database accepted a waiver with %s", name)
		}
	}
}

// An unexpired waiver silences the stale alert; an expired one does not.
func TestAWaiverSilencesTheAlertUntilItExpires(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearDeadLetters(ctx, t, p)

	seedWaived(ctx, t, p, 48*time.Hour, 24*time.Hour)
	if got := staleCount(ctx, t, p, time.Now().Add(-24*time.Hour)); got != 0 {
		t.Errorf("%d stale incidents under an unexpired waiver, want 0", got)
	}

	seedWaived(ctx, t, p, 48*time.Hour, -time.Hour)
	if got := staleCount(ctx, t, p, time.Now().Add(-24*time.Hour)); got != 1 {
		t.Errorf("%d stale incidents with one waiver expired, want 1: a forgotten exception must "+
			"become a question again", got)
	}
}

// A waived payload is disposed after retention, and the incident and its waiver stay.
func TestAWaivedPayloadIsDisposedAfterRetention(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearDeadLetters(ctx, t, p)

	eventID := seedWaived(ctx, t, p, 100*24*time.Hour, 24*time.Hour)
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := DisposeResolvedDeadLetters(ctx, tx, time.Now().Add(-90*24*time.Hour))
		return err
	}); err != nil {
		t.Fatalf("DisposeResolvedDeadLetters: %v", err)
	}

	var disposed, waiverKept, stillOpen bool
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT envelope IS NULL AND payload IS NULL, waiver_reason IS NOT NULL, resolved_at IS NULL
			  FROM platform.dead_letter WHERE event_id = $1::uuid`, eventID).Scan(&disposed, &waiverKept, &stillOpen)
	}); err != nil {
		t.Fatalf("reading the incident: %v", err)
	}
	if !disposed {
		t.Error("a payload waived past retention was kept")
	}
	if !waiverKept || !stillOpen {
		t.Error("disposal changed the incident itself: a waiver is not a closure, and disposal is not one either")
	}
}

// An unexpired waiver does not hold receipt pruning; an unwaived incident still does.
func TestAWaivedIncidentDoesNotSuspendReceiptRetention(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearDeadLetters(ctx, t, p)
	clearReceipts(ctx, t, p)

	seedWaived(ctx, t, p, time.Hour, 24*time.Hour)
	seedReceipt(ctx, t, p, retentionAge+time.Hour)
	if got := prune(ctx, t, p, time.Now().Add(-retentionAge)); got != 1 {
		t.Errorf("pruned %d receipts with only a waived incident open, want 1", got)
	}

	seedIncident(ctx, t, p, "")
	seedReceipt(ctx, t, p, retentionAge+time.Hour)
	if got := prune(ctx, t, p, time.Now().Add(-retentionAge)); got != 0 {
		t.Errorf("pruned %d receipts with an unwaived incident open, want 0", got)
	}
}
