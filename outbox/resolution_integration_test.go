package outbox

// The resolution record, and the one thing this schema can enforce about it.
//
// Whether a reason is CORRECT is the consuming system's judgement: REPLAYED is right for a
// Membership revocation under rules this schema has never heard of. What this schema decides is
// whether a resolution is COMPLETE — closed with a stated reason, an author, and a reference to
// whatever the evidence is.
//
// The distinction matters because the incomplete case used to be one statement away. `UPDATE
// platform.dead_letter SET resolved_at = now()` closed an incident silently, and the frontier
// stopped reporting the debt. These cases hold the line that replaced that.

import (
	"context"
	"strings"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db"
)

// abandonedRow writes one unresolved incident and returns its identifier.
func abandonedRow(ctx context.Context, t *testing.T, p *db.Pool) string {
	t.Helper()

	var eventID string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO platform.dead_letter
			    (event_id, event_type, envelope, payload, aggregate_id, priority,
			     failure_class, failure_detail, attempts, first_failed_at)
			VALUES (gen_random_uuid(), 'com.scnehaux.organization.membership.security.revoked',
			        '{"specversion":"1.0"}'::jsonb, '{}'::jsonb, gen_random_uuid(), 0,
			        'poison', 'the consumer refused the envelope', 3, clock_timestamp())
			RETURNING event_id::text`).Scan(&eventID)
	}); err != nil {
		t.Fatalf("seeding the incident: %v", err)
	}
	t.Cleanup(func() {
		_ = p.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			_, _ = tx.Exec(ctx, `DELETE FROM platform.dead_letter WHERE event_id = $1`, eventID)
			return nil
		})
	})
	return eventID
}

func resolve(ctx context.Context, p *db.Pool, eventID, statement string, args ...any) error {
	return p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, statement, append([]any{eventID}, args...)...)
		return err
	})
}

// The statement this constraint exists to stop. It is one line, it needs only UPDATE, and before
// the constraint it closed an incident completely and said nothing about why.
func TestATimestampAloneCannotCloseAnIncident(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	eventID := abandonedRow(ctx, t, p)

	err := resolve(ctx, p, eventID,
		`UPDATE platform.dead_letter SET resolved_at = now() WHERE event_id = $1`)
	if err == nil {
		t.Fatal("an incident was closed with a timestamp and no account of why; the frontier " +
			"would stop reporting the debt and nothing would record what justified it")
	}
	if !strings.Contains(err.Error(), "dead_letter_resolution_complete") {
		t.Errorf("the refusal came from something other than the completeness constraint: %v", err)
	}
}

// A partial resolution is the same absence with more typing.
func TestAPartialResolutionIsRefused(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	partial := map[string]string{
		"reason without author or reference": `UPDATE platform.dead_letter
			SET resolved_at = now(), resolution_type = 'REPLAYED' WHERE event_id = $1`,
		"reason and author without evidence": `UPDATE platform.dead_letter
			SET resolved_at = now(), resolution_type = 'REPLAYED', resolved_by = 'operator'
			WHERE event_id = $1`,
		"evidence without a timestamp": `UPDATE platform.dead_letter
			SET resolution_type = 'REPLAYED', resolved_by = 'operator',
			    resolution_reference = 'receipt' WHERE event_id = $1`,
	}

	for name, statement := range partial {
		t.Run(name, func(t *testing.T) {
			eventID := abandonedRow(ctx, t, p)
			if err := resolve(ctx, p, eventID, statement); err == nil {
				t.Error("a partial resolution was accepted")
			}
		})
	}
}

// Blank is not a value. A resolution recorded as an empty resolution_type satisfies NOT NULL and
// tells an auditor nothing, which is the shape a constraint written carelessly would allow.
//
// This half passed while the NULL half did not: an empty string makes the branch false, and an
// unset column makes it NULL — and a CHECK that evaluates to NULL passes. The constraint refused
// the typo and allowed the omission until the cases below said so.
func TestBlankResolutionFieldsAreRefused(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	blanks := map[string]string{
		"blank type":      `'', 'operator', 'receipt'`,
		"blank author":    `'REPLAYED', '   ', 'receipt'`,
		"blank reference": `'REPLAYED', 'operator', ''`,
	}

	for name, values := range blanks {
		t.Run(name, func(t *testing.T) {
			eventID := abandonedRow(ctx, t, p)
			err := resolve(ctx, p, eventID, `UPDATE platform.dead_letter
				SET resolved_at = now(), resolution_type = `+strings.Split(values, ", ")[0]+`,
				    resolved_by = `+strings.Split(values, ", ")[1]+`,
				    resolution_reference = `+strings.Split(values, ", ")[2]+`
				WHERE event_id = $1`)
			if err == nil {
				t.Error("a resolution with a blank field was accepted")
			}
		})
	}
}

// And the case that must succeed, so the three above are not passing because every update is
// refused.
func TestACompleteResolutionIsAccepted(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	eventID := abandonedRow(ctx, t, p)

	if err := resolve(ctx, p, eventID, `UPDATE platform.dead_letter
		SET resolved_at = now(),
		    resolution_type = 'REPLAYED',
		    resolved_by = 'operator@example.test',
		    resolution_reference = 'delivery_receipt:' || event_id::text
		WHERE event_id = $1`); err != nil {
		t.Fatalf("a complete resolution was refused: %v", err)
	}

	var (
		kind      string
		by        string
		reference string
	)
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT resolution_type, resolved_by, resolution_reference
			  FROM platform.dead_letter WHERE event_id = $1`, eventID).Scan(&kind, &by, &reference)
	}); err != nil {
		t.Fatalf("reading the resolution: %v", err)
	}
	if kind != "REPLAYED" || by == "" || !strings.HasPrefix(reference, "delivery_receipt:") {
		t.Errorf("the resolution recorded %q / %q / %q", kind, by, reference)
	}
}

// Reopening is symmetric: clearing the timestamp must clear the account of why, or the row would
// claim to be an open incident that also carries a resolution.
func TestReopeningMustClearTheWholeRecord(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	eventID := abandonedRow(ctx, t, p)

	if err := resolve(ctx, p, eventID, `UPDATE platform.dead_letter
		SET resolved_at = now(), resolution_type = 'REPLAYED', resolved_by = 'operator',
		    resolution_reference = 'receipt' WHERE event_id = $1`); err != nil {
		t.Fatalf("resolving: %v", err)
	}

	if err := resolve(ctx, p, eventID,
		`UPDATE platform.dead_letter SET resolved_at = NULL WHERE event_id = $1`); err == nil {
		t.Error("an incident was reopened while keeping its resolution record; the row now reports " +
			"an open debt and an account of how it was closed")
	}

	if err := resolve(ctx, p, eventID, `UPDATE platform.dead_letter
		SET resolved_at = NULL, resolution_type = NULL, resolved_by = NULL,
		    resolution_reference = NULL WHERE event_id = $1`); err != nil {
		t.Errorf("a complete reopening was refused: %v", err)
	}
}
