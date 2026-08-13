package outbox

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
	"github.com/anshacerbia2/foundation-platform/migrations"
)

// These tests answer what a fake structurally cannot. The unit tests assert which
// arguments Append sends; only a database says whether the driver encodes an identifier
// as uuid, a []byte as jsonb, and whether the DDL parses at all. Parameter encoding and
// schema validity are the two defects that survive every test written against a double.
//
// They are skipped rather than gated behind a build tag, so the file is compiled, vetted,
// and type-checked on every run. A tagged file rots silently until the day the tag is
// finally set.

// pool is opened once for the package when TEST_DATABASE_URL is set.
var pool *db.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		// A suite that skips silently is worse than one that is absent, because the run
		// is green either way and only one of those means anything was checked. CI sets
		// REQUIRE_INTEGRATION, so a misconfigured service container fails the build
		// instead of quietly removing the only tests that touch a database.
		if os.Getenv("REQUIRE_INTEGRATION") != "" {
			fmt.Fprintln(os.Stderr,
				"REQUIRE_INTEGRATION is set but TEST_DATABASE_URL is empty: the integration suite would have skipped silently")
			os.Exit(1)
		}
		os.Exit(m.Run())
	}

	ctx := context.Background()

	p, err := db.Open(ctx, db.Config{Name: "integration", DSN: dsn})
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening the test database: %v\n", err)
		os.Exit(1)
	}
	pool = p

	if err := resetSchema(ctx, p); err != nil {
		fmt.Fprintf(os.Stderr, "preparing the platform schema: %v\n", err)
		p.Close()
		os.Exit(1)
	}

	code := m.Run()
	p.Close()
	os.Exit(code)
}

// resetSchema drops and rebuilds the platform schema so a run never inherits state from
// the one before it. A test that passes only against a schema an earlier run left behind
// is not a test of this migration set.
func resetSchema(ctx context.Context, p *db.Pool) error {
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "DROP SCHEMA IF EXISTS platform CASCADE")
		return err
	}); err != nil {
		return fmt.Errorf("dropping the schema: %w", err)
	}

	set, err := migrations.PlatformMigrations()
	if err != nil {
		return err
	}

	for _, mig := range set {
		if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			// Sent with no arguments, so the driver uses the simple protocol and the
			// file's several statements run as one unit. Whether that holds is itself
			// something only a database can confirm.
			_, err := tx.Exec(ctx, mig.SQL)
			return err
		}); err != nil {
			return fmt.Errorf("applying %s: %w", mig.Name, err)
		}
	}
	return nil
}

func requireDatabase(t *testing.T) *db.Pool {
	t.Helper()
	if pool == nil {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	return pool
}

// storedRow is the subset of platform.outbox these tests read back.
type storedRow struct {
	eventID     string
	eventType   string
	aggregateID string
	priority    int16
	published   bool
	attempts    int32
	sequence    int64
	payloadKind string
	envelopeAt  string
}

func readRow(ctx context.Context, p *db.Pool, eventID string) (storedRow, error) {
	var r storedRow
	err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT event_id::text,
			       event_type,
			       aggregate_id::text,
			       priority,
			       published,
			       attempts,
			       sequence,
			       jsonb_typeof(payload),
			       envelope ->> 'type'
			FROM platform.outbox
			WHERE event_id = $1`, eventID,
		).Scan(&r.eventID, &r.eventType, &r.aggregateID, &r.priority,
			&r.published, &r.attempts, &r.sequence, &r.payloadKind, &r.envelopeAt)
	})
	return r, err
}

func appendOne(ctx context.Context, t *testing.T, p *db.Pool, opts ...Option) (event.Envelope, id.UUID) {
	t.Helper()

	e := newEnvelope(t)
	aggregate := newAggregateID(t)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, aggregate, e, opts...)
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return e, aggregate
}

// The encoding test. Every column here is a type the driver had to convert into: uuid
// from a string, jsonb from a []byte, smallint from an int16.
func TestAppendWritesARowTheDatabaseAccepts(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	e, aggregate := appendOne(ctx, t, p)

	row, err := readRow(ctx, p, e.ID.String())
	if err != nil {
		t.Fatalf("reading the row back: %v", err)
	}

	if row.eventID != e.ID.String() {
		t.Errorf("event_id = %s, want %s", row.eventID, e.ID)
	}
	if row.aggregateID != aggregate.String() {
		t.Errorf("aggregate_id = %s, want %s", row.aggregateID, aggregate)
	}
	if row.eventType != testType {
		t.Errorf("event_type = %s, want %s", row.eventType, testType)
	}
	if row.payloadKind != "object" {
		t.Errorf("payload stored as jsonb %q, want an object; the driver encoded it as something else", row.payloadKind)
	}
	if row.envelopeAt != testType {
		t.Errorf("envelope ->> 'type' = %q, want %q; the envelope did not store as queryable jsonb", row.envelopeAt, testType)
	}
}

// The defaults the dispatcher relies on. If published defaulted to true, every event
// would be born already sent.
func TestAppendedRowStartsUnpublishedWithNoAttempts(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	e, _ := appendOne(ctx, t, p)

	row, err := readRow(ctx, p, e.ID.String())
	if err != nil {
		t.Fatalf("reading the row back: %v", err)
	}

	if row.published {
		t.Error("published defaulted to true; the row would never be dispatched")
	}
	if row.attempts != 0 {
		t.Errorf("attempts = %d, want 0", row.attempts)
	}
	if row.priority != PriorityStandard {
		t.Errorf("priority = %d, want %d", row.priority, PriorityStandard)
	}
}

func TestPriorityIsPersistedAsTheReservedLane(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	e, _ := appendOne(ctx, t, p, Priority())

	row, err := readRow(ctx, p, e.ID.String())
	if err != nil {
		t.Fatalf("reading the row back: %v", err)
	}
	if row.priority != PriorityHigh {
		t.Errorf("priority = %d, want %d", row.priority, PriorityHigh)
	}
}

// sequence supplies dispatch ordering and the high-water mark a projection consumer
// resumes from, so it must advance and never repeat.
func TestSequenceAdvancesAcrossAppends(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	first, _ := appendOne(ctx, t, p)
	second, _ := appendOne(ctx, t, p)

	a, err := readRow(ctx, p, first.ID.String())
	if err != nil {
		t.Fatalf("reading the first row: %v", err)
	}
	b, err := readRow(ctx, p, second.ID.String())
	if err != nil {
		t.Fatalf("reading the second row: %v", err)
	}

	if b.sequence <= a.sequence {
		t.Errorf("sequence did not advance: %d then %d", a.sequence, b.sequence)
	}
}

// Atomicity, stated by TDD-foundation-platform-001 as the reason Append takes a handle it
// does not open. A failure after the append must leave no row behind.
func TestAFailureAfterAppendRollsTheEventBack(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	e := newEnvelope(t)
	injected := fmt.Errorf("domain rule rejected the mutation")

	err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if appendErr := Append(ctx, tx, newAggregateID(t), e); appendErr != nil {
			return appendErr
		}
		return injected
	})
	if err == nil {
		t.Fatal("InTx returned nil despite the injected failure")
	}

	var count int
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT count(*) FROM platform.outbox WHERE event_id = $1", e.ID.String(),
		).Scan(&count)
	}); err != nil {
		t.Fatalf("counting rows: %v", err)
	}

	if count != 0 {
		t.Errorf("%d row(s) survived a rolled-back transaction; the append was not atomic", count)
	}
}

// The dispatcher's claim query depends on this index existing with this predicate. A
// full scan of a partitioned outbox on every poll is the difference between the 1 s claim
// budget and missing it.
func TestTheUnpublishedIndexExists(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	var definition string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT indexdef FROM pg_indexes
			WHERE schemaname = 'platform' AND indexname = 'outbox_unpublished'`,
		).Scan(&definition)
	}); err != nil {
		t.Fatalf("outbox_unpublished is absent from the applied schema: %v", err)
	}

	lowered := strings.ToLower(definition)
	for _, fragment := range []string{"priority", "sequence", "published = false"} {
		if !strings.Contains(lowered, fragment) {
			t.Errorf("index definition %q omits %q", definition, fragment)
		}
	}
}

// A row must reach a partition. Without one it would be rejected, and the rejection
// would abort the caller's domain transaction.
func TestTheAppendedRowLandsInAPartition(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()

	e, _ := appendOne(ctx, t, p)

	var partition string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT tableoid::regclass::text FROM platform.outbox WHERE event_id = $1", e.ID.String(),
		).Scan(&partition)
	}); err != nil {
		t.Fatalf("locating the partition: %v", err)
	}

	if partition == "" || partition == "platform.outbox" {
		t.Errorf("row reports partition %q; it did not land in a child partition", partition)
	}
	t.Logf("row landed in %s", partition)
}
