package migrations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db"
)

// This suite exists because of a defect the rest of the suite could not have caught.
//
// Every other integration test in this repository drops and rebuilds the platform schema before
// it runs, which is correct for isolation and is exactly why nobody noticed that the migration
// set could only be applied to an empty schema. A consumer's migration command applies the whole
// set on every invocation — this package ships no revision table — so the first deployment
// succeeded and the second aborted on `column "scope" ... already exists`.
//
// Skipped rather than build-tagged, so the file is compiled and vetted on every run.
//
// EVERY TEST HERE ROLLS BACK. The suite drops and rebuilds the platform schema, and `go test ./...`
// runs package binaries concurrently against one TEST_DATABASE_URL, so a suite that committed
// would delete the schema the outbox suite is mid-way through using. Rolling back means the two
// serialise on locks instead of corrupting each other. It also means this file needs no cleanup
// path: a panic or a failed assertion leaves nothing behind.

var pool *db.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_INTEGRATION") != "" {
			fmt.Fprintln(os.Stderr,
				"REQUIRE_INTEGRATION is set but TEST_DATABASE_URL is empty: the integration suite would have skipped silently")
			os.Exit(1)
		}
		os.Exit(m.Run())
	}

	p, err := db.Open(context.Background(), db.Config{Name: "migrations-integration", DSN: dsn})
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening the test database: %v\n", err)
		os.Exit(1)
	}
	pool = p

	code := m.Run()
	p.Close()
	os.Exit(code)
}

// errRollback unwinds the transaction once the assertions have run. It is not a failure, so it is
// never reported: it is the mechanism by which this suite leaves the database untouched.
var errRollback = errors.New("rolling back the migration probe")

// onCleanSchema drops the platform schema, hands probe a transaction, and rolls back.
//
// Every statement — the drop, the migrations, and the assertions — runs in one transaction.
// PostgreSQL makes DDL transactional, which is what allows a suite that rebuilds a schema from
// scratch to be a read-only operation from the outside.
func onCleanSchema(t *testing.T, probe func(ctx context.Context, tx db.Tx)) {
	t.Helper()
	if pool == nil {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	err := pool.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		if _, execErr := tx.Exec(ctx, "DROP SCHEMA IF EXISTS platform CASCADE"); execErr != nil {
			return fmt.Errorf("dropping the platform schema: %w", execErr)
		}
		probe(ctx, tx)
		return errRollback
	})
	if err != nil && !errors.Is(err, errRollback) {
		t.Fatalf("%v", err)
	}
}

// applyAll applies the whole set in order, the way a consumer's migration command does.
func applyAll(ctx context.Context, tx db.Tx) error {
	set, err := PlatformMigrations()
	if err != nil {
		return fmt.Errorf("PlatformMigrations: %w", err)
	}
	if len(set) == 0 {
		return errors.New("no migrations were embedded")
	}
	for _, migration := range set {
		if _, execErr := tx.Exec(ctx, migration.SQL); execErr != nil {
			return fmt.Errorf("%s: %w", migration.Name, execErr)
		}
	}
	return nil
}

// TestEveryPlatformMigrationIsIdempotent is the property this package owes every consumer that
// applies the set without a revision table. Three applications rather than two, because a
// statement can be wrong in a way that shows only on the transition from "changed once" to
// "changed twice" — a primary key dropped and re-added on each run, for instance, looks correct
// on the second application and fails on the third.
func TestEveryPlatformMigrationIsIdempotent(t *testing.T) {
	onCleanSchema(t, func(ctx context.Context, tx db.Tx) {
		for attempt := 1; attempt <= 3; attempt++ {
			if err := applyAll(ctx, tx); err != nil {
				t.Fatalf("application %d failed; the migration set is not re-runnable: %v", attempt, err)
			}
		}
	})
}

// TestRepeatedApplicationLeavesTheSchemaCorrect states that idempotency is not merely the absence
// of an error. A guard written as "skip when the constraint exists" also passes when the
// constraint was never added, so the outcome is asserted rather than the exit status.
func TestRepeatedApplicationLeavesTheSchemaCorrect(t *testing.T) {
	onCleanSchema(t, func(ctx context.Context, tx db.Tx) {
		for i := 0; i < 2; i++ {
			if err := applyAll(ctx, tx); err != nil {
				t.Fatalf("applying the migration set: %v", err)
			}
		}

		primaryKey := scanOne[string](ctx, t, tx, `
			SELECT string_agg(a.attname, ',' ORDER BY k.ord)
			  FROM pg_constraint c
			  CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
			  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
			 WHERE c.conrelid = 'platform.idempotency_key'::regclass
			   AND c.contype = 'p'`)
		if primaryKey != "scope,key" {
			t.Errorf("primary key = %q, want \"scope,key\"", primaryKey)
		}

		// Each named constraint must exist exactly once. A guard that re-added rather than
		// skipped would produce a duplicate under a generated name and still pass a mere
		// existence check.
		for _, constraint := range []struct {
			table string
			name  string
		}{
			{"platform.idempotency_key", "idempotency_key_scope_valid"},
			{"platform.idempotency_key", "idempotency_key_key_valid"},
			{"platform.idempotency_key", "idempotency_key_digest_valid"},
			{"platform.processed_event", "processed_event_consumer_valid"},
		} {
			count := scanOne[int](ctx, t, tx,
				`SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2`,
				constraint.table, constraint.name)
			if count != 1 {
				t.Errorf("%s on %s: count = %d, want 1", constraint.name, constraint.table, count)
			}
		}

		// A column count guards against a guard that adds the column again under another name.
		scopeColumns := scanOne[int](ctx, t, tx,
			`SELECT count(*) FROM information_schema.columns
			  WHERE table_schema = 'platform' AND table_name = 'idempotency_key' AND column_name = 'scope'`)
		if scopeColumns != 1 {
			t.Errorf("idempotency_key.scope appears %d times, want 1", scopeColumns)
		}
	})
}

// scanOne reads a single value. A failure is fatal: every query here is a schema assertion, and a
// query that could not run says nothing about the schema either way.
func scanOne[T any](ctx context.Context, t *testing.T, tx db.Tx, sql string, args ...any) T {
	t.Helper()
	var value T
	if err := tx.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		t.Fatalf("query failed: %v\n%s", err, sql)
	}
	return value
}
