package idempotency

// Withholding and expiry, against PostgreSQL.
//
// Both properties are statements about what the database holds: that the recorded response carries
// no secret after jsonb has normalised it, and which rows the purge deletes by the database's own
// clock. A double would only assert itself.
//
// A database of its own, created for this suite and dropped after it, like e2e's. `go test ./...`
// runs package binaries concurrently against one TEST_DATABASE_URL, and the outbox and migrations
// suites drop and rebuild the platform schema in it; a third suite rebuilding it at the same time
// races them on CREATE SCHEMA. Each test then runs in one transaction and rolls back, so the tests
// see none of each other's rows.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/migrations"
)

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
	os.Exit(runWithThrowawayDatabase(m, dsn))
}

func runWithThrowawayDatabase(m *testing.M, dsn string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := fmt.Sprintf("platform_idempotency_%d", time.Now().UnixNano())
	if err := dbtest.ExecOutsideTransaction(ctx, dsn, "CREATE DATABASE "+name); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer func() {
		if err := dbtest.ExecOutsideTransaction(context.Background(), dsn,
			"DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
	}()

	p, err := db.Open(ctx, db.Config{Name: "idempotency-integration", DSN: replaceDatabase(dsn, name)})
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening the throwaway database: %v\n", err)
		return 1
	}
	defer p.Close()

	set, err := migrations.PlatformMigrations()
	if err != nil {
		fmt.Fprintf(os.Stderr, "PlatformMigrations: %v\n", err)
		return 1
	}
	for _, migration := range set {
		if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, migration.SQL)
			return err
		}); err != nil {
			fmt.Fprintf(os.Stderr, "applying %s: %v\n", migration.Name, err)
			return 1
		}
	}
	pool = p
	return m.Run()
}

// replaceDatabase swaps the database name in a URL-form DSN, keeping its query string.
func replaceDatabase(dsn, name string) string {
	index := strings.LastIndex(dsn, "/")
	if index < 0 {
		return dsn
	}
	rest := ""
	if query := strings.Index(dsn[index:], "?"); query >= 0 {
		rest = dsn[index+query:]
	}
	return dsn[:index+1] + name + rest
}

var errRollback = errors.New("rolling back the idempotency probe")

// inRolledBackTx hands probe a transaction on the migrated throwaway database and rolls it back.
func inRolledBackTx(t *testing.T, probe func(ctx context.Context, tx db.Tx)) {
	t.Helper()
	if pool == nil {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	err := pool.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		probe(ctx, tx)
		return errRollback
	})
	if err != nil && !errors.Is(err, errRollback) {
		t.Fatalf("%v", err)
	}
}

// TestAReplayAnswersTheResultWithoutTheSecret is STD-GLB-001 1.7.0's test for a command that
// answers a secret: its idempotency record holds no secret, and a replay answers without it.
func TestAReplayAnswersTheResultWithoutTheSecret(t *testing.T) {
	inRolledBackTx(t, func(ctx context.Context, tx db.Tx) {
		const secret = "inv_9f8e7d6c5b4a"
		live := json.RawMessage(`{"invitation":{"invitation_id":"019a0000-0000-7000-8000-000000000001","state":"pending"},"token":"` + secret + `"}`)

		if _, err := Claim(ctx, tx, "caller-1", "key-1", "digest-1"); err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if err := CompleteWithheld(ctx, tx, "caller-1", "key-1", "digest-1", 201, live, "/token"); err != nil {
			t.Fatalf("CompleteWithheld: %v", err)
		}

		var record string
		if err := tx.QueryRow(ctx,
			`SELECT response_body::text FROM platform.idempotency_key WHERE scope = 'caller-1' AND key = 'key-1'`,
		).Scan(&record); err != nil {
			t.Fatalf("reading the record: %v", err)
		}
		if strings.Contains(record, secret) || strings.Contains(record, `"token"`) {
			t.Errorf("the idempotency record holds the secret: %s", record)
		}

		replay, err := Claim(ctx, tx, "caller-1", "key-1", "digest-1")
		if err != nil {
			t.Fatalf("replaying: %v", err)
		}
		if replay.State != StateReplay || replay.Status != 201 {
			t.Fatalf("replay = %+v", replay)
		}
		var answered map[string]json.RawMessage
		if err := json.Unmarshal(replay.Body, &answered); err != nil {
			t.Fatalf("replay body: %v", err)
		}
		if _, ok := answered["token"]; ok {
			t.Errorf("the replay answers the secret: %s", replay.Body)
		}
		if _, ok := answered["invitation"]; !ok {
			t.Errorf("the replay does not answer the result: %s", replay.Body)
		}
	})
}

// TestPurgeDeletesOnlyCompletedKeysPastTheWindow covers every row the purge must keep and the one it
// must not.
func TestPurgeDeletesOnlyCompletedKeysPastTheWindow(t *testing.T) {
	inRolledBackTx(t, func(ctx context.Context, tx db.Tx) {
		// completed_at is set relative to now(), the clock the purge reads.
		if _, err := tx.Exec(ctx, `
			INSERT INTO platform.idempotency_key
			    (scope, key, request_digest, response_status, response_body, claimed_at, completed_at)
			VALUES
			    ('s', 'expired',        'd', 201, '{}', now() - interval '73 hours', now() - interval '72 hours 1 second'),
			    ('s', 'at-the-boundary','d', 201, '{}', now() - interval '73 hours', now() - interval '72 hours'),
			    ('s', 'recent',         'd', 201, '{}', now() - interval '2 hours',  now() - interval '1 hour'),
			    ('s', 'in-progress',    'd', NULL, NULL, now() - interval '30 days', NULL)`); err != nil {
			t.Fatalf("seeding keys: %v", err)
		}

		purged, err := PurgeCompleted(ctx, tx, 72*time.Hour)
		if err != nil {
			t.Fatalf("PurgeCompleted: %v", err)
		}
		if purged != 2 {
			t.Errorf("purged %d keys, want 2", purged)
		}

		rows, err := tx.Query(ctx, `SELECT key FROM platform.idempotency_key ORDER BY key`)
		if err != nil {
			t.Fatalf("listing keys: %v", err)
		}
		var kept []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				t.Fatalf("scanning: %v", err)
			}
			kept = append(kept, key)
		}
		rows.Close()
		if got := strings.Join(kept, ","); got != "in-progress,recent" {
			t.Errorf("kept %q, want the uncompleted key and the recent one", got)
		}

		// A purged key is a new request: the next claim reserves it again.
		claim, err := Claim(ctx, tx, "s", "expired", "d")
		if err != nil || claim.State != StateClaimed {
			t.Errorf("claiming a purged key = %+v, %v; want a new claim", claim, err)
		}
	})
}

// TestThePurgeFunctionRefusesAWindowUnder24Hours asserts the floor in the database itself, for a
// consumer that calls the function without this package.
func TestThePurgeFunctionRefusesAWindowUnder24Hours(t *testing.T) {
	inRolledBackTx(t, func(ctx context.Context, tx db.Tx) {
		if _, err := tx.Exec(ctx, "SAVEPOINT floor"); err != nil {
			t.Fatalf("savepoint: %v", err)
		}
		var purged int64
		err := tx.QueryRow(ctx, `SELECT platform.purge_idempotency_keys(interval '23 hours 59 minutes')`).Scan(&purged)
		if err == nil || !strings.Contains(err.Error(), "shorter than 24 hours") {
			t.Errorf("a 23h59m window = %d, %v; want refused", purged, err)
		}
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT floor"); err != nil {
			t.Fatalf("rollback to savepoint: %v", err)
		}
		if err := tx.QueryRow(ctx, `SELECT platform.purge_idempotency_keys(interval '24 hours')`).Scan(&purged); err != nil {
			t.Errorf("a 24h window was refused: %v", err)
		}
	})
}

// TestThePurgeIsServedByThePartialIndex asserts that the index the purge reads exists, once, and
// covers completed keys alone.
func TestThePurgeIsServedByThePartialIndex(t *testing.T) {
	inRolledBackTx(t, func(ctx context.Context, tx db.Tx) {
		var definition string
		if err := tx.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE schemaname = 'platform' AND indexname = 'idempotency_key_completed'`,
		).Scan(&definition); err != nil {
			t.Fatalf("idempotency_key_completed: %v", err)
		}
		if !strings.Contains(definition, "(completed_at)") || !strings.Contains(definition, "WHERE (completed_at IS NOT NULL)") {
			t.Errorf("index definition = %s", definition)
		}
	})
}
