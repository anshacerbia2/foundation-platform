package outbox

// The preflight's failing branches, provoked here rather than left to a consuming repository.
//
// This suite runs as the owner of its own database, so every privilege question answers true and
// every table exists. Left alone, the check would be a mechanism whose red had never been seen —
// which is indistinguishable from one that cannot go red, and is the standard this estate applies
// to every other gate it keeps.
//
// Both conditions are therefore built on purpose: a database whose migration state stops short of
// the table, and a login role deliberately missing a privilege. Neither touches the shared test
// database's schema — dropping a table other tests need would trade one silent failure for
// another.

import (
	"context"
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

// adminDSN is the connection the suite already uses, which owns its database.
func adminDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_INTEGRATION") != "" {
			t.Fatal("REQUIRE_INTEGRATION is set and TEST_DATABASE_URL is empty")
		}
		t.Skip("TEST_DATABASE_URL is unset")
	}
	return dsn
}

// TestRunRefusesWhenARequiredTableIsMissing builds a database whose migrations stop before the
// table the current dispatcher writes to, which is exactly the version skew this check exists for:
// this module a release ahead of the schema the consuming service has applied.
//
// A separate database rather than a DROP against the shared one. The shared database is what every
// other test in this package reads, and removing a table from underneath them would produce
// failures with nothing to do with what they assert.
func TestRunRefusesWhenARequiredTableIsMissing(t *testing.T) {
	admin := requireDatabase(t)
	ctx := boundedContext(t)

	// CREATE DATABASE cannot run inside a transaction block, and db.Pool exposes no path that is
	// not one — deliberately, since every statement this module issues belongs in a transaction.
	// A lone pgx connection is the exception, and it is confined to these two statements.
	name := fmt.Sprintf("platform_preflight_%d", time.Now().UnixNano())
	outsideTx(t, ctx, "CREATE DATABASE "+name)
	t.Cleanup(func() {
		outsideTx(t, context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	_ = admin

	behind, err := db.Open(ctx, db.Config{Name: "preflight-behind", DSN: replaceDatabase(adminDSN(t), name), MaxConns: 2})
	if err != nil {
		t.Fatalf("opening the throwaway database: %v", err)
	}
	t.Cleanup(behind.Close)

	// Every migration except the one that adds the table the current dispatcher needs. That is
	// the state a consuming service is in while it is a version behind.
	set, err := migrations.PlatformMigrations()
	if err != nil {
		t.Fatalf("PlatformMigrations: %v", err)
	}
	for _, migration := range set {
		if strings.Contains(migration.Name, "delivery_receipt") {
			break
		}
		if err := behind.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, migration.SQL)
			return err
		}); err != nil {
			t.Fatalf("applying %s: %v", migration.Name, err)
		}
	}

	dispatcher := newTestDispatcher(t, behind, &fakePublisher{}, Config{Consumer: "preflight"})

	runErr := dispatcher.Run(ctx)
	if runErr == nil {
		t.Fatal("Run started against a database with no platform.delivery_receipt; every publication " +
			"would have failed one event at a time, forever")
	}
	if !errors.Is(runErr, ErrPrerequisite) {
		t.Fatalf("Run returned %v, which is not classified as a contract failure; an operator "+
			"cannot tell it from an outage", runErr)
	}
	if !strings.Contains(runErr.Error(), "platform.delivery_receipt") {
		t.Errorf("the diagnostic does not name the missing table: %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "migrations") {
		t.Errorf("the diagnostic does not say what to do about it: %v", runErr)
	}
}

// TestRunRefusesWhenAPrivilegeIsMissing is the other half, and the one the owner-run suite could
// never have reached: the tables are all there and the role cannot write one of them.
func TestRunRefusesWhenAPrivilegeIsMissing(t *testing.T) {
	admin := requireDatabase(t)
	ctx := boundedContext(t)

	role := fmt.Sprintf("preflight_underprivileged_%d", time.Now().UnixNano())
	password := "preflight"

	if err := admin.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"CREATE ROLE %s LOGIN PASSWORD '%s'", role, password)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA platform TO %s", role)); err != nil {
			return err
		}
		// Everything the contract asks for except INSERT on the receipt table. The dispatcher
		// would otherwise reach its first publication before discovering this.
		for statement, on := range map[string]string{
			"GRANT SELECT, UPDATE ON platform.outbox TO %s":      "outbox",
			"GRANT INSERT, SELECT ON platform.dead_letter TO %s": "dead_letter",
			"GRANT SELECT ON platform.delivery_receipt TO %s":    "delivery_receipt",
		} {
			if _, err := tx.Exec(ctx, fmt.Sprintf(statement, role)); err != nil {
				return fmt.Errorf("granting on %s: %w", on, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("creating the under-privileged role: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			_, _ = tx.Exec(ctx, fmt.Sprintf("REASSIGN OWNED BY %s TO CURRENT_USER", role))
			_, _ = tx.Exec(ctx, fmt.Sprintf("DROP OWNED BY %s", role))
			_, _ = tx.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", role))
			return nil
		})
	})

	limited, err := db.Open(ctx, db.Config{
		Name: "preflight-limited", DSN: replaceCredentials(adminDSN(t), role, password), MaxConns: 2})
	if err != nil {
		t.Fatalf("opening the under-privileged pool: %v", err)
	}
	t.Cleanup(limited.Close)

	dispatcher := newTestDispatcher(t, limited, &fakePublisher{}, Config{Consumer: "preflight"})

	runErr := dispatcher.Run(ctx)
	if runErr == nil {
		t.Fatal("Run started as a role that cannot write platform.delivery_receipt")
	}
	if !errors.Is(runErr, ErrPrerequisite) {
		t.Fatalf("Run returned %v, which is not classified as a contract failure", runErr)
	}
	if !strings.Contains(runErr.Error(), "INSERT") || !strings.Contains(runErr.Error(), "platform.delivery_receipt") {
		t.Errorf("the diagnostic does not name the missing privilege and table: %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "grant") {
		t.Errorf("the diagnostic does not distinguish a missing grant from a missing migration: %v", runErr)
	}
}

// And the case that must NOT refuse, so the two above are not passing because Run refuses always.
func TestRunStartsWhenTheContractIsSatisfied(t *testing.T) {
	pool := requireDatabase(t)

	if err := CheckDispatcherPrerequisites(context.Background(), pool); err != nil {
		t.Fatalf("the prerequisites are unmet against the suite's own database: %v", err)
	}
}

// replaceDatabase swaps the database name in a DSN, keeping credentials and host.
func replaceDatabase(dsn, name string) string {
	if index := strings.LastIndex(dsn, "/"); index >= 0 {
		rest := ""
		if query := strings.Index(dsn[index:], "?"); query >= 0 {
			rest = dsn[index+query:]
		}
		return dsn[:index+1] + name + rest
	}
	return dsn
}

// replaceCredentials swaps the user and password in a DSN, keeping host and database.
func replaceCredentials(dsn, user, password string) string {
	scheme := "postgres://"
	rest := strings.TrimPrefix(dsn, scheme)
	if at := strings.Index(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	return fmt.Sprintf("%s%s:%s@%s", scheme, user, password, rest)
}

// outsideTx is CREATE/DROP DATABASE, which PostgreSQL refuses inside a transaction block. The
// driver is confined to db/ by arch.json, so the connection is opened there rather than here.
func outsideTx(t *testing.T, ctx context.Context, statement string) {
	t.Helper()
	if err := dbtest.ExecOutsideTransaction(ctx, adminDSN(t), statement); err != nil {
		t.Fatalf("%v", err)
	}
}

// boundedContext bounds Run, and the bound is part of what these tests assert.
//
// Run is designed never to return until its context is cancelled: with the preflight satisfied it
// starts workers and blocks. So a refusal test given context.Background() does not fail when the
// preflight is removed -- it HANGS, until the Go test timeout ten minutes later, and reports a
// panic rather than the assertion it was written for.
//
// That was the first thing the mutation of this gate showed, and it is the same defect twice over:
// a check whose absence produces a confusing failure teaches people to distrust the check. Bounded,
// the removal fails in seconds with the message that names it.
func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}
