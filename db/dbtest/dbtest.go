// Package dbtest provides a db.Tx that records statements instead of executing them.
//
// It exists so packages above db can test what they send to the database without a
// database. Those packages are forbidden from naming the driver — arch.json asserts it,
// because a driver change must be one edit rather than an edit in every signature — and
// a hand-written fake would name pgconn.CommandTag in each of them. Owning the fake here
// keeps the driver named beneath db/, which is the point of the rule.
//
// It is deliberately not a mock framework. It records and it fails; it expresses no
// expectations, because a test that asserts on a call it also configured is testing its
// own setup.
package dbtest

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anshacerbia2/foundation-platform/db"
)

// Call is one statement sent to the transaction.
type Call struct {
	SQL  string
	Args []any
}

// Tx is a db.Tx that records Exec calls.
//
// pgx.Tx is embedded and left nil deliberately. Tx therefore satisfies the interface and
// can be passed wherever a real transaction is expected, but any method it does not
// implement panics on a nil interface rather than returning a zero value. That is the
// assertion: code under test must touch only what is implemented here, and reaching for
// anything else fails loudly instead of silently succeeding against a stub.
type Tx struct {
	pgx.Tx

	// ExecErr, when set, is returned by every Exec. The statement is still recorded, so
	// a test can assert both what was attempted and how the failure surfaced.
	ExecErr error

	// Tag is the command tag Exec reports. The zero value reports no rows affected.
	Tag pgconn.CommandTag

	calls []Call
}

// Tx satisfies db.Tx. Asserted at compile time so a driver upgrade that widens the
// interface fails here, in one place, rather than in every package that uses this fake.
var _ db.Tx = (*Tx)(nil)

// Exec records the statement and returns the configured outcome.
func (t *Tx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.calls = append(t.calls, Call{SQL: sql, Args: args})
	if t.ExecErr != nil {
		return pgconn.CommandTag{}, t.ExecErr
	}
	return t.Tag, nil
}

// Calls reports every statement in the order it was sent.
func (t *Tx) Calls() []Call { return t.calls }

// Only reports the single statement sent, and fails the test if the count is anything
// other than one.
//
// It takes a testing.TB rather than panicking so the failure names the test's own file
// and line. A test asserting on a statement that never ran, or on the wrong one of
// several, is broken rather than failing, and it should say so where it is written.
func (t *Tx) Only(tb testingTB) Call {
	tb.Helper()
	if len(t.calls) != 1 {
		tb.Fatalf("expected exactly one statement, got %d", len(t.calls))
		// testing.TB.Fatalf ends the test, so this return is unreachable there. It is
		// reached by a TB that only records the failure, which is how this package
		// tests its own helper.
		return Call{}
	}
	return t.calls[0]
}

// testingTB is the slice of testing.TB this package needs.
//
// Declared here rather than importing testing so that linking this package into a
// production binary does not pull the testing flag set in with it.
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
}
