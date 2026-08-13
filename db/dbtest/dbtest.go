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
	"errors"
	"fmt"
	"reflect"

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

	// RowValues are copied into the destinations passed to QueryRow().Scan.
	RowValues []any

	// RowErr, when set, is returned by QueryRow().Scan.
	RowErr error

	// Rows are returned by Query in order.
	Rows [][]any

	// QueryErr, when set, is returned directly by Query.
	QueryErr error

	// RowsErr is reported after Query rows are exhausted.
	RowsErr error

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

// QueryRow records the statement and returns a row backed by RowValues.
func (t *Tx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.calls = append(t.calls, Call{SQL: sql, Args: args})
	return row{values: t.RowValues, err: t.RowErr}
}

// Query records the statement and returns the configured result set.
func (t *Tx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.calls = append(t.calls, Call{SQL: sql, Args: args})
	if t.QueryErr != nil {
		return nil, t.QueryErr
	}
	return &rows{values: t.Rows, err: t.RowsErr}, nil
}

// CommandTag constructs a command tag without leaking the driver into a caller's tests.
func CommandTag(rowsAffected int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAffected))
}

type row struct {
	values []any
	err    error
}

func (r row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scan(r.values, dest)
}

func scan(values []any, dest []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("dbtest: Scan has %d destinations for %d values", len(dest), len(values))
	}

	for i := range dest {
		if dest[i] == nil {
			continue
		}
		dst := reflect.ValueOf(dest[i])
		if dst.Kind() != reflect.Pointer || dst.IsNil() {
			return fmt.Errorf("dbtest: Scan destination %d is not a non-nil pointer", i)
		}
		if values[i] == nil {
			dst.Elem().SetZero()
			continue
		}

		src := reflect.ValueOf(values[i])
		if src.Type().AssignableTo(dst.Elem().Type()) {
			dst.Elem().Set(src)
			continue
		}
		if src.Type().ConvertibleTo(dst.Elem().Type()) {
			dst.Elem().Set(src.Convert(dst.Elem().Type()))
			continue
		}
		return fmt.Errorf("dbtest: Scan value %d of type %s cannot populate %s", i, src.Type(), dst.Elem().Type())
	}
	return nil
}

var _ pgx.Row = row{}

type rows struct {
	values  [][]any
	err     error
	current int
	closed  bool
}

func (r *rows) Close() { r.closed = true }

func (r *rows) Err() error {
	if !r.closed {
		return nil
	}
	return r.err
}

func (r *rows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *rows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *rows) Next() bool {
	if r.closed || r.current >= len(r.values) {
		r.Close()
		return false
	}
	r.current++
	return true
}

func (r *rows) Scan(dest ...any) error {
	if r.current == 0 || r.current > len(r.values) {
		r.Close()
		return errors.New("dbtest: Scan called without a current row")
	}
	if err := scan(r.values[r.current-1], dest); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *rows) Values() ([]any, error) {
	if r.current == 0 || r.current > len(r.values) {
		return nil, errors.New("dbtest: Values called without a current row")
	}
	return append([]any(nil), r.values[r.current-1]...), nil
}

func (r *rows) RawValues() [][]byte { return nil }

func (r *rows) Conn() *pgx.Conn { return nil }

var _ pgx.Rows = (*rows)(nil)

// ErrNoRows is returned by tests that need to model a query with no result.
var ErrNoRows = errors.New("dbtest: no rows")

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
