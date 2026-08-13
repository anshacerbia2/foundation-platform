package dbtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestExecRecordsStatementAndArguments(t *testing.T) {
	tx := &Tx{}

	if _, err := tx.Exec(context.Background(), "INSERT INTO t (a, b) VALUES ($1, $2)", 7, "x"); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	call := tx.Only(t)
	if call.SQL != "INSERT INTO t (a, b) VALUES ($1, $2)" {
		t.Errorf("SQL = %q", call.SQL)
	}
	if len(call.Args) != 2 || call.Args[0] != 7 || call.Args[1] != "x" {
		t.Errorf("Args = %v", call.Args)
	}
}

func TestCallsAreReportedInOrder(t *testing.T) {
	tx := &Tx{}
	for _, sql := range []string{"first", "second", "third"} {
		if _, err := tx.Exec(context.Background(), sql); err != nil {
			t.Fatalf("Exec(%s): %v", sql, err)
		}
	}

	got := tx.Calls()
	if len(got) != 3 {
		t.Fatalf("recorded %d statements, want 3", len(got))
	}
	for i, want := range []string{"first", "second", "third"} {
		if got[i].SQL != want {
			t.Errorf("call %d = %q, want %q", i, got[i].SQL, want)
		}
	}
}

// A failing Exec must still record the attempt. A test that can see the failure but not
// the statement that caused it cannot tell a wrong query from a broken connection.
func TestExecErrorIsReturnedAndTheAttemptIsStillRecorded(t *testing.T) {
	sentinel := errors.New("connection reset")
	tx := &Tx{ExecErr: sentinel}

	_, err := tx.Exec(context.Background(), "UPDATE t SET a = 1")
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if call := tx.Only(t); call.SQL != "UPDATE t SET a = 1" {
		t.Errorf("statement was not recorded: %q", call.SQL)
	}
}

func TestOnlyFailsWhenTheStatementCountIsNotOne(t *testing.T) {
	for _, tc := range []struct {
		name  string
		execs int
	}{
		{"none", 0},
		{"two", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &Tx{}
			for i := 0; i < tc.execs; i++ {
				if _, err := tx.Exec(context.Background(), "x"); err != nil {
					t.Fatalf("Exec: %v", err)
				}
			}

			spy := &recordingTB{}
			tx.Only(spy)

			if spy.failure == "" {
				t.Fatal("Only accepted a statement count it should have rejected")
			}
		})
	}
}

// The structural assertion this fake is built on: a method it does not implement must
// panic rather than return a zero value. Without this, a change that made the code under
// test issue an unexpected Query would pass silently against a stub returning nil.
func TestAnUnimplementedMethodPanics(t *testing.T) {
	tx := &Tx{}

	defer func() {
		if recover() == nil {
			t.Fatal("Commit returned instead of panicking; the embedded pgx.Tx is not nil")
		}
	}()

	_ = tx.Commit(context.Background())
}

// recordingTB captures a failure instead of ending the test, so a test can assert that
// a helper reports one.
type recordingTB struct {
	failure string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failure = fmt.Sprintf(format, args...)
}
