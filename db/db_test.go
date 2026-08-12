package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// fakeTx embeds the driver interface without satisfying it. Only the methods this
// package calls are implemented, so any other call panics on a nil interface — which
// asserts, structurally, that InTx touches nothing beyond commit and rollback.
type fakeTx struct {
	pgx.Tx
	commits   int
	rollbacks int
	commitErr error
	closed    bool
}

func (f *fakeTx) Commit(context.Context) error {
	if f.closed {
		return pgx.ErrTxClosed
	}
	f.commits++
	if f.commitErr != nil {
		return f.commitErr
	}
	f.closed = true
	return nil
}

func (f *fakeTx) Rollback(context.Context) error {
	if f.closed {
		return pgx.ErrTxClosed
	}
	f.rollbacks++
	f.closed = true
	return nil
}

// fakeBeginner hands out a fresh transaction per Begin, as a pool does. Returning one
// shared instance would make a second transaction inherit the first one's closed state,
// which is a property of the fixture rather than of the code under test.
type fakeBeginner struct {
	txs       []*fakeTx
	begins    int
	beginErr  error
	commitErr error
}

func (f *fakeBeginner) Begin(context.Context) (pgx.Tx, error) {
	f.begins++
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	tx := &fakeTx{commitErr: f.commitErr}
	f.txs = append(f.txs, tx)
	return tx, nil
}

// last returns the most recently begun transaction, or an empty one so a test that
// expected none reports its assertion rather than a nil dereference.
func (f *fakeBeginner) last() *fakeTx {
	if len(f.txs) == 0 {
		return &fakeTx{}
	}
	return f.txs[len(f.txs)-1]
}

func newTestPool(binder SessionBinder) (*Pool, *fakeBeginner) {
	src := &fakeBeginner{}
	return &Pool{name: "test", src: src, binder: binder}, src
}

func noop(context.Context, Tx) error { return nil }

func TestInTxCommitsOnSuccess(t *testing.T) {
	pool, src := newTestPool(nil)

	if err := pool.InTx(context.Background(), noop); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	if src.begins != 1 {
		t.Errorf("begins = %d, want 1", src.begins)
	}
	if got := src.last().commits; got != 1 {
		t.Errorf("commits = %d, want 1", got)
	}
	if got := src.last().rollbacks; got != 0 {
		t.Errorf("rollbacks = %d, want 0", got)
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	pool, src := newTestPool(nil)
	sentinel := errors.New("domain refused the transition")

	err := pool.InTx(context.Background(), func(context.Context, Tx) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("InTx returned %v, want the caller's error unwrapped", err)
	}
	if got := src.last().commits; got != 0 {
		t.Errorf("commits = %d, want 0", got)
	}
	if got := src.last().rollbacks; got != 1 {
		t.Errorf("rollbacks = %d, want 1", got)
	}
}

// A panic that left a transaction open would hold a connection and a row lock until the
// pool recycled it, so the rollback must happen before the panic continues.
func TestInTxRollsBackOnPanicAndRepanics(t *testing.T) {
	pool, src := newTestPool(nil)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("InTx swallowed the panic")
		}
		if r != "boom" {
			t.Errorf("recovered %v, want boom", r)
		}
		if got := src.last().rollbacks; got != 1 {
			t.Errorf("rollbacks = %d, want 1", got)
		}
		if got := src.last().commits; got != 0 {
			t.Errorf("commits = %d, want 0", got)
		}
	}()

	_ = pool.InTx(context.Background(), func(context.Context, Tx) error { panic("boom") })
}

// The binder applies the session scope the transaction requires. Running the caller's
// work without it would execute against an unbound session, which under Row-Level
// Security raises on the first query and under a permissive schema reads the wrong rows.
func TestBinderRunsBeforeCallerAndInsideTransaction(t *testing.T) {
	var order []string

	binder := BinderFunc(func(_ context.Context, tx Tx) error {
		if tx == nil {
			t.Error("binder received no transaction handle")
		}
		order = append(order, "bind")
		return nil
	})

	pool, src := newTestPool(binder)

	err := pool.InTx(context.Background(), func(context.Context, Tx) error {
		order = append(order, "work")
		return nil
	})
	if err != nil {
		t.Fatalf("InTx: %v", err)
	}
	if len(order) != 2 || order[0] != "bind" || order[1] != "work" {
		t.Errorf("order = %v, want [bind work]", order)
	}
	if got := src.last().commits; got != 1 {
		t.Errorf("commits = %d, want 1", got)
	}
}

func TestBinderFailureRollsBackAndSkipsCaller(t *testing.T) {
	sentinel := errors.New("scope unavailable")
	called := false

	pool, src := newTestPool(BinderFunc(func(context.Context, Tx) error { return sentinel }))

	err := pool.InTx(context.Background(), func(context.Context, Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("InTx returned %v, want the binder error", err)
	}
	if called {
		t.Error("caller ran despite the binder failing")
	}
	if got := src.last().rollbacks; got != 1 {
		t.Errorf("rollbacks = %d, want 1", got)
	}
}

// A second transaction on the same call path acquires a second connection, loses
// atomicity between the two, and can wait on a row the outer transaction holds.
func TestNestedTransactionIsRefused(t *testing.T) {
	pool, src := newTestPool(nil)

	err := pool.InTx(context.Background(), func(inner context.Context, _ Tx) error {
		return pool.InTx(inner, func(context.Context, Tx) error {
			t.Error("nested transaction body ran")
			return nil
		})
	})
	if !errors.Is(err, ErrNestedTransaction) {
		t.Fatalf("InTx returned %v, want ErrNestedTransaction", err)
	}
	if src.begins != 1 {
		t.Errorf("begins = %d, want 1; the nested call must not reach the pool", src.begins)
	}
}

// The marker is scoped to the call path, so an unrelated transaction afterwards is
// unaffected.
func TestMarkerDoesNotLeakBeyondTheCallPath(t *testing.T) {
	pool, src := newTestPool(nil)
	ctx := context.Background()

	if err := pool.InTx(ctx, noop); err != nil {
		t.Fatalf("first InTx: %v", err)
	}
	if err := pool.InTx(ctx, noop); err != nil {
		t.Fatalf("second InTx: %v", err)
	}
	if src.begins != 2 {
		t.Errorf("begins = %d, want 2", src.begins)
	}
	for i, tx := range src.txs {
		if tx.commits != 1 {
			t.Errorf("transaction %d commits = %d, want 1", i, tx.commits)
		}
	}
}

func TestCommitFailureIsReported(t *testing.T) {
	pool, src := newTestPool(nil)
	src.commitErr = errors.New("connection lost")

	err := pool.InTx(context.Background(), noop)
	if err == nil {
		t.Fatal("InTx reported success despite a commit failure")
	}
	if !errors.Is(err, src.commitErr) {
		t.Errorf("InTx returned %v, want the commit error", err)
	}
}

func TestBeginFailureIsReported(t *testing.T) {
	pool, src := newTestPool(nil)
	src.beginErr = errors.New("pool exhausted")
	called := false

	err := pool.InTx(context.Background(), func(context.Context, Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, src.beginErr) {
		t.Fatalf("InTx returned %v, want the begin error", err)
	}
	if called {
		t.Error("caller ran despite the transaction never opening")
	}
}

// A rollback still has to reach the database after the caller's context is cancelled,
// or a cancelled request leaves a transaction open until the pool recycles it.
func TestRollbackSurvivesCallerCancellation(t *testing.T) {
	pool, src := newTestPool(nil)

	ctx, cancel := context.WithCancel(context.Background())

	err := pool.InTx(ctx, func(context.Context, Tx) error {
		cancel()
		return errors.New("aborted")
	})
	if err == nil {
		t.Fatal("InTx reported success")
	}
	if got := src.last().rollbacks; got != 1 {
		t.Errorf("rollbacks = %d, want 1 despite the cancelled context", got)
	}
}

func TestClosedPoolRefusesWork(t *testing.T) {
	pool := &Pool{name: "test"}

	if err := pool.InTx(context.Background(), noop); !errors.Is(err, ErrClosed) {
		t.Errorf("InTx on a closed pool returned %v, want ErrClosed", err)
	}
	if err := pool.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Ping on a closed pool returned %v, want ErrClosed", err)
	}
	pool.Close()
	pool.Close() // idempotent
}

func TestNameIsReported(t *testing.T) {
	pool, _ := newTestPool(nil)
	if pool.Name() != "test" {
		t.Errorf("Name() = %q, want test", pool.Name())
	}
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	if _, err := Open(context.Background(), Config{}); err == nil {
		t.Error("Open accepted a configuration with no DSN")
	}
	if _, err := Open(context.Background(), Config{DSN: "://not a dsn"}); err == nil {
		t.Error("Open accepted an unparseable DSN")
	}
}

func TestConfigDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()

	if c.Name != "default" {
		t.Errorf("Name = %q", c.Name)
	}
	if c.MaxConns != 20 {
		t.Errorf("MaxConns = %d", c.MaxConns)
	}
	if c.MaxConnLifetime != 30*time.Minute {
		t.Errorf("MaxConnLifetime = %v", c.MaxConnLifetime)
	}
	if c.MaxConnIdleTime != 5*time.Minute {
		t.Errorf("MaxConnIdleTime = %v", c.MaxConnIdleTime)
	}
	if c.AcquireTimeout != 3*time.Second {
		t.Errorf("AcquireTimeout = %v", c.AcquireTimeout)
	}

	if err := c.validate(); err == nil {
		t.Error("validate accepted a configuration with no DSN")
	}
	c.DSN = "postgres://localhost/x"
	if err := c.validate(); err != nil {
		t.Errorf("validate: %v", err)
	}
}

// applyDefaults must not overwrite a value the caller chose deliberately, or a pool
// sized for provider administration silently becomes a pool sized for tenant traffic.
func TestConfigDefaultsPreserveExplicitValues(t *testing.T) {
	c := Config{
		Name:            "organization-provider",
		DSN:             "postgres://localhost/x",
		MaxConns:        4,
		MaxConnLifetime: time.Minute,
		MaxConnIdleTime: 30 * time.Second,
		AcquireTimeout:  time.Second,
	}
	c.applyDefaults()

	if c.Name != "organization-provider" || c.MaxConns != 4 ||
		c.MaxConnLifetime != time.Minute || c.MaxConnIdleTime != 30*time.Second ||
		c.AcquireTimeout != time.Second {
		t.Errorf("applyDefaults overwrote an explicit value: %+v", c)
	}
}
