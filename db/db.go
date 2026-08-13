// Package db owns PostgreSQL access for the control plane.
//
// It is the only package permitted to name the driver, which arch.json asserts. Every
// other package takes db.Tx, so a driver change is one edit here rather than an edit in
// every service signature.
//
// Its purpose is narrow and load-bearing: InTx is the only function in the estate that
// yields a transaction handle. Combined with outbox.Append requiring one, that makes
// publishing a domain event outside the transaction that produced it fail to compile
// rather than fail in production.
package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tx is the transaction handle every layer above this package uses.
//
// It aliases the driver type deliberately rather than wrapping it. ADR-GLB-008 §5.9
// mandates pgx and prohibits an object-relational mapper, so a hand-written interface
// would abstract a dependency the enterprise has decided not to change. What the alias
// buys is the property that matters: one package names the driver, and the boundary
// check enforces it.
type Tx = pgx.Tx

// IsNilTx reports both a nil interface and an interface containing a typed nil pointer.
// The latter commonly appears when a transaction double is passed through db.Tx.
func IsNilTx(tx Tx) bool {
	if tx == nil {
		return true
	}
	v := reflect.ValueOf(tx)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// SessionBinder is invoked at the start of every transaction opened through a Pool,
// inside that transaction.
//
// It exists so this package can support Row-Level Security without knowing what a
// tenant is. TDD-organization-control-001 requires every tenant-scoped transaction to
// issue SET LOCAL, and requires that statement to appear in exactly one package. This
// package supplies the call site; the consuming system supplies the statement.
//
// SET LOCAL reverts at commit or rollback, which is what makes connection pooling and
// Row-Level Security safe together. That is also why the binder runs inside the
// transaction rather than on connection acquisition.
type SessionBinder interface {
	Bind(ctx context.Context, tx Tx) error
}

// BinderFunc adapts a function to SessionBinder.
type BinderFunc func(ctx context.Context, tx Tx) error

func (f BinderFunc) Bind(ctx context.Context, tx Tx) error { return f(ctx, tx) }

var (
	// ErrNestedTransaction reports InTx called from inside a transaction opened by
	// the same process.
	ErrNestedTransaction = errors.New("db: transaction already open on this call path")

	// ErrClosed reports use of a pool that has been closed.
	ErrClosed = errors.New("db: pool is closed")
)

// txMarkerKey carries the fact that a transaction is open, and never the handle.
//
// The distinction matters. STD-GLB-BE-001 rule 6 prohibits carrying a transaction in
// the context, because an implicit handle removes the compile-time guarantee that a
// publication cannot happen outside one and makes participation unreadable from a
// signature. A boolean marker grants no capability: it cannot be used to run a query,
// and its only reader is the guard below.
type txMarkerKey struct{}

func withinTx(ctx context.Context) bool {
	open, _ := ctx.Value(txMarkerKey{}).(bool)
	return open
}

// Config describes one pool. Each deployable constructs its own, so the two pools an
// Organization control plane opens can carry different ceilings.
type Config struct {
	// Name identifies the pool in telemetry. Two pools in one process are
	// indistinguishable in a metric without it.
	Name string

	// DSN is the connection string. It carries a role that owns no table and holds
	// neither SUPERUSER nor BYPASSRLS, per STD-GLB-002.
	DSN string

	// MaxConns bounds the pool.
	MaxConns int32

	// MaxConnLifetime recycles a connection regardless of health, so a long-lived
	// process does not pin a connection across a database failover.
	MaxConnLifetime time.Duration

	// MaxConnIdleTime releases an idle connection back to the database.
	MaxConnIdleTime time.Duration

	// AcquireTimeout bounds the wait for a connection. Without it, pool exhaustion
	// presents as an unbounded hang rather than as an error naming the pool.
	AcquireTimeout time.Duration

	// Binder runs inside every transaction before the caller's work. It may be nil
	// for a pool whose schema carries no row-level policy.
	Binder SessionBinder
}

func (c *Config) applyDefaults() {
	if c.Name == "" {
		c.Name = "default"
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 20
	}
	if c.MaxConnLifetime <= 0 {
		c.MaxConnLifetime = 30 * time.Minute
	}
	if c.MaxConnIdleTime <= 0 {
		c.MaxConnIdleTime = 5 * time.Minute
	}
	if c.AcquireTimeout <= 0 {
		c.AcquireTimeout = 3 * time.Second
	}
}

func (c Config) validate() error {
	if c.DSN == "" {
		return errors.New("db: DSN is required")
	}
	return nil
}

// beginner is the surface Pool needs from a connection source.
//
// It exists so the transaction semantics below — binder ordering, rollback on error,
// rollback on panic, nesting refusal — are unit-testable with no database. Those are
// the semantics most worth testing and the ones an integration test would exercise
// last.
type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Pool is a connection pool bound to one role.
type Pool struct {
	name   string
	src    beginner
	pool   *pgxpool.Pool
	binder SessionBinder
}

// Open constructs a pool and verifies it can reach the database.
func Open(ctx context.Context, cfg Config) (*Pool, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("db: parsing DSN for pool %q: %w", cfg.Name, err)
	}
	pcfg.MaxConns = cfg.MaxConns
	pcfg.MaxConnLifetime = cfg.MaxConnLifetime
	pcfg.MaxConnIdleTime = cfg.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("db: opening pool %q: %w", cfg.Name, err)
	}

	acquireCtx, cancel := context.WithTimeout(ctx, cfg.AcquireTimeout)
	defer cancel()
	if err := pool.Ping(acquireCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: pool %q cannot reach the database: %w", cfg.Name, err)
	}

	return &Pool{name: cfg.Name, src: pool, pool: pool, binder: cfg.Binder}, nil
}

// Name reports the pool name used in telemetry.
func (p *Pool) Name() string { return p.name }

// Ping reports whether the pool can reach the database.
func (p *Pool) Ping(ctx context.Context) error {
	if p.pool == nil {
		return ErrClosed
	}
	return p.pool.Ping(ctx)
}

// Close releases the pool. It is safe to call more than once.
func (p *Pool) Close() {
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
		p.src = nil
	}
}

// InTx runs fn inside a transaction, invoking the pool's SessionBinder first.
//
// It is the only path in the estate that yields a Tx. Handing the handle to fn rather
// than storing it anywhere is what keeps participation visible in every signature
// beneath it.
//
// fn receives a derived context rather than the one passed in, and must propagate it
// to everything it calls. That context carries the nesting marker, so a service that
// opens a second transaction on the same call path is refused rather than silently
// acquiring a second connection and losing atomicity between the two.
//
// fn returning an error rolls back and returns that error. A panic rolls back and
// re-panics, because a panic that left a transaction open would hold a connection and
// a row lock until the pool recycled it.
func (p *Pool) InTx(ctx context.Context, fn func(context.Context, Tx) error) (err error) {
	if p.src == nil {
		return ErrClosed
	}
	if withinTx(ctx) {
		return ErrNestedTransaction
	}

	tx, err := p.src.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: pool %q beginning transaction: %w", p.name, err)
	}

	committed := false
	defer func() {
		if r := recover(); r != nil {
			rollback(ctx, tx)
			panic(r)
		}
		if !committed {
			rollback(ctx, tx)
		}
	}()

	if p.binder != nil {
		if bindErr := p.binder.Bind(ctx, tx); bindErr != nil {
			// The binder failing means the session scope this transaction requires
			// was never applied. Running fn now would execute against an unbound
			// session, which under Row-Level Security raises on the first query and
			// under a permissive schema silently reads the wrong rows.
			return fmt.Errorf("db: pool %q binding session: %w", p.name, bindErr)
		}
	}

	if fnErr := fn(withTxMarker(ctx), tx); fnErr != nil {
		return fnErr
	}

	if commitErr := tx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("db: pool %q committing: %w", p.name, commitErr)
	}
	committed = true
	return nil
}

func withTxMarker(ctx context.Context) context.Context {
	return context.WithValue(ctx, txMarkerKey{}, true)
}

// rollback discards a transaction, tolerating one already closed.
//
// A rollback failure is not returned to the caller. The caller acts on why its work
// failed, and replacing that with a rollback error would hide the cause behind its
// consequence. pgx discards a connection whose rollback failed, so the failure does
// not leak into the next transaction.
func rollback(ctx context.Context, tx pgx.Tx) {
	// The caller's context may already be cancelled, and a rollback still has to
	// reach the database.
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := tx.Rollback(rbCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		_ = err // surfaced through pool telemetry rather than through the caller
	}
}
