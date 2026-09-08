package outbox

// The dead-letter temporal boundary.
//
// dead_lettered_at is read outside this repository as evidence that a projection snapshot
// taken after it read state including the failed event's effect. 0003 moved its default
// from now() (the dispatcher transaction's start, which precedes every publish attempt in
// the batch) to statement_timestamp() (the row's own transition). This file is the gate on
// that, and it exists because the mistake is invisible to every other test in the suite:
// with a fast publisher the two values differ by microseconds and nothing notices.
//
// Both instants compared here come from the DATABASE clock. An earlier draft of this test
// captured the "before" instant from the Go process, which would have compared a Go clock
// against a database clock -- the very defect class the gate is protecting. A gate that
// embodies the bug it forbids is worse than no gate, because it passes and looks like
// evidence.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
)

// parkedPoisonPublisher parks inside Publish until released, then fails permanently.
//
// It reproduces the only condition under which the defect is observable: a publish attempt
// that takes real time while the dispatcher's transaction -- and therefore its
// transaction_timestamp() -- is already open.
//
// Distinct from blockingPublisher in dispatcher_integration_test.go, which parks but then
// succeeds and never announces that it was entered. Both properties are required here: the
// test must capture a database instant while the dispatcher is parked, so it has to know
// when that has happened.
type parkedPoisonPublisher struct {
	entered   chan struct{}
	release   chan struct{}
	err       error
	enterOnce sync.Once
}

func (b *parkedPoisonPublisher) Publish(ctx context.Context, _ event.Envelope) error {
	b.enterOnce.Do(func() { close(b.entered) })

	select {
	case <-b.release:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// databaseClock reads the server's wall clock on a connection of its own.
//
// A connection of its own is required rather than tidy: the dispatcher is parked inside an
// open transaction holding its own connection, so this read cannot share it. The pool's
// default of twenty makes that free.
//
// clock_timestamp() rather than now() here, and for the opposite reason 0003 chose
// statement_timestamp() for the column: this probe wants the real instant it ran, not the
// start of the transaction wrapping it. now() here would report this transaction's start
// and the test would still be comparing the wrong pair of instants.
func databaseClock(ctx context.Context, t *testing.T, p *db.Pool) time.Time {
	t.Helper()

	var at time.Time
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&at)
	}); err != nil {
		t.Fatalf("reading the database clock: %v", err)
	}
	return at.UTC()
}

func deadLetteredAt(ctx context.Context, t *testing.T, p *db.Pool, eventID string) time.Time {
	t.Helper()

	var at time.Time
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT dead_lettered_at FROM platform.dead_letter WHERE event_id = $1",
			eventID).Scan(&at)
	}); err != nil {
		t.Fatalf("reading dead_lettered_at: %v", err)
	}
	return at.UTC()
}

// With the default at now(), dead_lettered_at is the dispatcher transaction's start, which
// is before the publisher was ever entered -- so it is before the instant this test
// captures while the publisher is still parked, and the assertion fails. With
// statement_timestamp() the write happens after the release and the assertion holds.
func TestDeadLetteredAtNamesTheTransitionNotTheTransactionStart(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)

	pub := &parkedPoisonPublisher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     fmt.Errorf("unregistered type: %w", ErrPoison),
	}
	d := newTestDispatcher(t, p, pub, Config{})

	done := make(chan error, 1)
	go func() {
		_, err := d.dispatchOnce(ctx, false)
		done <- err
	}()

	// Released explicitly rather than through t.Cleanup. Cleanup runs LIFO, so a release
	// registered there would run after anything registered earlier that waits on the
	// dispatcher -- which deadlocks the test instead of failing it.
	released := false
	release := func() {
		if !released {
			close(pub.release)
			released = true
		}
	}

	select {
	case <-pub.entered:
	case <-time.After(30 * time.Second):
		release()
		t.Fatal("the dispatcher never reached the publisher")
	}

	// The dispatcher's transaction is open and its transaction_timestamp() is already in
	// the past. Anything the dead-letter write records must be after this instant.
	duringPublish := databaseClock(ctx, t, p)

	release()

	if err := <-done; err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Fatalf("%d dead-letter rows, want 1", got)
	}

	at := deadLetteredAt(ctx, t, p, e.ID.String())
	if !at.After(duringPublish) {
		t.Fatalf("dead_lettered_at = %s is not after %s, a database instant observed while "+
			"the publisher was still blocked.\n"+
			"The column is recording the dispatcher transaction's start rather than the "+
			"dead-letter transition. Outside this repository the resolution contract treats "+
			"a snapshot taken after this value as having read state that includes the failed "+
			"event; an understated value lets a snapshot taken BEFORE the event committed "+
			"satisfy that comparison, and a revoked membership keeps being served.\n"+
			"See migrations/platform/0003_dead_letter_temporal_boundary.sql.",
			at.Format(time.RFC3339Nano), duringPublish.Format(time.RFC3339Nano))
	}
}

// The batch case, and the second half of the same defect: now() is one value for the whole
// transaction, so every row in a batch would share a timestamp while each transitions at its
// own instant. Distinct timestamps are what let an operator order the failures inside one
// dispatch pass, and what keeps a per-row resolution comparison honest.
func TestEachDeadLetterInABatchCarriesItsOwnTransition(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	first, _ := appendOne(ctx, t, p)
	second, _ := appendOne(ctx, t, p)

	// A publisher that spends real time on each row, so two transitions inside one
	// transaction are separated by more than the clock's resolution.
	pub := &slowPoisonPublisher{delay: 5 * time.Millisecond,
		err: fmt.Errorf("unregistered type: %w", ErrPoison)}
	d := newTestDispatcher(t, p, pub, Config{})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	firstAt := deadLetteredAt(ctx, t, p, first.ID.String())
	secondAt := deadLetteredAt(ctx, t, p, second.ID.String())

	if firstAt.Equal(secondAt) {
		t.Fatalf("both dead letters carry %s; the column is recording one instant for the "+
			"whole transaction rather than each row's own transition",
			firstAt.Format(time.RFC3339Nano))
	}
}

type slowPoisonPublisher struct {
	delay time.Duration
	err   error
}

func (s *slowPoisonPublisher) Publish(ctx context.Context, _ event.Envelope) error {
	select {
	case <-time.After(s.delay):
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
