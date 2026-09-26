package outbox

// The lease: what replaced the claim transaction's row lock.
//
// Each test parks a publication on purpose, because every property here is about what is true
// while a consumer has not answered yet: no transaction is open, other workers step over the
// row, an outcome from a lease that was taken over is discarded, and shutdown gives the batch
// back without spending attempts.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
)

// publishFunc adapts a function to Publisher, receipt included.
type publishFunc func(context.Context, event.Envelope) (Receipt, error)

func (f publishFunc) Publish(ctx context.Context, e event.Envelope) (Receipt, error) {
	return f(ctx, e)
}

// gate parks a publication until released, and announces that it was entered.
type gate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate() *gate { return &gate{entered: make(chan struct{}), release: make(chan struct{})} }

// wait parks the caller. It returns ctx.Err() if shutdown comes first.
func (g *gate) wait(ctx context.Context) error {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) awaitEntry(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher was never entered")
	}
}

// parkedThen parks on g, then answers with err, or with the consumer's applied marker.
func parkedThen(g *gate, err error) publishFunc {
	return func(ctx context.Context, _ event.Envelope) (Receipt, error) {
		if waitErr := g.wait(ctx); waitErr != nil {
			return Receipt{}, waitErr
		}
		if err != nil {
			return Receipt{}, err
		}
		return ReceiptFromMarker(ApplicationReceiptApplied), nil
	}
}

type leaseState struct {
	leased      bool
	leaseLive   bool
	published   bool
	attempts    int
	failureSeen bool
}

func readLease(ctx context.Context, t *testing.T, p *db.Pool, eventID string) leaseState {
	t.Helper()
	var s leaseState
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT lease_id IS NOT NULL, coalesce(leased_until > now(), false), published, attempts,
			       failure_class IS NOT NULL
			FROM platform.outbox WHERE event_id = $1`, eventID,
		).Scan(&s.leased, &s.leaseLive, &s.published, &s.attempts, &s.failureSeen)
	}); err != nil {
		t.Fatalf("reading the lease: %v", err)
	}
	return s
}

func runAsync(ctx context.Context, d *Dispatcher) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := d.dispatchOnce(ctx, false)
		done <- err
	}()
	return done
}

func awaitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("dispatchOnce: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("dispatchOnce did not return")
	}
}

// The point of the lease. While the consumer has not answered, no lock is held on the row and
// no transaction is open, and the row carries a live lease instead. Under the old design the
// NOWAIT probe failed here, because the claim transaction still held the row.
func TestNoLockIsHeldWhilePublishing(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	g := newGate()
	d := newTestDispatcher(t, p, parkedThen(g, nil), Config{})
	done := runAsync(ctx, d)
	released := false
	defer func() {
		if !released {
			close(g.release)
		}
	}()
	g.awaitEntry(t)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var one int
		return tx.QueryRow(ctx,
			`SELECT 1 FROM platform.outbox WHERE event_id = $1 FOR UPDATE NOWAIT`, e.ID.String()).Scan(&one)
	}); err != nil {
		t.Fatalf("the row is locked while its publication is in flight: %v", err)
	}
	if s := readLease(ctx, t, p, e.ID.String()); !s.leased || !s.leaseLive || s.published {
		t.Fatalf("while publishing, lease = %+v; want a live lease on an unpublished row", s)
	}

	close(g.release)
	released = true
	awaitDone(t, done)

	if s := readLease(ctx, t, p, e.ID.String()); !s.published || s.leased {
		t.Errorf("after publishing, lease = %+v; want published with the lease cleared", s)
	}
}

// Without a lock, the lease is what keeps a second worker off a row in flight.
func TestALeasedRowIsSkippedByOtherWorkers(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	appendOne(ctx, t, p)
	g := newGate()
	first := newTestDispatcher(t, p, parkedThen(g, nil), Config{})
	done := runAsync(ctx, first)
	defer func() { close(g.release); awaitDone(t, done) }()
	g.awaitEntry(t)

	second := &fakePublisher{}
	n, err := newTestDispatcher(t, p, second, Config{}).dispatchOnce(ctx, false)
	if err != nil {
		t.Fatalf("second dispatcher: %v", err)
	}
	if n != 0 || second.count() != 0 {
		t.Errorf("a second worker claimed %d rows and published %d while the first held the lease", n, second.count())
	}
}

// A lease that expired while its worker was still waiting on the consumer belongs to whoever
// claimed the row next. The first worker's late answer -- here poison -- must record nothing:
// no incident, no attempt. Without the fence on lease_id it would dead-letter a row another
// worker is delivering.
func TestAnOutcomeFromATakenOverLeaseIsDiscarded(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)

	e, _ := appendOne(ctx, t, p)

	slow := newGate()
	first := newTestDispatcher(t, p,
		parkedThen(slow, fmt.Errorf("late refusal: %w", ErrPoison)), Config{LeaseDuration: 300 * time.Millisecond})
	firstDone := runAsync(ctx, first)
	slow.awaitEntry(t)

	time.Sleep(600 * time.Millisecond) // the first lease has now expired in the database

	fresh := newGate()
	second := newTestDispatcher(t, p, parkedThen(fresh, nil), Config{})
	secondDone := runAsync(ctx, second)
	fresh.awaitEntry(t)

	close(slow.release)
	awaitDone(t, firstDone)

	if n := deadLetterCount(ctx, t, p, e.ID.String()); n != 0 {
		t.Errorf("the worker whose lease was taken over dead-lettered the row (%d incidents)", n)
	}
	if s := readLease(ctx, t, p, e.ID.String()); s.published || s.attempts != 0 || s.failureSeen || !s.leaseLive {
		t.Errorf("after the stale outcome, row = %+v; want it untouched and still leased by the second worker", s)
	}

	close(fresh.release)
	awaitDone(t, secondDone)

	if s := readLease(ctx, t, p, e.ID.String()); !s.published || s.attempts != 0 {
		t.Errorf("after the current holder answered, row = %+v; want published with no attempt spent", s)
	}
	if got := readReceipts(ctx, t, p, e.ID.String()); len(got) != 1 || got[0].evidence != string(EvidenceConsumerApplied) {
		t.Errorf("receipts = %+v; want the current holder's applied receipt only", got)
	}
}

// Shutdown is nobody's failure. The batch goes back to the pool at once, with no attempt spent,
// so a lifecycle row on its last attempt is not dead-lettered because the process was stopped.
func TestShutdownReleasesTheBatchWithoutCountingAnAttempt(t *testing.T) {
	p := requireDatabase(t)
	clearOutbox(context.Background(), t, p)

	var ids []string
	for i := 0; i < 3; i++ {
		e, _ := appendOne(context.Background(), t, p)
		ids = append(ids, e.ID.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := newGate()
	d := newTestDispatcher(t, p, parkedThen(g, nil), Config{BatchSize: 3})
	done := runAsync(ctx, d)
	g.awaitEntry(t)
	cancel()
	awaitDone(t, done)

	for _, eventID := range ids {
		if s := readLease(context.Background(), t, p, eventID); s.leased || s.published || s.attempts != 0 || s.failureSeen {
			t.Errorf("after shutdown, %s = %+v; want released, unpublished, and no attempt spent", eventID, s)
		}
	}

	n, err := newTestDispatcher(t, p, &fakePublisher{}, Config{}).dispatchOnce(context.Background(), false)
	if err != nil {
		t.Fatalf("the next dispatcher: %v", err)
	}
	if n != 3 {
		t.Errorf("the next dispatcher claimed %d rows at once, want all 3; shutdown left them leased", n)
	}
}

// The consumer answered "applied" and then the process was told to stop. That answer is the
// receipt a resolution may rest on, so it is recorded anyway.
func TestAnAppliedAnswerIsRecordedEvenWhenShutdownFollows(t *testing.T) {
	p := requireDatabase(t)
	clearOutbox(context.Background(), t, p)
	clearReceipts(context.Background(), t, p)

	e, _ := appendOne(context.Background(), t, p)
	ctx, cancel := context.WithCancel(context.Background())
	d := newTestDispatcher(t, p, publishFunc(func(context.Context, event.Envelope) (Receipt, error) {
		cancel()
		return ReceiptFromMarker(ApplicationReceiptApplied), nil
	}), Config{})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if s := readLease(context.Background(), t, p, e.ID.String()); !s.published {
		t.Errorf("row = %+v; the applied answer was dropped by shutdown", s)
	}
	if got := readReceipts(context.Background(), t, p, e.ID.String()); len(got) != 1 {
		t.Errorf("receipts = %+v; want the applied receipt", got)
	}
}

// Each outcome commits before the next row is published, so a crash later in the batch cannot
// roll back a delivery that already happened. Under the old design the first row's outcome was
// invisible here: it sat in the batch transaction, uncommitted.
func TestEachOutcomeCommitsBeforeTheNextRowIsPublished(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	firstRow, _ := appendOne(ctx, t, p)
	appendOne(ctx, t, p)

	g := newGate()
	var mu sync.Mutex
	calls := 0
	d := newTestDispatcher(t, p, publishFunc(func(ctx context.Context, _ event.Envelope) (Receipt, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 2 {
			if err := g.wait(ctx); err != nil {
				return Receipt{}, err
			}
		}
		return ReceiptFromMarker(ApplicationReceiptApplied), nil
	}), Config{BatchSize: 2})
	done := runAsync(ctx, d)
	defer func() { close(g.release); awaitDone(t, done) }()
	g.awaitEntry(t)

	if s := readLease(ctx, t, p, firstRow.ID.String()); !s.published {
		t.Errorf("while the second row is in flight, the first = %+v; its delivery is not committed", s)
	}
}
