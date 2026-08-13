package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
)

// fakePublisher records what it was asked to publish and fails on command.
type fakePublisher struct {
	mu        sync.Mutex
	published []event.Envelope
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, e event.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, e)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

// state is the dispatch-relevant slice of a row.
type state struct {
	published     bool
	publishedAt   *time.Time
	attempts      int
	failureClass  *string
	lastError     *string
	nextAttemptAt *time.Time
	firstFailedAt *time.Time
}

func readState(ctx context.Context, t *testing.T, p *db.Pool, eventID string) state {
	t.Helper()

	var s state
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT published, published_at, attempts, failure_class, last_error,
			       next_attempt_at, first_failed_at
			FROM platform.outbox WHERE event_id = $1`, eventID,
		).Scan(&s.published, &s.publishedAt, &s.attempts, &s.failureClass, &s.lastError,
			&s.nextAttemptAt, &s.firstFailedAt)
	}); err != nil {
		t.Fatalf("reading row state: %v", err)
	}
	return s
}

func deadLetterCount(ctx context.Context, t *testing.T, p *db.Pool, eventID string) int {
	t.Helper()

	var n int
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT count(*) FROM platform.dead_letter WHERE event_id = $1", eventID).Scan(&n)
	}); err != nil {
		t.Fatalf("counting dead-letter rows: %v", err)
	}
	return n
}

func deadLetterAttempts(ctx context.Context, t *testing.T, p *db.Pool, eventID string) int {
	t.Helper()

	var n int
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			"SELECT attempts FROM platform.dead_letter WHERE event_id = $1", eventID).Scan(&n)
	}); err != nil {
		t.Fatalf("reading dead-letter attempts: %v", err)
	}
	return n
}

// clearOutbox empties the tables so a test sees only the rows it wrote. TRUNCATE on the
// partitioned parent clears every partition, which is the bulk disposal ADR-GLB-003 asks
// partitioning for in the first place.
func clearOutbox(ctx context.Context, t *testing.T, p *db.Pool) {
	t.Helper()

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "TRUNCATE platform.outbox"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "TRUNCATE platform.dead_letter")
		return err
	}); err != nil {
		t.Fatalf("clearing the outbox: %v", err)
	}
}

// newTestDispatcher builds a dispatcher with a fixed jitter, so a schedule under test is
// reproducible rather than merely probable.
func newTestDispatcher(t *testing.T, p *db.Pool, pub Publisher, cfg Config) *Dispatcher {
	t.Helper()

	d, err := NewDispatcher(p, pub, cfg)
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	d.jitter = func() float64 { return 0 }
	return d
}

func TestDispatcherPublishesAndClosesTheRow(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{})

	n, err := d.dispatchOnce(ctx, false)
	if err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("claimed %d rows, want 1", n)
	}
	if pub.count() != 1 {
		t.Fatalf("published %d envelopes, want 1", pub.count())
	}
	if pub.published[0].ID != e.ID {
		t.Errorf("published %s, want %s", pub.published[0].ID, e.ID)
	}

	s := readState(ctx, t, p, e.ID.String())
	if !s.published {
		t.Error("the row is still unpublished after a successful publication")
	}
	if s.publishedAt == nil {
		t.Error("published_at was not recorded, so dispatch latency cannot be measured")
	}
}

func TestAPublishedRowIsNotClaimedAgain(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	appendOne(ctx, t, p)
	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	n, err := d.dispatchOnce(ctx, false)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if n != 0 {
		t.Errorf("claimed %d rows on the second pass, want 0", n)
	}
}

// Poison spends no attempts. Retrying a message the broker will always reject buys
// nothing and delays the alert that matters.
func TestPoisonDeadLettersOnTheFirstFailure(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{err: fmt.Errorf("unregistered type: %w", ErrPoison)}
	d := newTestDispatcher(t, p, pub, Config{})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Fatalf("%d dead-letter rows, want 1", got)
	}

	s := readState(ctx, t, p, e.ID.String())
	if !s.published {
		t.Error("a dead-lettered row is still claimable, so it will be redelivered forever")
	}
	if s.publishedAt != nil {
		t.Error("published_at was set for a row that was never delivered")
	}
	if s.failureClass == nil || *s.failureClass != string(FailurePoison) {
		t.Errorf("failure_class = %v, want %q", s.failureClass, FailurePoison)
	}
	if s.attempts != 1 {
		t.Errorf("attempts = %d, want 1", s.attempts)
	}
}

func TestAnUnavailableBrokerSchedulesARetryRatherThanAbandoning(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{err: errors.New("connection refused")}
	d := newTestDispatcher(t, p, pub, Config{BackoffBase: 5 * time.Second})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 0 {
		t.Fatalf("%d dead-letter rows after one failure, want 0", got)
	}

	s := readState(ctx, t, p, e.ID.String())
	if s.published {
		t.Error("the row was closed after a retryable failure")
	}
	if s.attempts != 1 {
		t.Errorf("attempts = %d, want 1", s.attempts)
	}
	if s.failureClass == nil || *s.failureClass != string(FailureUnavailable) {
		t.Errorf("failure_class = %v, want %q", s.failureClass, FailureUnavailable)
	}
	if s.firstFailedAt == nil {
		t.Error("first_failed_at was not recorded; platform.dead_letter requires it later")
	}
	if s.nextAttemptAt == nil {
		t.Fatal("next_attempt_at was not set, so the backoff has no effect")
	}
	if !s.nextAttemptAt.After(time.Now()) {
		t.Errorf("next_attempt_at %v is not in the future", s.nextAttemptAt)
	}
}

// The backoff must actually withhold the row. A next_attempt_at that the claim predicate
// ignores is a column, not a delay.
func TestABackedOffRowIsNotClaimedUntilItsDelayElapses(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	appendOne(ctx, t, p)
	pub := &fakePublisher{err: errors.New("connection refused")}
	d := newTestDispatcher(t, p, pub, Config{BackoffBase: time.Hour})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}

	n, err := d.dispatchOnce(ctx, false)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if n != 0 {
		t.Errorf("claimed %d rows while the backoff was still running, want 0", n)
	}
}

func TestAStandardRowIsDeadLetteredOnceItsAttemptsAreSpent(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p)
	pub := &fakePublisher{err: errors.New("connection refused")}
	// No backoff, so the row is immediately claimable and the attempts can be spent
	// without waiting.
	d := newTestDispatcher(t, p, pub, Config{MaxAttempts: 3, BackoffBase: time.Nanosecond})

	for i := 0; i < 3; i++ {
		if _, err := d.dispatchOnce(ctx, false); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Fatalf("%d dead-letter rows after three attempts, want 1", got)
	}

	s := readState(ctx, t, p, e.ID.String())
	if !s.published {
		t.Error("the dead-lettered row is still claimable")
	}
	if s.attempts != 3 {
		t.Errorf("attempts = %d, want 3", s.attempts)
	}

	// The closed row and its incident record must tell the same story. A row carrying a
	// failure class and an error message but a stale attempt count describes a failure
	// that never happened.
	if dl := deadLetterAttempts(ctx, t, p, e.ID.String()); dl != s.attempts {
		t.Errorf("dead_letter.attempts = %d but outbox.attempts = %d; the two records disagree", dl, s.attempts)
	}
}

// The rule the design states twice, now asserted against a database rather than against a
// pure function. A revocation must survive an outage that outlasts its retry budget.
func TestAPriorityRowSurvivesFarMoreFailuresThanItsAttemptBudget(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p, Priority())
	pub := &fakePublisher{err: errors.New("connection refused")}
	d := newTestDispatcher(t, p, pub, Config{MaxAttempts: 3, BackoffBase: time.Nanosecond})

	for i := 0; i < 10; i++ {
		if _, err := d.dispatchOnce(ctx, true); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 0 {
		t.Fatalf("a priority row was dead-lettered for unavailability after ten attempts")
	}

	s := readState(ctx, t, p, e.ID.String())
	if s.published {
		t.Error("a priority row was closed while the broker was merely unavailable")
	}
	if s.attempts != 10 {
		t.Errorf("attempts = %d, want 10; the count must keep rising for the backoff to escalate", s.attempts)
	}
}

// And it must publish the moment the broker returns.
func TestAPriorityRowPublishesOnceTheBrokerRecovers(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e, _ := appendOne(ctx, t, p, Priority())
	pub := &fakePublisher{err: errors.New("connection refused")}
	d := newTestDispatcher(t, p, pub, Config{MaxAttempts: 3, BackoffBase: time.Nanosecond})

	for i := 0; i < 5; i++ {
		if _, err := d.dispatchOnce(ctx, true); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	pub.mu.Lock()
	pub.err = nil
	pub.mu.Unlock()

	if _, err := d.dispatchOnce(ctx, true); err != nil {
		t.Fatalf("dispatch after recovery: %v", err)
	}

	if s := readState(ctx, t, p, e.ID.String()); !s.published {
		t.Error("the row did not publish after the broker recovered")
	}
	if pub.count() != 1 {
		t.Errorf("published %d envelopes, want 1", pub.count())
	}
}

// The reservation that makes the priority lane worth having: a worker in it must not be
// consumed by lifecycle traffic.
func TestThePriorityLaneClaimsOnlyPriorityRows(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	for i := 0; i < 5; i++ {
		appendOne(ctx, t, p)
	}
	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{})

	n, err := d.dispatchOnce(ctx, true)
	if err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("the priority lane claimed %d standard rows, want 0", n)
	}
}

func TestTheStandardLaneTakesPriorityRowsFirst(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	appendOne(ctx, t, p)
	appendOne(ctx, t, p)
	urgent, _ := appendOne(ctx, t, p, Priority())

	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{BatchSize: 1})

	if _, err := d.dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	if pub.count() != 1 {
		t.Fatalf("published %d envelopes, want 1", pub.count())
	}
	if pub.published[0].ID != urgent.ID {
		t.Errorf("published %s first, want the priority event %s", pub.published[0].ID, urgent.ID)
	}
}

// SKIP LOCKED is what lets two replicas run. Without it the second would block behind the
// first's locks and the dispatcher would not scale past one process.
func TestConcurrentDispatchersClaimDisjointRows(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	const rows = 20
	for i := 0; i < rows; i++ {
		appendOne(ctx, t, p)
	}

	// A publisher that blocks until released, so both dispatchers hold their claims at the
	// same time. Without the overlap the test would pass even if the second waited for the
	// first to commit.
	release := make(chan struct{})
	blocking := &blockingPublisher{release: release}

	var wg sync.WaitGroup
	counts := make([]int, 2)
	errs := make([]error, 2)

	// Half the rows each. With the full batch size the first dispatcher would take
	// everything and the second would claim nothing, which passes a total-count assertion
	// while proving only exclusion. Halving it means both must do work, and the only way
	// the totals add up is if they claimed disjoint sets.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := newTestDispatcher(t, p, blocking, Config{BatchSize: rows / 2})
			counts[i], errs[i] = d.dispatchOnce(ctx, false)
		}(i)
	}

	// Let both claim, then let both finish.
	time.Sleep(500 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("dispatcher %d: %v", i, err)
		}
	}
	for i, got := range counts {
		if got != rows/2 {
			t.Errorf("dispatcher %d claimed %d rows, want %d", i, got, rows/2)
		}
	}
	if total := counts[0] + counts[1]; total != rows {
		t.Errorf("the two dispatchers claimed %d rows in total, want %d with no overlap and none skipped",
			total, rows)
	}
	if blocking.publishedCount() != rows {
		t.Errorf("%d envelopes were published, want %d", blocking.publishedCount(), rows)
	}
}

type blockingPublisher struct {
	release chan struct{}

	mu    sync.Mutex
	count int
}

func (b *blockingPublisher) Publish(ctx context.Context, _ event.Envelope) error {
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	b.mu.Lock()
	b.count++
	b.mu.Unlock()
	return nil
}

func (b *blockingPublisher) publishedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// Run owns its goroutines and must give them all back. A dispatcher that outlives its
// context holds a pool connection until the process exits.
func TestRunStopsEveryWorkerOnCancellation(t *testing.T) {
	p := requireDatabase(t)
	ctx, cancel := context.WithCancel(context.Background())
	clearOutbox(ctx, t, p)

	appendOne(ctx, t, p)
	pub := &fakePublisher{}
	d := newTestDispatcher(t, p, pub, Config{Interval: 10 * time.Millisecond})

	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	// Give the workers time to drain the single row, then shut down.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within five seconds of cancellation")
	}

	if pub.count() != 1 {
		t.Errorf("published %d envelopes, want 1", pub.count())
	}
}

func TestNewDispatcherRefusesAnIncompleteConstruction(t *testing.T) {
	if _, err := NewDispatcher(nil, &fakePublisher{}, Config{}); err == nil {
		t.Error("NewDispatcher accepted a nil pool")
	}
	if _, err := NewDispatcher(&db.Pool{}, nil, Config{}); err == nil {
		t.Error("NewDispatcher accepted a nil publisher")
	}
}
