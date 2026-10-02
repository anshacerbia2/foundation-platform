package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
	"github.com/anshacerbia2/foundation-platform/redact"
)

// transactor is the slice of db.Pool the dispatcher needs.
//
// Declared here, by the consumer, so this package states what it requires rather than
// depending on everything a pool can do. *db.Pool satisfies it.
type transactor interface {
	InTx(ctx context.Context, fn func(context.Context, db.Tx) error) error
}

// Config configures a dispatcher. Each deployable supplies its own values; this module
// reads no environment variable, and the composition root injects what it constructs.
type Config struct {
	// Consumer names the destination this dispatcher delivers to.
	//
	// Required, and it is identity rather than labelling: a delivery receipt is an answer
	// about a delivery -- this event, to this consumer -- and a receipt written without a
	// consumer would establish that an event was delivered somewhere, which resolves nothing.
	//
	// One dispatcher, one consumer (ADR-GLB-018 §5.4). It claims only this consumer's rows in
	// platform.outbox_delivery, so its leases, backoff and lanes are this consumer's, and a
	// slow consumer never holds another's priority lane. A second consumer is a second
	// dispatcher with its own name, endpoint and credential.
	Consumer string

	// Interval is the poll period. It sets a latency floor on accept-to-claim, which the
	// revocation design budgets at 1 s, so it is a security-relevant setting rather than
	// a tuning preference.
	Interval time.Duration

	// IdleInterval caps the backoff applied after consecutive empty polls.
	IdleInterval time.Duration

	// BatchSize bounds the rows claimed per cycle.
	BatchSize int

	// Workers drive the standard lane, which claims any unpublished row in priority order.
	Workers int

	// PriorityWorkers are reserved for priority rows alone, so a lifecycle backlog cannot
	// consume the capacity a revocation needs.
	PriorityWorkers int

	// MaxAttempts bounds local retries per row, as STD-GLB-004 requires.
	MaxAttempts int

	// BackoffBase is the first retry delay, doubled per attempt with jitter.
	BackoffBase time.Duration

	// BackoffMax caps the retry delay, including the escalating claim backoff a
	// repeatedly failing priority row accumulates.
	BackoffMax time.Duration

	// LeaseDuration is how long a claimed row stays this worker's before another may claim it.
	//
	// It must exceed the time to publish a whole batch, or rows at the end of a slow batch are
	// claimed again and published twice. Consumers deduplicate, so that costs work and not
	// correctness. It is also how long a crashed dispatcher's rows wait before anyone retries
	// them, which makes it part of the revocation latency after a crash, and the reason it
	// is not longer.
	LeaseDuration time.Duration
}

// Defaults are TDD-foundation-platform-001's configuration table.
const (
	defaultInterval     = 500 * time.Millisecond
	defaultIdleInterval = 5 * time.Second
	defaultBatchSize    = 100
	defaultWorkers      = 4
	defaultPriorityWork = 2
	defaultMaxAttempts  = 3
	defaultBackoffBase  = 250 * time.Millisecond
	defaultBackoffMax   = 30 * time.Second
	defaultLease        = 30 * time.Second
)

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.IdleInterval <= 0 {
		c.IdleInterval = defaultIdleInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.PriorityWorkers <= 0 {
		c.PriorityWorkers = defaultPriorityWork
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = defaultBackoffBase
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = defaultBackoffMax
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = defaultLease
	}
}

// Dispatcher drains the outbox to the broker.
//
// It owns goroutines, which is why it is constructed by a composition root and driven by
// Run rather than starting anything on its own. STD-GLB-BE-001 rule 7 places worker loops
// in a driving adapter for exactly this reason: a package that starts a goroutine when it
// is imported cannot be shut down by the process that imported it.
type Dispatcher struct {
	tx        transactor
	publisher Publisher
	cfg       Config

	// jitter is a field so a test can make a schedule deterministic. It is the only
	// randomness in this type.
	jitter func() float64
}

// NewDispatcher constructs a dispatcher. It starts nothing; Run does.
func NewDispatcher(pool *db.Pool, publisher Publisher, cfg Config) (*Dispatcher, error) {
	if pool == nil {
		return nil, errors.New("outbox: a pool is required")
	}
	if publisher == nil {
		return nil, errors.New("outbox: a publisher is required")
	}
	// Not defaulted. Every other field here has a defensible default; a consumer name does
	// not, because the wrong one attributes a delivery receipt to a destination that never
	// received the event, and a made-up one would satisfy the resolution predicate for a
	// consumer that does not exist.
	if strings.TrimSpace(cfg.Consumer) == "" {
		return nil, errors.New("outbox: a consumer name is required; a delivery receipt without one establishes nothing")
	}
	cfg.applyDefaults()
	return &Dispatcher{tx: pool, publisher: publisher, cfg: cfg, jitter: rand.Float64}, nil
}

// Run drives the dispatcher until ctx is cancelled, then waits for every worker to stop.
//
// It returns ctx.Err(), so a caller that blocks on it learns why it stopped. Cancellation
// is the only way it ends: a publication failure is a row's problem and is recorded on
// that row, not a reason to take the dispatcher down.
func (d *Dispatcher) Run(ctx context.Context) error {
	// The database contract, before any worker exists.
	//
	// The schema this dispatcher writes to is applied by a different repository on a different
	// release cadence, and nothing links the two -- which has already produced one version skew
	// where this module wrote to a table the consuming service had not deployed. Discovered per
	// event, that failure is silent and endless: the receipt write shares a transaction with the
	// row being marked published, so no row is ever marked published and every event retries
	// forever against an error about a table nobody installed.
	//
	// Checked here rather than left to the caller because Run is the one function nobody can
	// skip. CheckDispatcherPrerequisites is exported for readiness probes, and nothing depends
	// on anyone remembering to call it.
	// Cancellation first, because a cancelled context is not a contract failure and must not be
	// reported as one. A shutdown that races startup would otherwise return "the database does
	// not satisfy the dispatcher's contract" about a database that was never asked.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkPrerequisites(ctx, d.tx); err != nil {
		return err
	}

	var wg sync.WaitGroup

	start := func(priorityOnly bool, count int) {
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.work(ctx, priorityOnly)
			}()
		}
	}

	start(false, d.cfg.Workers)
	start(true, d.cfg.PriorityWorkers)

	wg.Wait()
	return ctx.Err()
}

// work is one worker's poll loop.
func (d *Dispatcher) work(ctx context.Context, priorityOnly bool) {
	empty := 0

	for ctx.Err() == nil {
		claimed, err := d.dispatchOnce(ctx, priorityOnly)

		switch {
		case err != nil:
			// A claim or settle failure is a database problem, not an event problem. The
			// row stays unpublished and is retried on the next cycle; backing off here
			// keeps a failing database from being hammered by every worker at once.
			empty++
		case claimed == 0:
			empty++
		default:
			empty = 0
		}

		delay := d.cfg.Interval
		if empty > 0 {
			delay = emptyPollDelay(d.cfg.Interval, empty, d.cfg.IdleInterval)
		}
		if !wait(ctx, delay) {
			return
		}
	}
}

// claimed is one row taken from the outbox.
type claimed struct {
	createdAt time.Time
	eventID   string
	eventType string
	position  int64
	priority  int16
	attempts  int
	envelope  []byte
}

// claimStatement leases a batch in one statement.
//
// The inner SELECT picks the rows and locks them with SKIP LOCKED, so two workers claiming at
// the same instant take disjoint sets. The UPDATE stamps each one with this claim's lease, and
// the lock ends when the claim transaction commits, a few milliseconds later. From then on the
// lease, not a lock, keeps other workers off the row: the predicate skips a row whose
// leased_until has not passed.
//
// It takes the reserved lane as a parameter rather than embedding the priority value, so the
// predicate cannot drift from the Go constant it is meant to match. RETURNING has no order, so
// claim sorts the batch itself.
//
// It claims this consumer's deliveries only (ADR-GLB-018 §5.4), and reads each event's envelope
// from platform.outbox, where the event is written once.
const claimStatement = `WITH batch AS (
    SELECT created_at, event_id
    FROM platform.outbox_delivery
    WHERE consumer = $6
      AND published = FALSE
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
      AND (leased_until IS NULL OR leased_until <= now())
      AND ($1::boolean = FALSE OR priority = $3)
    ORDER BY priority ASC, sequence ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE platform.outbox_delivery AS d
SET lease_id = $4::uuid, leased_until = now() + make_interval(secs => $5)
FROM batch
WHERE d.created_at = batch.created_at AND d.event_id = batch.event_id AND d.consumer = $6
RETURNING d.created_at, d.event_id::text, d.event_type, d.sequence, d.priority, d.attempts,
    (SELECT o.envelope FROM platform.outbox o WHERE o.created_at = d.created_at AND o.event_id = d.event_id)`

// settleTimeout bounds each outcome's transaction.
//
// An outcome is recorded on a context that shutdown does not cancel. A consumer that answered
// "applied" has applied the event, and dropping that answer because the process was asked to
// stop would throw away the receipt that proves it.
const settleTimeout = 10 * time.Second

// errLeaseLost means this worker's lease on a row expired and another worker claimed it. The
// outcome is discarded; the row's current holder records its own.
var errLeaseLost = errors.New("outbox: the lease on this row was taken over")

// errStopped means publication was cut short by shutdown. It is nobody's failure and counts
// no attempt.
var errStopped = errors.New("outbox: publication stopped by shutdown")

// dispatchOnce leases a batch, publishes each row outside any transaction, and records each
// outcome in a transaction of its own. It reports how many rows it claimed.
//
// Before this design the whole batch shared one transaction and the row lock was the lease.
// That transaction stayed open while every row in the batch was published, so one slow
// consumer held back vacuum and a pooled connection for the whole batch, and a crash rolled
// back the outcomes already recorded. Now a crash loses at most the outcome being recorded,
// and the lease holds the rest until leased_until passes. That delay is the price: a crashed
// dispatcher's rows wait for the lease to expire rather than being released at once. A graceful
// shutdown releases them immediately.
func (d *Dispatcher) dispatchOnce(ctx context.Context, priorityOnly bool) (int, error) {
	leaseID, err := id.NewV7()
	if err != nil {
		return 0, fmt.Errorf("outbox: minting a lease: %w", err)
	}
	lease := leaseID.String()

	var batch []claimed
	if err := d.tx.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		batch, err = d.claim(ctx, tx, priorityOnly, lease)
		return err
	}); err != nil {
		return 0, err
	}

	// A local deadline, set after the claim committed and so slightly later than the database's
	// leased_until. It only stops this worker publishing rows another worker may already hold;
	// the fence on lease_id is what keeps two outcomes from being recorded.
	deadline := time.Now().Add(d.cfg.LeaseDuration)

	for i, row := range batch {
		if time.Now().After(deadline) {
			// The rest of the batch is claimable again. Publishing it here would race the
			// worker that claims it.
			break
		}
		err := d.settle(ctx, row, lease)
		switch {
		case err == nil, errors.Is(err, errLeaseLost):
		case errors.Is(err, errStopped) || ctx.Err() != nil:
			d.release(ctx, batch[i:], lease)
			return len(batch), nil
		default:
			d.release(ctx, batch[i+1:], lease)
			return len(batch), err
		}
	}
	return len(batch), nil
}

func (d *Dispatcher) claim(ctx context.Context, tx db.Tx, priorityOnly bool, lease string) ([]claimed, error) {
	rows, err := tx.Query(ctx, claimStatement, priorityOnly, d.cfg.BatchSize, PriorityHigh,
		lease, d.cfg.LeaseDuration.Seconds(), d.cfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("outbox: claiming a batch: %w", err)
	}
	defer rows.Close()

	var batch []claimed
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.createdAt, &c.eventID, &c.eventType, &c.position, &c.priority, &c.attempts, &c.envelope); err != nil {
			return nil, fmt.Errorf("outbox: reading a claimed row: %w", err)
		}
		batch = append(batch, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: reading the claimed batch: %w", err)
	}

	// The order the lanes promise: priority first, then the order the events were written.
	sort.Slice(batch, func(i, j int) bool {
		if batch[i].priority != batch[j].priority {
			return batch[i].priority < batch[j].priority
		}
		return batch[i].position < batch[j].position
	})
	return batch, nil
}

// settle publishes one row and records the outcome.
//
// It returns errLeaseLost when the row was claimed by another worker in the meantime,
// errStopped when shutdown cut publication short, and any other error only for a database
// failure. A publication failure is the row's outcome, written to that row.
func (d *Dispatcher) settle(ctx context.Context, row claimed, lease string) error {
	var envelope event.Envelope
	if err := json.Unmarshal(row.envelope, &envelope); err != nil {
		// A stored envelope that no longer decodes cannot be published by any attempt.
		// It is poison without ever reaching the broker.
		return d.fail(ctx, row, lease, FailurePoison,
			fmt.Sprintf("stored envelope is undecodable: %v", err))
	}
	if envelope.StreamPosition != 0 && envelope.StreamPosition != row.position {
		return d.fail(ctx, row, lease, FailurePoison,
			fmt.Sprintf("stored streamposition %d disagrees with outbox sequence %d", envelope.StreamPosition, row.position))
	}
	envelope = envelope.WithStreamPosition(row.position)
	if err := envelope.ValidatePublished(); err != nil {
		return d.fail(ctx, row, lease, FailurePoison,
			fmt.Sprintf("stored envelope is not publishable: %v", err))
	}

	receipt, err := d.publisher.Publish(ctx, envelope)
	if err != nil {
		// A publication cut short by shutdown says nothing about the consumer. Counting it
		// would spend an attempt, and a lifecycle row on its last attempt would be
		// dead-lettered because this process was asked to stop.
		if ctx.Err() != nil {
			return errStopped
		}
		class := classify(err)
		return d.fail(ctx, row, lease, class, redact.String(err.Error()))
	}

	return d.markPublished(ctx, row, lease, receipt)
}

// record runs one outcome's writes in a transaction of their own, on a context shutdown does
// not cancel.
func (d *Dispatcher) record(ctx context.Context, fn func(context.Context, db.Tx) error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	return d.tx.InTx(ctx, fn)
}

// fenced runs a write fenced on the lease, and reports errLeaseLost when it matched no row,
// which rolls back the outcome's transaction.
func fenced(ctx context.Context, tx db.Tx, what, statement string, args ...any) error {
	tag, err := tx.Exec(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("outbox: %s: %w", what, err)
	}
	if tag.RowsAffected() == 0 {
		return errLeaseLost
	}
	return nil
}

// releaseStatement returns a leased row to the pool at once, without counting an attempt.
const releaseStatement = `UPDATE platform.outbox_delivery
SET lease_id = NULL, leased_until = NULL
WHERE created_at = $1 AND event_id = $2 AND consumer = $4 AND lease_id = $3::uuid AND published = FALSE`

// release gives back rows this worker leased and will not publish, so a shutdown does not
// leave them waiting for the lease to expire. Best effort: a row it fails to release becomes
// claimable when its lease expires.
func (d *Dispatcher) release(ctx context.Context, rows []claimed, lease string) {
	if len(rows) == 0 {
		return
	}
	_ = d.record(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, row := range rows {
			if _, err := tx.Exec(ctx, releaseStatement, row.createdAt, row.eventID, lease, d.cfg.Consumer); err != nil {
				return err
			}
		}
		return nil
	})
}

// markPublishedStatement is fenced on the lease. A worker whose lease was taken over matches
// no row and records nothing, including no receipt.
const markPublishedStatement = `UPDATE platform.outbox_delivery
SET published = TRUE, published_at = now(), last_error = NULL, failure_class = NULL,
    next_attempt_at = NULL, lease_id = NULL, leased_until = NULL
WHERE created_at = $1 AND event_id = $2 AND consumer = $4 AND lease_id = $3::uuid AND published = FALSE`

// recordReceiptStatement records what this delivery established.
//
// Written in the same transaction as the row being marked published, and only when the lease
// fence matched, so a receipt cannot
// exist for a delivery that was rolled back and a published row cannot exist without its
// receipt. The dead-letter resolution contract reads these to establish that a specific event
// reached a specific consumer, and a receipt whose delivery never committed would be evidence
// of something that did not happen.
//
// ON CONFLICT DO NOTHING because a replay of an event the consumer already applied is a
// successful no-op at the consumer -- its inbox guard dedupes on the same key -- and the first
// receipt already records what was established. Overwriting it with a later, possibly weaker
// class would let a replay through a broker erase the evidence a direct delivery produced.
const recordReceiptStatement = `INSERT INTO platform.delivery_receipt
    (event_id, consumer, event_type, evidence)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id, consumer) DO NOTHING`

func (d *Dispatcher) markPublished(ctx context.Context, row claimed, lease string, receipt Receipt) error {
	return d.record(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := fenced(ctx, tx, "marking "+row.eventID+" published",
			markPublishedStatement, row.createdAt, row.eventID, lease, d.cfg.Consumer); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, recordReceiptStatement,
			row.eventID, d.cfg.Consumer, row.eventType, string(receipt.Evidence())); err != nil {
			return fmt.Errorf("outbox: recording the receipt for %s: %w", row.eventID, err)
		}
		return nil
	})
}

// recordFailureStatement counts the attempt and schedules the next one. now() is the start of
// this outcome's own transaction, which begins after the publication returned, so
// first_failed_at and next_attempt_at are measured from the failure and not from the claim.
const recordFailureStatement = `UPDATE platform.outbox_delivery
SET attempts = $3,
    last_error = $4,
    failure_class = $5,
    first_failed_at = COALESCE(first_failed_at, now()),
    next_attempt_at = now() + make_interval(secs => $6),
    lease_id = NULL, leased_until = NULL
WHERE created_at = $1 AND event_id = $2 AND consumer = $8 AND lease_id = $7::uuid AND published = FALSE`

// deadLetterStatement copies the row into platform.dead_letter, reading envelope and
// payload from the outbox rather than from the dispatcher's memory so the two cannot
// disagree.
//
// aggregate_id and priority travel with it because a dead letter has to be replayable from
// its own row. Replaying means appending to platform.outbox again, which requires
// aggregate_id NOT NULL and takes priority to pick the lane -- so without them a replay had
// to read the original outbox row, and that row lives in a partition with retention. Once
// the partition was dropped the incident record survived and the ability to act on it did
// not. See 0004 for why that mattered: REPLAYED is the only first-hand evidence the
// resolution contract has.
//
// consumer is the destination that refused the event, which is Config.Consumer. The column
// existed from 0001 and was never written, so every dead letter was nobody's: a host could not
// tell whose projection a poison event left behind, and had to treat one consumer's refusal as
// every consumer's debt. Written, a host can attribute debt to the consumer it belongs to. Rows
// dead-lettered before this keep NULL, and a host must read NULL as belonging to everyone.
//
// The incident is this consumer's (ADR-GLB-018 §5.3), keyed by the event and the consumer, so
// another consumer's refusal of the same event is a second incident and not a conflict.
const deadLetterStatement = `INSERT INTO platform.dead_letter
    (event_id, event_type, envelope, payload, aggregate_id, priority, failure_class,
     failure_detail, attempts, first_failed_at, consumer)
SELECT o.event_id, o.event_type, o.envelope, o.payload, o.aggregate_id, o.priority, $3, $4, $5,
       COALESCE(d.first_failed_at, now()), $6
FROM platform.outbox o
JOIN platform.outbox_delivery d
  ON d.created_at = o.created_at AND d.event_id = o.event_id AND d.consumer = $6
WHERE o.created_at = $1 AND o.event_id = $2
ON CONFLICT ON CONSTRAINT dead_letter_delivery DO NOTHING`

// stopRedeliveryStatement marks a dead-lettered row published so the dispatcher stops
// claiming it. published here means "no longer this dispatcher's concern" rather than
// "delivered", which is why published_at stays null and the incident lives in
// platform.dead_letter.
//
// It records the attempt count as well. A row closed with a failure class and an error
// message but attempts still at its previous value describes a failure that never
// happened, and an operator reading it would draw the wrong conclusion about what the
// event cost. The closed row and its dead-letter row must agree.
//
// It runs before the dead-letter insert and is fenced on the lease, so a worker that lost its
// lease rolls back without writing an incident.
const stopRedeliveryStatement = `UPDATE platform.outbox_delivery
SET published = TRUE, attempts = $5, last_error = $3, failure_class = $4,
    next_attempt_at = NULL, first_failed_at = COALESCE(first_failed_at, now()),
    lease_id = NULL, leased_until = NULL
WHERE created_at = $1 AND event_id = $2 AND consumer = $7 AND lease_id = $6::uuid AND published = FALSE`

func (d *Dispatcher) fail(ctx context.Context, row claimed, lease string, class FailureClass, detail string) error {
	attempts := row.attempts + 1

	switch disposition := decide(class, row.priority, attempts, d.cfg.MaxAttempts); disposition {
	case dispositionDeadLetter:
		return d.record(ctx, func(ctx context.Context, tx db.Tx) error {
			if err := fenced(ctx, tx, "closing "+row.eventID+" for dead-letter", stopRedeliveryStatement,
				row.createdAt, row.eventID, detail, string(class), attempts, lease, d.cfg.Consumer); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, deadLetterStatement,
				row.createdAt, row.eventID, string(class), detail, attempts, d.cfg.Consumer); err != nil {
				return fmt.Errorf("outbox: dead-lettering %s: %w", row.eventID, err)
			}
			return nil
		})

	case dispositionRetry, dispositionRelease:
		// The two share one effect on the row: the attempt is counted, and the row becomes
		// claimable again once its backoff elapses. They differ in meaning rather than in
		// SQL. A release is a priority row that has spent its local retries and will keep
		// trying regardless, and it is what the undelivered-priority alert watches.
		//
		// The design's algorithm says a released priority row resets its attempt count.
		// It is not reset here, and the same clause is the reason: it also requires the
		// claim backoff to escalate, and a counter that resets cannot escalate. Nothing is
		// lost by keeping it, because what protects a priority row from being abandoned is
		// the classification in decide and not the size of the number. Keeping the count
		// lets the delay grow toward its ceiling instead of oscillating, and leaves an
		// operator able to see what the outage has cost.
		delay := backoffFor(d.cfg.BackoffBase, attempts, d.cfg.BackoffMax, d.jitter())
		return d.record(ctx, func(ctx context.Context, tx db.Tx) error {
			return fenced(ctx, tx, "recording failure for "+row.eventID, recordFailureStatement,
				row.createdAt, row.eventID, attempts, detail, string(class), delay.Seconds(), lease, d.cfg.Consumer)
		})

	default:
		return fmt.Errorf("outbox: unhandled disposition %v for %s", disposition, row.eventID)
	}
}
