package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
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
	// about a delivery -- this event, to this consumer -- and platform.outbox's single
	// `published` flag is exactly what cannot express that. A receipt written without a
	// consumer would establish that an event was delivered somewhere, which resolves nothing.
	//
	// One dispatcher, one destination. A second destination is a second dispatcher with its
	// own name, and the fan-out that would need -- one delivery row per (event, consumer) --
	// is deliberately not built: the enforcement scope is one producer and one projection
	// consumer, and organization-control refuses a second active one.
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

// claimStatement takes the reserved lane as a parameter rather than embedding the
// priority value, so the predicate cannot drift from the Go constant it is meant to match.
const claimStatement = `SELECT created_at, event_id::text, event_type, sequence, priority, attempts, envelope
FROM platform.outbox
WHERE published = FALSE
  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
  AND ($1::boolean = FALSE OR priority = $3)
ORDER BY priority ASC, sequence ASC
LIMIT $2
FOR UPDATE SKIP LOCKED`

// dispatchOnce claims a batch and settles every row in it, reporting how many were taken.
//
// Claim, publish, and settle share one transaction, so the row lock is the lease: another
// worker cannot take a row that is mid-publication, and a crash releases it immediately
// rather than leaving it claimed until a lease expires. The cost is that a broker's
// latency is spent holding locks, which SKIP LOCKED makes survivable — a second worker
// steps over the locked rows instead of waiting behind them.
func (d *Dispatcher) dispatchOnce(ctx context.Context, priorityOnly bool) (int, error) {
	var handled int

	err := d.tx.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		batch, err := d.claim(ctx, tx, priorityOnly)
		if err != nil {
			return err
		}
		handled = len(batch)

		for _, row := range batch {
			if err := d.settle(ctx, tx, row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return handled, nil
}

func (d *Dispatcher) claim(ctx context.Context, tx db.Tx, priorityOnly bool) ([]claimed, error) {
	rows, err := tx.Query(ctx, claimStatement, priorityOnly, d.cfg.BatchSize, PriorityHigh)
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
	return batch, nil
}

// settle publishes one row and records the outcome.
//
// It returns an error only for a database failure. A publication failure is the row's
// outcome, written to that row, and returning it would roll back the whole batch —
// including the failure records of the rows that had already been settled.
func (d *Dispatcher) settle(ctx context.Context, tx db.Tx, row claimed) error {
	var envelope event.Envelope
	if err := json.Unmarshal(row.envelope, &envelope); err != nil {
		// A stored envelope that no longer decodes cannot be published by any attempt.
		// It is poison without ever reaching the broker.
		return d.fail(ctx, tx, row, FailurePoison,
			fmt.Sprintf("stored envelope is undecodable: %v", err))
	}
	if envelope.StreamPosition != 0 && envelope.StreamPosition != row.position {
		return d.fail(ctx, tx, row, FailurePoison,
			fmt.Sprintf("stored streamposition %d disagrees with outbox sequence %d", envelope.StreamPosition, row.position))
	}
	envelope = envelope.WithStreamPosition(row.position)
	if err := envelope.ValidatePublished(); err != nil {
		return d.fail(ctx, tx, row, FailurePoison,
			fmt.Sprintf("stored envelope is not publishable: %v", err))
	}

	receipt, err := d.publisher.Publish(ctx, envelope)
	if err != nil {
		class := classify(err)
		return d.fail(ctx, tx, row, class, redact.String(err.Error()))
	}

	return d.markPublished(ctx, tx, row, receipt)
}

const markPublishedStatement = `UPDATE platform.outbox
SET published = TRUE, published_at = now(), last_error = NULL, failure_class = NULL,
    next_attempt_at = NULL
WHERE created_at = $1 AND event_id = $2`

// recordReceiptStatement records what this delivery established.
//
// Written in the same transaction as the row being marked published, so a receipt cannot
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

func (d *Dispatcher) markPublished(ctx context.Context, tx db.Tx, row claimed, receipt Receipt) error {
	if _, err := tx.Exec(ctx, markPublishedStatement, row.createdAt, row.eventID); err != nil {
		return fmt.Errorf("outbox: marking %s published: %w", row.eventID, err)
	}
	if _, err := tx.Exec(ctx, recordReceiptStatement,
		row.eventID, d.cfg.Consumer, row.eventType, string(receipt.Evidence())); err != nil {
		return fmt.Errorf("outbox: recording the receipt for %s: %w", row.eventID, err)
	}
	return nil
}

const recordFailureStatement = `UPDATE platform.outbox
SET attempts = $3,
    last_error = $4,
    failure_class = $5,
    first_failed_at = COALESCE(first_failed_at, now()),
    next_attempt_at = now() + make_interval(secs => $6)
WHERE created_at = $1 AND event_id = $2`

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
const deadLetterStatement = `INSERT INTO platform.dead_letter
    (event_id, event_type, envelope, payload, aggregate_id, priority, failure_class,
     failure_detail, attempts, first_failed_at)
SELECT event_id, event_type, envelope, payload, aggregate_id, priority, $3, $4, $5,
       COALESCE(first_failed_at, now())
FROM platform.outbox
WHERE created_at = $1 AND event_id = $2
ON CONFLICT (event_id) DO NOTHING`

// stopRedeliveryStatement marks a dead-lettered row published so the dispatcher stops
// claiming it. published here means "no longer this dispatcher's concern" rather than
// "delivered", which is why published_at stays null and the incident lives in
// platform.dead_letter.
//
// It records the attempt count as well. A row closed with a failure class and an error
// message but attempts still at its previous value describes a failure that never
// happened, and an operator reading it would draw the wrong conclusion about what the
// event cost. The closed row and its dead-letter row must agree.
const stopRedeliveryStatement = `UPDATE platform.outbox
SET published = TRUE, attempts = $5, last_error = $3, failure_class = $4,
    next_attempt_at = NULL, first_failed_at = COALESCE(first_failed_at, now())
WHERE created_at = $1 AND event_id = $2`

func (d *Dispatcher) fail(ctx context.Context, tx db.Tx, row claimed, class FailureClass, detail string) error {
	attempts := row.attempts + 1

	switch disposition := decide(class, row.priority, attempts, d.cfg.MaxAttempts); disposition {
	case dispositionDeadLetter:
		if _, err := tx.Exec(ctx, deadLetterStatement,
			row.createdAt, row.eventID, string(class), detail, attempts); err != nil {
			return fmt.Errorf("outbox: dead-lettering %s: %w", row.eventID, err)
		}
		if _, err := tx.Exec(ctx, stopRedeliveryStatement,
			row.createdAt, row.eventID, detail, string(class), attempts); err != nil {
			return fmt.Errorf("outbox: closing %s after dead-letter: %w", row.eventID, err)
		}
		return nil

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
		if _, err := tx.Exec(ctx, recordFailureStatement,
			row.createdAt, row.eventID, attempts, detail, string(class), delay.Seconds()); err != nil {
			return fmt.Errorf("outbox: recording failure for %s: %w", row.eventID, err)
		}
		return nil

	default:
		return fmt.Errorf("outbox: unhandled disposition %v for %s", disposition, row.eventID)
	}
}
