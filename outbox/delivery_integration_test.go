package outbox

// Per-consumer delivery (ADR-GLB-018, TDD-foundation-platform-001 §Per-Consumer Delivery), against
// the real engine. Each test that subscribes a second consumer retires it on cleanup, so later
// tests are owed deliveries for the TestMain subscriber alone.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/migrations"
)

const (
	second       = "second-consumer"
	unsubscribed = "com.scnehaux.organization.tenant.lifecycle.renamed"
)

// deliveries reads which consumers an event is owed to, and whether each is published.
func deliveries(ctx context.Context, t *testing.T, p *db.Pool, eventID string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT consumer, published FROM platform.outbox_delivery WHERE event_id = $1`, eventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var consumer string
			var published bool
			if err := rows.Scan(&consumer, &published); err != nil {
				return err
			}
			out[consumer] = published
		}
		return rows.Err()
	}); err != nil {
		t.Fatalf("reading deliveries: %v", err)
	}
	return out
}

func appendOfType(ctx context.Context, t *testing.T, p *db.Pool, eventType string) event.Envelope {
	t.Helper()
	e, err := event.New(event.MustParseSource(testSource), event.MustParseType(eventType),
		time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC), map[string]any{"tenant_id": "x"})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, newAggregateID(t), e)
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return e
}

// One event, one delivery per subscriber, and each consumer's dispatcher settles its own.
func TestEachSubscriberIsOwedItsOwnDelivery(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	subscribed(ctx, t, p, second)

	e, _ := appendOne(ctx, t, p)
	if got := deliveries(ctx, t, p, e.ID.String()); len(got) != 2 || got[subscriber] || got[second] {
		t.Fatalf("deliveries = %v, want one unpublished delivery for each of %s and %s", got, subscriber, second)
	}

	if _, err := newTestDispatcher(t, p, &fakePublisher{}, Config{}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if got := deliveries(ctx, t, p, e.ID.String()); !got[subscriber] || got[second] {
		t.Fatalf("after %s dispatched, deliveries = %v; one consumer's outcome was written to another", subscriber, got)
	}

	pub := &fakePublisher{}
	if _, err := newTestDispatcher(t, p, pub, Config{Consumer: second}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if pub.count() != 1 || pub.published[0].ID != e.ID {
		t.Fatalf("%s received %d envelopes, want the event once", second, pub.count())
	}
	if got := deliveries(ctx, t, p, e.ID.String()); !got[subscriber] || !got[second] {
		t.Errorf("deliveries = %v, want both published", got)
	}
}

// A type no consumer subscribes to is never delivered, so it cannot park as poison anywhere.
func TestAnEventTypeNobodySubscribesToOwesNoDelivery(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)

	e := appendOfType(ctx, t, p, unsubscribed)
	if got := deliveries(ctx, t, p, e.ID.String()); len(got) != 0 {
		t.Fatalf("deliveries = %v for a type nobody subscribes to", got)
	}
	if _, err := readRow(ctx, p, e.ID.String()); err == nil {
		t.Fatal("readRow found a subscriber delivery for an unsubscribed type")
	}
	if n, err := newTestDispatcher(t, p, &fakePublisher{}, Config{}).dispatchOnce(ctx, false); err != nil || n != 0 {
		t.Errorf("the dispatcher claimed %d, %v; want nothing", n, err)
	}
}

// One consumer's refusal parks that consumer's delivery and no other, and a second refusal of the
// same event is a second incident rather than a conflict (ADR-GLB-018 §5.3).
func TestOneConsumersRefusalParksOnlyItsDelivery(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	subscribed(ctx, t, p, second)

	e, _ := appendOne(ctx, t, p)
	poison := &fakePublisher{err: fmt.Errorf("refused: %w", ErrPoison)}
	if _, err := newTestDispatcher(t, p, poison, Config{}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}

	accepting := &fakePublisher{marker: ApplicationReceiptApplied}
	if _, err := newTestDispatcher(t, p, accepting, Config{Consumer: second}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if accepting.count() != 1 {
		t.Fatalf("%s received %d envelopes after %s refused the event, want 1", second, accepting.count(), subscriber)
	}
	if got := deadLetterCount(ctx, t, p, e.ID.String()); got != 1 {
		t.Fatalf("%d dead letters, want the refusing consumer's alone", got)
	}

	// Now the second consumer refuses a fresh event too: two incidents, one per consumer.
	clearOutbox(ctx, t, p)
	f, _ := appendOne(ctx, t, p)
	if _, err := newTestDispatcher(t, p, poison, Config{}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if _, err := newTestDispatcher(t, p, poison, Config{Consumer: second}).dispatchOnce(ctx, false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	var consumers []string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT consumer FROM platform.dead_letter WHERE event_id = $1 ORDER BY consumer`, f.ID.String())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				return err
			}
			consumers = append(consumers, c)
		}
		return rows.Err()
	}); err != nil {
		t.Fatalf("reading dead letters: %v", err)
	}
	if len(consumers) != 2 || consumers[0] != second || consumers[1] != subscriber {
		t.Errorf("dead letters name %v, want one for each consumer", consumers)
	}
}

// A replay names the consumer it is for, and is refused when that consumer is owed nothing.
func TestToOwesTheReplayToOneConsumer(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	subscribed(ctx, t, p, second)

	e, _ := appendOne(ctx, t, p, To(second))
	if got := deliveries(ctx, t, p, e.ID.String()); len(got) != 1 || !hasKey(got, second) {
		t.Fatalf("deliveries = %v, want %s alone", got, second)
	}

	f := newEnvelope(t)
	err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, newAggregateID(t), f, To("nobody"))
	})
	if !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("a replay to an unsubscribed consumer answered %v, want ErrNotSubscribed", err)
	}
	var count int
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM platform.outbox WHERE event_id = $1`, f.ID.String()).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("the refused replay left an event owed to nobody")
	}
}

func hasKey(m map[string]bool, k string) bool {
	_, ok := m[k]
	return ok
}

// A subscription is replaced, never edited, and a retired one is owed nothing new.
func TestASubscriptionIsReplacedAndRetired(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	subscribed(ctx, t, p, second)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Subscribe(ctx, tx, second, []event.Type{event.MustParseType(unsubscribed)})
	}); err != nil {
		t.Fatalf("replacing the subscription: %v", err)
	}
	var active, total int
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE retired_at IS NULL), count(*)
			FROM platform.subscription WHERE consumer = $1`, second).Scan(&active, &total)
	}); err != nil {
		t.Fatal(err)
	}
	if active != 1 || total < 2 {
		t.Fatalf("%s has %d active of %d subscriptions, want one active and the old one retired", second, active, total)
	}

	e, _ := appendOne(ctx, t, p)
	if got := deliveries(ctx, t, p, e.ID.String()); hasKey(got, second) {
		t.Errorf("deliveries = %v; the replaced subscription is still owed %s", got, testType)
	}
	f := appendOfType(ctx, t, p, unsubscribed)
	if got := deliveries(ctx, t, p, f.ID.String()); !hasKey(got, second) {
		t.Errorf("deliveries = %v; the new subscription is not owed %s", got, unsubscribed)
	}

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Unsubscribe(ctx, tx, second)
	}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	g := appendOfType(ctx, t, p, unsubscribed)
	if got := deliveries(ctx, t, p, g.ID.String()); len(got) != 0 {
		t.Errorf("deliveries = %v after the only subscriber retired", got)
	}
}

// The ordering ADR-GLB-018 §5.2 depends on. A subscription waits for every append in flight, and
// an append in flight when it began is not owed to it: that event committed first, and the
// consumer's snapshot, taken after the subscription commits, already holds it.
func TestASubscriptionWaitsForAppendsInFlight(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	t.Cleanup(func() {
		_ = p.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			return Unsubscribe(ctx, tx, second)
		})
	})

	inFlight := newEnvelope(t)
	appended, commit, appendDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		appendDone <- p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			if err := Append(ctx, tx, newAggregateID(t), inFlight); err != nil {
				return err
			}
			close(appended)
			<-commit
			return nil
		})
	}()
	select {
	case <-appended:
	case err := <-appendDone:
		t.Fatalf("the append ended early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the append did not happen")
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			return Subscribe(ctx, tx, second, []event.Type{event.MustParseType(testType)})
		})
	}()
	select {
	case err := <-subscribeDone:
		close(commit)
		<-appendDone
		t.Fatalf("the subscription committed while an append was in flight (%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(commit)
	if err := <-appendDone; err != nil {
		t.Fatalf("the append: %v", err)
	}
	select {
	case err := <-subscribeDone:
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the subscription did not proceed once the append committed")
	}

	if got := deliveries(ctx, t, p, inFlight.ID.String()); hasKey(got, second) {
		t.Errorf("deliveries = %v; an event that committed before the subscription is owed to it", got)
	}
	after, _ := appendOne(ctx, t, p)
	if got := deliveries(ctx, t, p, after.ID.String()); !hasKey(got, second) {
		t.Errorf("deliveries = %v; an event appended after the subscription is not owed to it", got)
	}
}

// A delivery lands in its event's day, so retention drops the two together.
func TestADeliveryLandsInItsEventsDayPartition(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	e, _ := appendOne(ctx, t, p)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := migrations.EnsureOutboxPartitions(ctx, tx, time.Now(), time.Now())
		return err
	}); err != nil {
		t.Fatalf("ensuring today's partition: %v", err)
	}

	var partition string
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT c.relname FROM platform.outbox_delivery AS d
			 JOIN pg_class AS c ON c.oid = d.tableoid
			 WHERE d.event_id = $1 AND d.consumer = $2`, e.ID.String(), subscriber,
		).Scan(&partition)
	}); err != nil {
		t.Fatalf("locating the delivery: %v", err)
	}
	if want := "outbox_delivery_" + time.Now().UTC().Format("20060102"); partition != want {
		t.Errorf("delivery partition = %q, want %q", partition, want)
	}
}

// ADR-GLB-018 §5.5: retiring a consumer closes what it was still owed, and nothing else.
func TestAbandonClosesARetiredConsumersDeliveries(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	clearOutbox(ctx, t, p)
	clearReceipts(ctx, t, p)
	subscribed(ctx, t, p, second)

	e, _ := appendOne(ctx, t, p)
	f, _ := appendOne(ctx, t, p)

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := Abandon(ctx, tx, second, "still subscribed")
		return err
	}); !errors.Is(err, ErrStillSubscribed) {
		t.Fatalf("abandoning a subscribed consumer answered %v, want ErrStillSubscribed", err)
	}

	var closed int64
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := Unsubscribe(ctx, tx, second); err != nil {
			return err
		}
		var err error
		closed, err = Abandon(ctx, tx, second, "retired: rebuilt under a new identity")
		return err
	}); err != nil {
		t.Fatalf("retiring %s: %v", second, err)
	}
	if closed != 2 {
		t.Fatalf("abandoned %d deliveries, want 2", closed)
	}

	for _, event := range []string{e.ID.String(), f.ID.String()} {
		var published, delivered bool
		var class *string
		if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, `SELECT published, published_at IS NOT NULL, failure_class
				FROM platform.outbox_delivery WHERE event_id = $1 AND consumer = $2`, event, second,
			).Scan(&published, &delivered, &class)
		}); err != nil {
			t.Fatal(err)
		}
		if !published || delivered || class == nil || *class != string(FailureAbandoned) {
			t.Errorf("%s's delivery reads published=%v delivered=%v class=%v; want closed, undelivered, abandoned",
				second, published, delivered, class)
		}
		if got := deliveries(ctx, t, p, event); got[subscriber] {
			t.Errorf("abandoning %s closed %s's delivery too", second, subscriber)
		}
		if got := readReceipts(ctx, t, p, event); len(got) != 0 {
			t.Errorf("an abandoned delivery has receipts %v; nothing may cite it", got)
		}
	}
}

// And the reason it exists: an abandoned delivery holds no day of the outbox.
func TestAnAbandonedDeliveryDoesNotHoldRetention(t *testing.T) {
	p := requireDatabase(t)
	ctx := context.Background()
	day := time.Now().UTC().AddDate(0, 0, -45).Truncate(24 * time.Hour)
	partition := "outbox_" + day.Format("20060102")
	const gone = "retired-consumer"

	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := migrations.EnsureOutboxPartitions(ctx, tx, day, day); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `WITH e AS (
			INSERT INTO platform.outbox
			    (event_id, event_type, aggregate_id, payload, envelope, created_at)
			VALUES (gen_random_uuid(), 'com.scnehaux.test.record.lifecycle.created',
			        gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, $1)
			RETURNING created_at, event_id, sequence, event_type, priority)
			INSERT INTO platform.outbox_delivery (created_at, event_id, consumer, sequence, event_type, priority)
			SELECT created_at, event_id, $2, sequence, event_type, priority FROM e`, day.Add(time.Hour), gone)
		return err
	}); err != nil {
		t.Fatalf("preparing the day: %v", err)
	}

	drop := func() []string {
		var dropped []string
		if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
			var err error
			dropped, err = migrations.DropPublishedOutboxPartitions(ctx, tx, day.Add(48*time.Hour))
			return err
		}); err != nil {
			t.Fatalf("retention: %v", err)
		}
		return dropped
	}
	if dropped := drop(); len(dropped) != 0 {
		t.Fatalf("dropped %v while a delivery was owed", dropped)
	}
	if err := p.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := Abandon(ctx, tx, gone, "retired")
		return err
	}); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if dropped := drop(); len(dropped) != 1 || dropped[0] != partition {
		t.Errorf("dropped %v after the abandonment, want %s", dropped, partition)
	}
}

// ADR-GLB-018 §5.6, measured: a role holding only what publishing needs can append, and cannot
// read the outbox it appends to.
func TestARoleThatOnlyPublishesCanAppend(t *testing.T) {
	admin := requireDatabase(t)
	ctx := boundedContext(t)
	clearOutbox(ctx, t, admin)

	role := fmt.Sprintf("append_only_%d", time.Now().UnixNano())
	if err := admin.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, statement := range []string{
			"CREATE ROLE %s LOGIN PASSWORD 'append'",
			"GRANT USAGE ON SCHEMA platform TO %s",
			"GRANT INSERT ON platform.outbox, platform.outbox_delivery TO %s",
			"GRANT SELECT (consumer, event_types, retired_at) ON platform.subscription TO %s",
			"GRANT USAGE ON SEQUENCE platform.outbox_sequence TO %s",
		} {
			if _, err := tx.Exec(ctx, fmt.Sprintf(statement, role)); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("creating the publishing role: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.InTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			_, _ = tx.Exec(ctx, fmt.Sprintf("REASSIGN OWNED BY %s TO CURRENT_USER", role))
			_, _ = tx.Exec(ctx, fmt.Sprintf("DROP OWNED BY %s", role))
			_, _ = tx.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", role))
			return nil
		})
	})
	publisher, err := db.Open(ctx, db.Config{Name: "append-only", DSN: replaceCredentials(adminDSN(t), role, "append"), MaxConns: 2})
	if err != nil {
		t.Fatalf("opening the publishing pool: %v", err)
	}
	t.Cleanup(publisher.Close)

	e := newEnvelope(t)
	if err := publisher.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return Append(ctx, tx, newAggregateID(t), e)
	}); err != nil {
		t.Fatalf("a role holding what publishing needs could not append: %v", err)
	}
	if got := deliveries(ctx, t, admin, e.ID.String()); !hasKey(got, subscriber) {
		t.Errorf("deliveries = %v; the append wrote none for %s", got, subscriber)
	}
	if err := publisher.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var n int
		return tx.QueryRow(ctx, "SELECT count(*) FROM platform.outbox").Scan(&n)
	}); err == nil {
		t.Error("the publishing role can read the outbox; the test grants more than publishing needs")
	}
}
