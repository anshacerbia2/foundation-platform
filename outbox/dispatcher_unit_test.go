package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/event"
)

type transactionFunc func(context.Context, func(context.Context, db.Tx) error) error

func (f transactionFunc) InTx(ctx context.Context, fn func(context.Context, db.Tx) error) error {
	return f(ctx, fn)
}

type publisherFunc func(context.Context, event.Envelope) error

func (f publisherFunc) Publish(ctx context.Context, envelope event.Envelope) error {
	return f(ctx, envelope)
}

func dispatcherWithRows(t *testing.T, rows [][]any, publish publisherFunc) (*Dispatcher, *dbtest.Tx) {
	t.Helper()
	tx := &dbtest.Tx{Rows: rows, Tag: dbtest.CommandTag(1)}
	transactor := transactionFunc(func(ctx context.Context, fn func(context.Context, db.Tx) error) error {
		return fn(ctx, tx)
	})
	return &Dispatcher{
		tx:        transactor,
		publisher: publish,
		cfg:       Config{BatchSize: 10, MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: time.Second},
		jitter:    func() float64 { return 0 },
	}, tx
}

func TestDispatchOnceClaimsPublishesAndMarksTheRow(t *testing.T) {
	envelope := newEnvelope(t)
	wire, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var published event.Envelope
	dispatcher, tx := dispatcherWithRows(t, [][]any{{
		time.Now(), envelope.ID.String(), string(envelope.Type), PriorityStandard, 0, wire,
	}}, func(_ context.Context, got event.Envelope) error {
		published = got
		return nil
	})

	count, err := dispatcher.dispatchOnce(context.Background(), false)
	if err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if count != 1 || published.ID != envelope.ID {
		t.Errorf("count = %d, published = %s", count, published.ID)
	}
	if len(tx.Calls()) != 2 {
		t.Fatalf("calls = %d, want claim and mark", len(tx.Calls()))
	}
}

func TestDispatchOnceRecordsPublisherFailure(t *testing.T) {
	envelope := newEnvelope(t)
	wire, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("connection refused token=visible")
	dispatcher, tx := dispatcherWithRows(t, [][]any{{
		time.Now(), envelope.ID.String(), string(envelope.Type), PriorityStandard, 0, wire,
	}}, func(context.Context, event.Envelope) error { return injected })

	if _, err := dispatcher.dispatchOnce(context.Background(), false); err != nil {
		t.Fatalf("dispatchOnce: %v", err)
	}
	if len(tx.Calls()) != 2 {
		t.Fatalf("calls = %d, want claim and failure update", len(tx.Calls()))
	}
	failure := tx.Calls()[1]
	for _, arg := range failure.Args {
		if value, ok := arg.(string); ok && value == "connection refused token=visible" {
			t.Error("failure statement received an unredacted credential")
		}
	}
}

func TestDispatchOnceReturnsClaimFailure(t *testing.T) {
	injected := errors.New("query failed")
	tx := &dbtest.Tx{QueryErr: injected}
	dispatcher := &Dispatcher{
		tx: transactionFunc(func(ctx context.Context, fn func(context.Context, db.Tx) error) error {
			return fn(ctx, tx)
		}),
		publisher: publisherFunc(func(context.Context, event.Envelope) error { return nil }),
		cfg:       Config{BatchSize: 1},
	}
	if _, err := dispatcher.dispatchOnce(context.Background(), false); !errors.Is(err, injected) {
		t.Errorf("dispatchOnce error = %v", err)
	}
}

func TestRunReturnsAfterAnAlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dispatcher := &Dispatcher{cfg: Config{Workers: 1, PriorityWorkers: 1}}
	if err := dispatcher.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v", err)
	}
}
