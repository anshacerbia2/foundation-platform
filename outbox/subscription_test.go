package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/event"
)

// A subscription change takes the lock exclusive before it writes, so it waits for every append
// already in flight and holds new ones until it commits.
func TestSubscribeOrdersItselfAgainstAppendsBeforeWriting(t *testing.T) {
	tx := &dbtest.Tx{}
	types := []event.Type{event.MustParseType(testType), event.MustParseType(testType)}
	if err := Subscribe(context.Background(), tx, " reference-projection ", types); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	calls := tx.Calls()
	if len(calls) != 3 {
		t.Fatalf("got %d statements, want the lock, the retirement and the insert", len(calls))
	}
	if calls[0].SQL != subscriptionExclusive || calls[1].SQL != retireSubscriptionStatement || calls[2].SQL != insertSubscriptionStatement {
		t.Fatalf("statements out of order: %v", calls)
	}
	if got := calls[2].Args[0]; got != "reference-projection" {
		t.Errorf("consumer = %v, want the trimmed name", got)
	}
	if got := calls[2].Args[1].([]string); len(got) != 1 || got[0] != testType {
		t.Errorf("event types = %v, want one, deduplicated", got)
	}
}

func TestSubscribeRefusesAnIncompleteSubscription(t *testing.T) {
	for name, c := range map[string]struct {
		consumer string
		types    []event.Type
		want     error
	}{
		"no consumer":    {" ", []event.Type{event.MustParseType(testType)}, ErrNoConsumer},
		"no event types": {"reference-projection", nil, ErrNoEventTypes},
	} {
		tx := &dbtest.Tx{}
		if err := Subscribe(context.Background(), tx, c.consumer, c.types); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
		if len(tx.Calls()) != 0 {
			t.Errorf("%s: a statement was sent despite the refusal", name)
		}
	}
	tx := &dbtest.Tx{}
	if err := Subscribe(context.Background(), tx, "reference-projection", []event.Type{"not a type"}); err == nil {
		t.Error("Subscribe accepted a malformed event type")
	}
	if err := Subscribe(context.Background(), nil, "reference-projection", []event.Type{event.MustParseType(testType)}); !errors.Is(err, ErrNoTransaction) {
		t.Errorf("a nil transaction answered %v, want ErrNoTransaction", err)
	}
}

func TestUnsubscribeRetiresUnderTheExclusiveLock(t *testing.T) {
	tx := &dbtest.Tx{}
	if err := Unsubscribe(context.Background(), tx, "reference-projection"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	calls := tx.Calls()
	if len(calls) != 2 || calls[0].SQL != subscriptionExclusive || calls[1].SQL != retireSubscriptionStatement {
		t.Fatalf("statements = %v, want the lock then the retirement", calls)
	}
	if err := Unsubscribe(context.Background(), &dbtest.Tx{}, " "); !errors.Is(err, ErrNoConsumer) {
		t.Errorf("an unnamed consumer answered %v, want ErrNoConsumer", err)
	}
}
