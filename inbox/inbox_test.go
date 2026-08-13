package inbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
)

var eventID = id.MustParse("019235f1-8c4a-7c1e-9d0b-3f4a2b6e5d71")
var eventType = event.MustParseType("com.scnehaux.test.record.lifecycle.created")

func TestGuardReportsTheFirstDelivery(t *testing.T) {
	tx := &dbtest.Tx{Tag: dbtest.CommandTag(1)}
	first, err := Guard(context.Background(), tx, "projection", eventID, eventType)
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}
	if !first {
		t.Error("Guard reported a new delivery as a duplicate")
	}

	call := tx.Only(t)
	if !strings.Contains(call.SQL, "ON CONFLICT (event_id, consumer) DO NOTHING") {
		t.Errorf("statement does not guard the composite key: %s", call.SQL)
	}
	if got := call.Args[1]; got != "projection" {
		t.Errorf("consumer = %v", got)
	}
}

func TestGuardReportsADuplicate(t *testing.T) {
	first, err := Guard(context.Background(), &dbtest.Tx{}, "projection", eventID, eventType)
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}
	if first {
		t.Error("Guard reported a duplicate as the first delivery")
	}
}

func TestGuardValidatesItsBoundary(t *testing.T) {
	for name, tc := range map[string]struct {
		tx       *dbtest.Tx
		consumer string
		eid      id.UUID
		typ      event.Type
		want     error
	}{
		"transaction":   {nil, "c", eventID, eventType, ErrNoTransaction},
		"consumer":      {&dbtest.Tx{}, " ", eventID, eventType, ErrNoConsumer},
		"consumer long": {&dbtest.Tx{}, strings.Repeat("c", 256), eventID, eventType, ErrConsumerTooLong},
		"event":         {&dbtest.Tx{}, "c", id.Nil, eventType, ErrNoEvent},
		"event type":    {&dbtest.Tx{}, "c", eventID, "", ErrNoEventType},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Guard(context.Background(), tc.tx, tc.consumer, tc.eid, tc.typ)
			if !errors.Is(err, tc.want) {
				t.Errorf("Guard error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestGuardWrapsDatabaseFailure(t *testing.T) {
	injected := errors.New("database unavailable")
	_, err := Guard(context.Background(), &dbtest.Tx{ExecErr: injected}, "projection", eventID, eventType)
	if !errors.Is(err, injected) {
		t.Errorf("Guard error = %v, want wrapped database error", err)
	}
}
