package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
)

const (
	testSource = "/systems/organization-control"
	testType   = "com.scnehaux.organization.membership.security.revoked"
)

func newEnvelope(t *testing.T) event.Envelope {
	t.Helper()

	e, err := event.New(
		event.MustParseSource(testSource),
		event.MustParseType(testType),
		time.Date(2026, 8, 13, 9, 14, 22, 0, time.UTC),
		map[string]any{"membership_id": "019235f5-0000-7000-8000-000000000001"},
	)
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	return e
}

func newAggregateID(t *testing.T) id.UUID {
	t.Helper()

	u, err := id.NewV7()
	if err != nil {
		t.Fatalf("generating aggregate id: %v", err)
	}
	return u
}

// The invariant the whole deduplication scheme rests on. If the row carries a different
// identifier from the envelope, every consumer's processed_event lookup misses and the
// effect is applied on each delivery, with no error raised anywhere.
func TestAppendWritesTheEnvelopeIdentifierAsTheRowIdentifier(t *testing.T) {
	tx := &dbtest.Tx{}
	e := newEnvelope(t)

	if err := Append(context.Background(), tx, newAggregateID(t), e); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := tx.Only(t).Args[0]; got != e.ID.String() {
		t.Errorf("event_id = %v, want the envelope identifier %s", got, e.ID)
	}
}

func TestAppendTargetsTheOutboxTable(t *testing.T) {
	tx := &dbtest.Tx{}

	if err := Append(context.Background(), tx, newAggregateID(t), newEnvelope(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	sql := tx.Only(t).SQL
	if !strings.Contains(sql, "platform.outbox") {
		t.Errorf("statement does not target platform.outbox:\n%s", sql)
	}
	if !strings.HasPrefix(strings.TrimSpace(sql), "INSERT") {
		t.Errorf("statement is not an insert:\n%s", sql)
	}
}

// A column list and a VALUES list that disagree fail at execution, which here means in
// production, because no test in this package reaches a real database.
func TestStatementBindsEveryColumnItNames(t *testing.T) {
	openParen := strings.Index(appendStatement, "(")
	closeParen := strings.Index(appendStatement, ")")
	if openParen < 0 || closeParen < openParen {
		t.Fatalf("cannot locate the column list in:\n%s", appendStatement)
	}
	columns := strings.Split(appendStatement[openParen+1:closeParen], ",")

	placeholders := strings.Count(appendStatement, "$")

	if len(columns) != placeholders {
		t.Errorf("%d columns but %d placeholders:\n%s", len(columns), placeholders, appendStatement)
	}

	tx := &dbtest.Tx{}
	if err := Append(context.Background(), tx, newAggregateID(t), newEnvelope(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := len(tx.Only(t).Args); got != placeholders {
		t.Errorf("%d arguments for %d placeholders", got, placeholders)
	}
}

func TestAppendDefaultsToTheStandardLane(t *testing.T) {
	tx := &dbtest.Tx{}

	if err := Append(context.Background(), tx, newAggregateID(t), newEnvelope(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := tx.Only(t).Args[3]; got != PriorityStandard {
		t.Errorf("priority = %v, want %v", got, PriorityStandard)
	}
}

func TestPriorityRoutesToTheReservedLane(t *testing.T) {
	tx := &dbtest.Tx{}

	if err := Append(context.Background(), tx, newAggregateID(t), newEnvelope(t), Priority()); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := tx.Only(t).Args[3]; got != PriorityHigh {
		t.Errorf("priority = %v, want %v", got, PriorityHigh)
	}
}

func TestAppendCarriesTypeAndAggregateAsColumns(t *testing.T) {
	tx := &dbtest.Tx{}
	aggregate := newAggregateID(t)

	if err := Append(context.Background(), tx, aggregate, newEnvelope(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	args := tx.Only(t).Args
	if args[1] != testType {
		t.Errorf("event_type = %v, want %s", args[1], testType)
	}
	if args[2] != aggregate.String() {
		t.Errorf("aggregate_id = %v, want %s", args[2], aggregate)
	}
}

// payload holds the domain data alone and envelope holds the full CloudEvents document.
// They are separate columns because operators query the payload and consumers replay the
// envelope, and collapsing them would force one of those to parse around the other.
func TestPayloadAndEnvelopeAreDistinctColumns(t *testing.T) {
	tx := &dbtest.Tx{}
	e := newEnvelope(t)

	if err := Append(context.Background(), tx, newAggregateID(t), e); err != nil {
		t.Fatalf("Append: %v", err)
	}

	args := tx.Only(t).Args

	payload, ok := args[4].([]byte)
	if !ok {
		t.Fatalf("payload argument is %T, want []byte", args[4])
	}
	if string(payload) != string(e.Data) {
		t.Errorf("payload = %s, want %s", payload, e.Data)
	}

	envelope, ok := args[5].([]byte)
	if !ok {
		t.Fatalf("envelope argument is %T, want []byte", args[5])
	}

	var decoded event.Envelope
	if err := json.Unmarshal(envelope, &decoded); err != nil {
		t.Fatalf("stored envelope does not decode: %v", err)
	}
	if decoded.ID != e.ID || decoded.Type != e.Type || decoded.Source != e.Source {
		t.Errorf("stored envelope does not round-trip: %+v", decoded)
	}
}

// The stored envelope is what a consumer receives after a replay, so it must be the
// CloudEvents wire form and not the Go field names.
func TestStoredEnvelopeIsCloudEventsJSON(t *testing.T) {
	tx := &dbtest.Tx{}

	if err := Append(context.Background(), tx, newAggregateID(t), newEnvelope(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(tx.Only(t).Args[5].([]byte), &members); err != nil {
		t.Fatalf("decoding stored envelope: %v", err)
	}

	for _, required := range []string{"specversion", "id", "source", "type", "time", "datacontenttype", "data"} {
		if _, present := members[required]; !present {
			t.Errorf("stored envelope omits the CloudEvents member %q", required)
		}
	}
}

func TestAppendRefusesAnAbsentTransaction(t *testing.T) {
	err := Append(context.Background(), nil, newAggregateID(t), newEnvelope(t))
	if !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("err = %v, want %v", err, ErrNoTransaction)
	}
}

func TestAppendRefusesAnAbsentAggregate(t *testing.T) {
	tx := &dbtest.Tx{}

	err := Append(context.Background(), tx, id.Nil, newEnvelope(t))
	if !errors.Is(err, ErrNoAggregate) {
		t.Fatalf("err = %v, want %v", err, ErrNoAggregate)
	}
	if len(tx.Calls()) != 0 {
		t.Errorf("a statement was sent despite the refusal: %v", tx.Calls())
	}
}

// An invalid envelope must be refused before anything is written. Writing it and letting
// the dispatcher classify it as poison would turn a caller's bug into a dead-letter row
// and an alert.
func TestAppendRefusesAnInvalidEnvelopeWithoutWriting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*event.Envelope)
		fragment string
	}{
		{"no type", func(e *event.Envelope) { e.Type = "" }, "type is absent"},
		{"no source", func(e *event.Envelope) { e.Source = "" }, "source is absent"},
		{"no data", func(e *event.Envelope) { e.Data = nil }, "data is absent"},
		{"wrong specversion", func(e *event.Envelope) { e.SpecVersion = "0.3" }, "specversion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &dbtest.Tx{}
			e := newEnvelope(t)
			tc.mutate(&e)

			err := Append(context.Background(), tx, newAggregateID(t), e)
			if err == nil {
				t.Fatal("Append accepted an invalid envelope")
			}
			if !strings.Contains(err.Error(), tc.fragment) {
				t.Errorf("err = %v, want it to mention %q", err, tc.fragment)
			}
			if len(tx.Calls()) != 0 {
				t.Errorf("a statement was sent despite the refusal: %v", tx.Calls())
			}
		})
	}
}

// The wrapped error has to name the event. A dispatcher log line reporting only "insert
// failed" cannot be correlated with anything.
func TestAppendWrapsAWriteFailureWithTheEventIdentity(t *testing.T) {
	sentinel := errors.New("deadlock detected")
	tx := &dbtest.Tx{ExecErr: sentinel}
	e := newEnvelope(t)

	err := Append(context.Background(), tx, newAggregateID(t), e)

	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap %v", err, sentinel)
	}
	if !strings.Contains(err.Error(), e.ID.String()) {
		t.Errorf("err = %v, want it to name %s", err, e.ID)
	}
	if !strings.Contains(err.Error(), testType) {
		t.Errorf("err = %v, want it to name %s", err, testType)
	}
}

func TestPriorityLanesAreOrderedSoHighDispatchesFirst(t *testing.T) {
	if PriorityHigh >= PriorityStandard {
		t.Fatalf("PriorityHigh %d must sort before PriorityStandard %d under ORDER BY priority ASC",
			PriorityHigh, PriorityStandard)
	}
}
