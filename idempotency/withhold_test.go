package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
)

func TestWithholdRemovesTheSecretAndKeepsTheResult(t *testing.T) {
	live := json.RawMessage(`{"invitation":{"invitation_id":"019a","state":"pending","n":12345678901234567890},"token":"s3cret"}`)
	stored, err := Withhold(live, "/token")
	if err != nil {
		t.Fatalf("Withhold: %v", err)
	}
	if strings.Contains(string(stored), "s3cret") || strings.Contains(string(stored), `"token"`) {
		t.Errorf("stored response still carries the secret: %s", stored)
	}
	// The number keeps its exact text: a float64 round trip would change it.
	want := `{"invitation":{"invitation_id":"019a","n":12345678901234567890,"state":"pending"}}`
	if string(stored) != want {
		t.Errorf("stored = %s, want %s", stored, want)
	}
	if !strings.Contains(string(live), "s3cret") {
		t.Error("Withhold changed the live body")
	}
}

func TestWithholdFollowsRFC6901(t *testing.T) {
	live := json.RawMessage(`{"credentials":[{"value":"a","id":"1"}],"a/b":1,"m~n":2,"keep":true}`)
	stored, err := Withhold(live, "/credentials/0/value", "/a~1b", "/m~0n")
	if err != nil {
		t.Fatalf("Withhold: %v", err)
	}
	if want := `{"credentials":[{"id":"1"}],"keep":true}`; string(stored) != want {
		t.Errorf("stored = %s, want %s", stored, want)
	}
}

func TestWithholdRefusesWhatItCannotWithhold(t *testing.T) {
	body := json.RawMessage(`{"invitation":{"id":"1"},"list":["x"],"token":"s"}`)
	for name, tc := range map[string]struct {
		pointer string
		want    error
	}{
		"a renamed secret":      {"/invitation_token", ErrNotWithheld},
		"a missing nested":      {"/invitation/token", ErrNotWithheld},
		"through a scalar":      {"/token/value", ErrNotWithheld},
		"an index out of range": {"/list/1/x", ErrNotWithheld},
		"a leading zero":        {"/list/00/x", ErrNotWithheld},
		"an array element":      {"/list/0", ErrInvalidPointer},
		"the whole document":    {"", ErrInvalidPointer},
		"no leading slash":      {"token", ErrInvalidPointer},
		"a bad escape":          {"/to~2ken", ErrInvalidPointer},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Withhold(body, tc.pointer); !errors.Is(err, tc.want) {
				t.Errorf("Withhold(%q) error = %v, want %v", tc.pointer, err, tc.want)
			}
		})
	}
	for _, malformed := range []string{``, `{`, `{"a":1} {"b":2}`} {
		if _, err := Withhold(json.RawMessage(malformed), "/a"); err == nil {
			t.Errorf("Withhold accepted %q", malformed)
		}
	}
}

func TestCompleteWithheldRecordsTheStoredBody(t *testing.T) {
	tx := &dbtest.Tx{Tag: dbtest.CommandTag(1)}
	live := json.RawMessage(`{"invitation":{"id":"1"},"token":"s3cret"}`)
	if err := CompleteWithheld(context.Background(), tx, "caller-1", "key-1", "digest-1", 201, live, "/token"); err != nil {
		t.Fatalf("CompleteWithheld: %v", err)
	}
	call := tx.Only(t)
	recorded, ok := call.Args[4].([]byte)
	if !ok {
		t.Fatalf("recorded body argument = %T", call.Args[4])
	}
	if string(recorded) != `{"invitation":{"id":"1"}}` {
		t.Errorf("recorded = %s", recorded)
	}
	if call.Args[3] != 201 {
		t.Errorf("recorded status = %v", call.Args[3])
	}
}

func TestCompleteWithheldRefusesBeforeTheDatabase(t *testing.T) {
	live := json.RawMessage(`{"token":"s"}`)
	for name, tc := range map[string]struct {
		tx       *dbtest.Tx
		withheld []string
		want     error
	}{
		"nothing withheld": {&dbtest.Tx{}, nil, ErrNothingWithheld},
		"no transaction":   {nil, []string{"/token"}, ErrNoTransaction},
		"a missing member": {&dbtest.Tx{}, []string{"/value"}, ErrNotWithheld},
	} {
		t.Run(name, func(t *testing.T) {
			err := CompleteWithheld(context.Background(), tc.tx, "caller-1", "key-1", "digest-1", 201, live, tc.withheld...)
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			if tc.tx != nil && len(tc.tx.Calls()) != 0 {
				t.Errorf("a refused completion reached the database: %+v", tc.tx.Calls())
			}
		})
	}
}

func TestPurgeCompletedPassesTheWindowToTheDatabase(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{int64(3)}}
	purged, err := PurgeCompleted(context.Background(), tx, 48*time.Hour)
	if err != nil || purged != 3 {
		t.Fatalf("PurgeCompleted = %d, %v", purged, err)
	}
	call := tx.Only(t)
	if !strings.Contains(call.SQL, "platform.purge_idempotency_keys(make_interval(secs => $1))") {
		t.Errorf("statement = %s", call.SQL)
	}
	if call.Args[0] != float64(48*60*60) {
		t.Errorf("window argument = %v, want 172800 seconds", call.Args[0])
	}
}

func TestPurgeCompletedRefusesAShortWindow(t *testing.T) {
	tx := &dbtest.Tx{}
	if _, err := PurgeCompleted(context.Background(), tx, MinimumRetention-time.Second); !errors.Is(err, ErrRetentionTooShort) {
		t.Errorf("error = %v, want ErrRetentionTooShort", err)
	}
	if _, err := PurgeCompleted(context.Background(), nil, MinimumRetention); !errors.Is(err, ErrNoTransaction) {
		t.Errorf("error = %v, want ErrNoTransaction", err)
	}
	if len(tx.Calls()) != 0 {
		t.Error("a refused purge reached the database")
	}
}
