package idempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
)

func TestDigestSeparatesPartBoundaries(t *testing.T) {
	if Digest([]byte("ab"), []byte("c")) == Digest([]byte("a"), []byte("bc")) {
		t.Error("Digest is ambiguous across part boundaries")
	}
	if Digest([]byte("same")) != Digest([]byte("same")) {
		t.Error("Digest is not stable")
	}
}

func TestClaimReportsANewKey(t *testing.T) {
	tx := &dbtest.Tx{Tag: dbtest.CommandTag(1)}
	got, err := Claim(context.Background(), tx, "caller-1", "key-1", "digest-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.State != StateClaimed {
		t.Errorf("state = %v, want claimed", got.State)
	}
	call := tx.Only(t)
	if !strings.Contains(call.SQL, "ON CONFLICT (scope, key) DO NOTHING") {
		t.Errorf("claim is not scoped atomically: %s", call.SQL)
	}
}

func TestClaimReplaysACompletedResponse(t *testing.T) {
	status := sql.NullInt64{Int64: 201, Valid: true}
	tx := &dbtest.Tx{RowValues: []any{"digest-1", status, []byte(`{"id":"1"}`), true}}
	got, err := Claim(context.Background(), tx, "caller-1", "key-1", "digest-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.State != StateReplay || got.Status != 201 || string(got.Body) != `{"id":"1"}` {
		t.Errorf("replay = %+v", got)
	}
	if len(tx.Calls()) != 2 {
		t.Fatalf("calls = %d, want insert then read", len(tx.Calls()))
	}
}

func TestClaimRejectsDigestConflict(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{"different", sql.NullInt64{}, []byte(nil), false}}
	_, err := Claim(context.Background(), tx, "caller-1", "key-1", "digest-1")
	if !errors.Is(err, ErrConflict) {
		t.Errorf("Claim error = %v, want conflict", err)
	}
}

func TestClaimReportsAnInProgressRequest(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{"digest-1", sql.NullInt64{}, []byte(nil), false}}
	got, err := Claim(context.Background(), tx, "caller-1", "key-1", "digest-1")
	if !errors.Is(err, ErrInProgress) || got.State != StateInProgress {
		t.Errorf("Claim = %+v, %v", got, err)
	}
}

func TestCompleteStoresAJSONResponse(t *testing.T) {
	tx := &dbtest.Tx{Tag: dbtest.CommandTag(1)}
	if err := Complete(context.Background(), tx, "caller-1", "key-1", "digest-1", 200, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	call := tx.Only(t)
	if !strings.Contains(call.SQL, "completed_at = now()") {
		t.Errorf("completion does not close the claim: %s", call.SQL)
	}
}

func TestCompleteRequiresTheClaim(t *testing.T) {
	err := Complete(context.Background(), &dbtest.Tx{}, "caller-1", "key-1", "digest-1", 200, json.RawMessage(`{}`))
	if !errors.Is(err, ErrNotClaimed) {
		t.Errorf("Complete error = %v", err)
	}
}

func TestBoundaryValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		tx                 *dbtest.Tx
		scope, key, digest string
		want               error
	}{
		"transaction": {nil, "s", "k", "d", ErrNoTransaction},
		"scope":       {&dbtest.Tx{}, " ", "k", "d", ErrNoScope},
		"key":         {&dbtest.Tx{}, "s", " ", "d", ErrNoKey},
		"digest":      {&dbtest.Tx{}, "s", "k", " ", ErrNoDigest},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Claim(context.Background(), tc.tx, tc.scope, tc.key, tc.digest)
			if !errors.Is(err, tc.want) {
				t.Errorf("Claim error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBoundaryLengthsAreRejectedBeforeTheDatabase(t *testing.T) {
	for name, tc := range map[string]struct {
		scope, key, digest string
		want               error
	}{
		"scope":  {strings.Repeat("s", 513), "key", "digest", ErrScopeTooLong},
		"key":    {"scope", strings.Repeat("k", 256), "digest", ErrKeyTooLong},
		"digest": {"scope", "key", strings.Repeat("d", 257), ErrDigestTooLong},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Claim(context.Background(), &dbtest.Tx{}, tc.scope, tc.key, tc.digest)
			if !errors.Is(err, tc.want) {
				t.Errorf("Claim error = %v, want %v", err, tc.want)
			}
		})
	}
}
