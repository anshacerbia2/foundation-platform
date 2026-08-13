package outbox

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
)

func TestDisposeResolvedDeadLettersClearsOnlyRetainedData(t *testing.T) {
	tx := &dbtest.Tx{Tag: dbtest.CommandTag(3)}
	count, err := DisposeResolvedDeadLetters(context.Background(), tx, time.Now())
	if err != nil {
		t.Fatalf("DisposeResolvedDeadLetters: %v", err)
	}
	if count != 3 {
		t.Errorf("disposed = %d", count)
	}
	call := tx.Only(t)
	for _, fragment := range []string{"resolved_at IS NOT NULL", "envelope = NULL", "payload = NULL"} {
		if !strings.Contains(call.SQL, fragment) {
			t.Errorf("statement omits %q: %s", fragment, call.SQL)
		}
	}
}

func TestCountStaleUnresolvedDeadLetters(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{int64(4)}}
	count, err := CountStaleUnresolvedDeadLetters(context.Background(), tx, time.Now())
	if err != nil {
		t.Fatalf("CountStaleUnresolvedDeadLetters: %v", err)
	}
	if count != 4 {
		t.Errorf("count = %d", count)
	}
	if call := tx.Only(t); !strings.Contains(call.SQL, "resolved_at IS NULL") {
		t.Errorf("query includes resolved incidents: %s", call.SQL)
	}
}
