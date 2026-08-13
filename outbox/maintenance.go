package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
)

const disposeDeadLettersStatement = `UPDATE platform.dead_letter
SET envelope = NULL, payload = NULL
WHERE resolved_at IS NOT NULL
  AND resolved_at <= $1
  AND (envelope IS NOT NULL OR payload IS NOT NULL)`

// DisposeResolvedDeadLetters removes retained event data after the configured retention
// boundary while preserving the incident record.
func DisposeResolvedDeadLetters(ctx context.Context, tx db.Tx, resolvedBefore time.Time) (int64, error) {
	if db.IsNilTx(tx) {
		return 0, ErrNoTransaction
	}
	if resolvedBefore.IsZero() {
		return 0, errors.New("outbox: dead-letter retention boundary is required")
	}
	tag, err := tx.Exec(ctx, disposeDeadLettersStatement, resolvedBefore.UTC())
	if err != nil {
		return 0, fmt.Errorf("outbox: disposing resolved dead letters: %w", err)
	}
	return tag.RowsAffected(), nil
}

const countStaleDeadLettersStatement = `SELECT count(*)
FROM platform.dead_letter
WHERE resolved_at IS NULL AND dead_lettered_at <= $1`

// CountStaleUnresolvedDeadLetters returns the unresolved incidents old enough to alert.
func CountStaleUnresolvedDeadLetters(ctx context.Context, tx db.Tx, olderThan time.Time) (int64, error) {
	if db.IsNilTx(tx) {
		return 0, ErrNoTransaction
	}
	if olderThan.IsZero() {
		return 0, errors.New("outbox: unresolved age boundary is required")
	}
	var count int64
	if err := tx.QueryRow(ctx, countStaleDeadLettersStatement, olderThan.UTC()).Scan(&count); err != nil {
		return 0, fmt.Errorf("outbox: counting stale unresolved dead letters: %w", err)
	}
	return count, nil
}
