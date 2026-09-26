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

// ReceiptReference is how a dead-letter closure cites the receipt it rests on, in its
// resolution_reference column.
//
// It is this module's format because it names this module's table and key, and because
// PruneDeliveryReceipts reads it: a receipt cited this way is retained for as long as the closure
// record citing it, which is forever. A host writing the reference any other way keeps its closures
// but loses that protection, so hosts build the string here rather than restating it.
func ReceiptReference(eventID, consumer string) string {
	return "platform.delivery_receipt:" + eventID + ":" + consumer
}

// pruneReceiptsStatement deletes receipts that no closure could still need.
//
// Three conditions, and each is a different reason a receipt must survive:
//
//   - Age. A receipt younger than the boundary is kept, so a resolution in progress finds it.
//   - Any open incident. While one dead letter is unresolved, nothing is pruned. Which receipt
//     could close it is the publishing system's rule, not this module's -- REPLAYED reads the
//     event's own receipt, and SUPERSEDED reads a receipt for a different event this module cannot
//     identify -- so the only generic answer is to keep all of them until the debt is closed.
//   - Citation. A receipt a closure names in resolution_reference is kept permanently, because the
//     closure record is: disposal removes a dead letter's payload and never its resolution, and a
//     resolution citing a receipt that no longer exists explains nothing.
const pruneReceiptsStatement = `DELETE FROM platform.delivery_receipt r
WHERE r.recorded_at <= $1
  AND NOT EXISTS (
        SELECT 1 FROM platform.dead_letter d WHERE d.resolved_at IS NULL)
  AND NOT EXISTS (
        SELECT 1 FROM platform.dead_letter d
         WHERE d.resolution_reference = 'platform.delivery_receipt:' || r.event_id::text || ':' || r.consumer)`

// PruneDeliveryReceipts removes delivery receipts recorded at or before recordedBefore that no
// closure cites, and removes none while any dead letter is unresolved.
//
// It runs as the migration role, never as a runtime: no runtime role holds DELETE on evidence.
func PruneDeliveryReceipts(ctx context.Context, tx db.Tx, recordedBefore time.Time) (int64, error) {
	if db.IsNilTx(tx) {
		return 0, ErrNoTransaction
	}
	if recordedBefore.IsZero() {
		return 0, errors.New("outbox: delivery-receipt retention boundary is required")
	}
	tag, err := tx.Exec(ctx, pruneReceiptsStatement, recordedBefore.UTC())
	if err != nil {
		return 0, fmt.Errorf("outbox: pruning delivery receipts: %w", err)
	}
	return tag.RowsAffected(), nil
}
