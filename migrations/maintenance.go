package migrations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
)

const ensurePartitionsStatement = `SELECT COALESCE(array_agg(partition_name ORDER BY partition_name), '{}')
FROM platform.ensure_outbox_partitions($1::date, $2::date)`

// EnsureOutboxPartitions creates every UTC daily partition in the inclusive window.
// It must run through a pool configured with the consuming system's migration role.
func EnsureOutboxPartitions(ctx context.Context, tx db.Tx, fromDay, throughDay time.Time) ([]string, error) {
	if db.IsNilTx(tx) {
		return nil, errors.New("migrations: a transaction handle is required")
	}
	if fromDay.IsZero() || throughDay.IsZero() {
		return nil, errors.New("migrations: partition window is required")
	}
	from := fromDay.UTC().Format(time.DateOnly)
	through := throughDay.UTC().Format(time.DateOnly)
	if through < from {
		return nil, errors.New("migrations: partition window ends before it starts")
	}

	var names []string
	if err := tx.QueryRow(ctx, ensurePartitionsStatement, from, through).Scan(&names); err != nil {
		return nil, fmt.Errorf("migrations: ensuring outbox partitions %s through %s: %w", from, through, err)
	}
	return names, nil
}

const dropPartitionsStatement = `SELECT COALESCE(array_agg(partition_name ORDER BY partition_name), '{}')
FROM platform.drop_outbox_partitions($1)`

// DropPublishedOutboxPartitions drops tracked partitions ending on or before retainAfter
// only when no unpublished row remains in them.
func DropPublishedOutboxPartitions(ctx context.Context, tx db.Tx, retainAfter time.Time) ([]string, error) {
	if db.IsNilTx(tx) {
		return nil, errors.New("migrations: a transaction handle is required")
	}
	if retainAfter.IsZero() {
		return nil, errors.New("migrations: retention boundary is required")
	}

	var names []string
	if err := tx.QueryRow(ctx, dropPartitionsStatement, retainAfter.UTC()).Scan(&names); err != nil {
		return nil, fmt.Errorf("migrations: dropping published outbox partitions: %w", err)
	}
	return names, nil
}
