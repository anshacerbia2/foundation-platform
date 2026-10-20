package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anshacerbia2/foundation-platform/db"
)

// MinimumRetention is the shortest window PurgeCompleted accepts.
//
// A key purged while a client still retries is a new request to the next retry, which executes the
// command again. Stripe, which prunes its own keys: "you can remove keys from the system
// automatically after they're at least 24 hours old. We generate a new request if a key is reused
// after the original is pruned." A service publishes its own window, at least this long and at
// least as long as any of its clients retries (STD-GLB-001 1.7.0).
const MinimumRetention = 24 * time.Hour

// ErrRetentionTooShort is a window below MinimumRetention.
var ErrRetentionTooShort = errors.New("idempotency: key retention is shorter than 24 hours")

const purgeStatement = `SELECT platform.purge_idempotency_keys(make_interval(secs => $1))`

// PurgeCompleted deletes the keys completed more than retention ago and reports how many.
//
// The boundary is computed by the database clock, which wrote completed_at, so the floor does not
// depend on the caller's clock. An uncompleted key is never purged: its effect may have committed
// without its response, and a retry finding no key would execute the command twice.
//
// A consumer calls it from its maintenance stage with the window it publishes. The role needs
// SELECT and DELETE on platform.idempotency_key.
func PurgeCompleted(ctx context.Context, tx db.Tx, retention time.Duration) (int64, error) {
	if db.IsNilTx(tx) {
		return 0, ErrNoTransaction
	}
	if retention < MinimumRetention {
		return 0, ErrRetentionTooShort
	}
	var purged int64
	if err := tx.QueryRow(ctx, purgeStatement, retention.Seconds()).Scan(&purged); err != nil {
		return 0, fmt.Errorf("idempotency: purging completed keys: %w", err)
	}
	return purged, nil
}
