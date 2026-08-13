// Package inbox prevents duplicate broker deliveries from applying an effect twice.
package inbox

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anshacerbia2/foundation-platform/db"
	"github.com/anshacerbia2/foundation-platform/event"
	"github.com/anshacerbia2/foundation-platform/id"
)

var (
	ErrNoTransaction   = errors.New("inbox: a transaction handle is required")
	ErrNoConsumer      = errors.New("inbox: consumer is required")
	ErrNoEvent         = errors.New("inbox: event identifier is required")
	ErrNoEventType     = errors.New("inbox: event type is required")
	ErrConsumerTooLong = errors.New("inbox: consumer exceeds 255 bytes")
)

const guardStatement = `INSERT INTO platform.processed_event (event_id, consumer, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (event_id, consumer) DO NOTHING`

// Guard registers an event inside the transaction that applies its effect and reports
// whether this logical consumer is seeing the delivery for the first time.
func Guard(ctx context.Context, tx db.Tx, consumer string, eventID id.UUID, eventType event.Type) (bool, error) {
	if db.IsNilTx(tx) {
		return false, ErrNoTransaction
	}
	consumer = strings.TrimSpace(consumer)
	if consumer == "" {
		return false, ErrNoConsumer
	}
	if len(consumer) > 255 {
		return false, ErrConsumerTooLong
	}
	if eventID.IsNil() {
		return false, ErrNoEvent
	}
	if eventType == "" {
		return false, ErrNoEventType
	}

	tag, err := tx.Exec(ctx, guardStatement, eventID.String(), consumer, eventType.String())
	if err != nil {
		return false, fmt.Errorf("inbox: guarding %s for %q: %w", eventID, consumer, err)
	}
	return tag.RowsAffected() == 1, nil
}
