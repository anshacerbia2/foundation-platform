package migrations

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anshacerbia2/foundation-platform/db/dbtest"
)

func TestEnsureOutboxPartitionsUsesUTCDateBounds(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{[]string{"outbox_20260813", "outbox_20260814"}}}
	from := time.Date(2026, 8, 13, 23, 0, 0, 0, time.FixedZone("west", -7*60*60))
	through := from.Add(24 * time.Hour)

	names, err := EnsureOutboxPartitions(context.Background(), tx, from, through)
	if err != nil {
		t.Fatalf("EnsureOutboxPartitions: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("partitions = %v", names)
	}
	call := tx.Only(t)
	if call.Args[0] != "2026-08-14" || call.Args[1] != "2026-08-15" {
		t.Errorf("UTC bounds = %v", call.Args)
	}
}

func TestEnsureOutboxPartitionsRejectsAnInvalidWindow(t *testing.T) {
	now := time.Now()
	if _, err := EnsureOutboxPartitions(context.Background(), &dbtest.Tx{}, now, now.Add(-24*time.Hour)); err == nil {
		t.Error("EnsureOutboxPartitions accepted a reversed window")
	}
}

func TestDropPublishedOutboxPartitionsReturnsDroppedNames(t *testing.T) {
	tx := &dbtest.Tx{RowValues: []any{[]string{"outbox_20260701"}}}
	names, err := DropPublishedOutboxPartitions(context.Background(), tx, time.Now())
	if err != nil {
		t.Fatalf("DropPublishedOutboxPartitions: %v", err)
	}
	if len(names) != 1 || names[0] != "outbox_20260701" {
		t.Errorf("partitions = %v", names)
	}
}

func TestPartitionMaintenanceRequiresATransaction(t *testing.T) {
	var tx *dbtest.Tx
	if _, err := EnsureOutboxPartitions(context.Background(), tx, time.Now(), time.Now()); err == nil {
		t.Error("EnsureOutboxPartitions accepted a typed nil transaction")
	}
	if _, err := DropPublishedOutboxPartitions(context.Background(), tx, time.Now()); err == nil {
		t.Error("DropPublishedOutboxPartitions accepted a typed nil transaction")
	}
}

func TestPartitionMaintenanceWrapsDatabaseFailures(t *testing.T) {
	injected := errors.New("database unavailable")
	tx := &dbtest.Tx{RowErr: injected}
	_, err := EnsureOutboxPartitions(context.Background(), tx, time.Now(), time.Now())
	if !errors.Is(err, injected) {
		t.Errorf("error = %v", err)
	}
}
