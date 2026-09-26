-- ---------------------------------------------------------------------------
-- Outbox lease
-- ---------------------------------------------------------------------------

-- The dispatcher used to claim, publish and settle a batch inside one transaction, so the row
-- lock was the lease. That transaction stayed open for as long as the slowest consumer took to
-- answer, for every row in the batch. It held back vacuum and kept a pooled connection busy for
-- the whole batch. A crash mid-batch also rolled back outcomes already recorded for rows that
-- had been delivered, and those rows were then delivered again.
--
-- A lease replaces the lock. A short transaction claims a batch and stamps each row with the
-- claim's identifier and an expiry. Publication happens outside any transaction, and each
-- outcome commits in a transaction of its own, fenced on lease_id: a worker whose lease was
-- taken over records nothing.
--
-- The cost is recovery time. A dispatcher that crashes leaves its rows claimed until
-- leased_until passes, where the lock used to release them at once. A graceful shutdown still
-- releases them immediately.
--
-- ALTER TABLE on the partitioned parent reaches every partition, existing and future.

ALTER TABLE platform.outbox
    ADD COLUMN IF NOT EXISTS lease_id     UUID,
    ADD COLUMN IF NOT EXISTS leased_until TIMESTAMPTZ;

-- Both or neither. A lease with no expiry would hold a row forever, and an expiry with no
-- identifier could not be fenced.
ALTER TABLE platform.outbox
    DROP CONSTRAINT IF EXISTS outbox_lease_complete;

ALTER TABLE platform.outbox
    ADD CONSTRAINT outbox_lease_complete CHECK ((lease_id IS NULL) = (leased_until IS NULL));
