-- Per-consumer delivery (ADR-GLB-018, TDD-foundation-platform-001 §Per-Consumer Delivery).
--
-- One outbox is delivered to several named consumers, each with its own subscription,
-- publication state, evidence and dead letters. The event stays one row in platform.outbox; what
-- happened to it at a consumer is that consumer's row in platform.outbox_delivery.
--
-- Upgrade: an event still unpublished when this runs has no delivery and is never dispatched.
-- Drain the outbox first. No production estate exists, and this migration invents no consumer
-- for those events.

-- ---------------------------------------------------------------------------
-- Subscriptions
-- ---------------------------------------------------------------------------

-- A consumer's event types. Replaced, never edited: a new subscription retires the old one, and
-- no runtime role updates event_types (ADR-GLB-018 §5.1).
CREATE TABLE IF NOT EXISTS platform.subscription (
    consumer      TEXT        NOT NULL,
    event_types   TEXT[]      NOT NULL,
    subscribed_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    retired_at    TIMESTAMPTZ,
    PRIMARY KEY (consumer, subscribed_at),
    CONSTRAINT subscription_consumer_named CHECK (btrim(consumer) <> ''),
    CONSTRAINT subscription_types_present CHECK (cardinality(event_types) > 0),
    CONSTRAINT subscription_retired_after CHECK (retired_at IS NULL OR retired_at >= subscribed_at)
);

-- One active subscription per consumer.
CREATE UNIQUE INDEX IF NOT EXISTS subscription_active
    ON platform.subscription (consumer) WHERE retired_at IS NULL;

-- ---------------------------------------------------------------------------
-- Deliveries
-- ---------------------------------------------------------------------------

-- One row per event and subscribed consumer, written in the event's own transaction. created_at
-- is the event's, so a delivery lives in the same day's partition as its event and both are
-- dropped together.
CREATE TABLE IF NOT EXISTS platform.outbox_delivery (
    created_at      TIMESTAMPTZ NOT NULL,
    event_id        UUID        NOT NULL,
    consumer        TEXT        NOT NULL,
    sequence        BIGINT      NOT NULL,
    event_type      TEXT        NOT NULL,
    priority        SMALLINT    NOT NULL,
    published       BOOLEAN     NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    last_error      TEXT,
    failure_class   TEXT,
    first_failed_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    lease_id        UUID,
    leased_until    TIMESTAMPTZ,
    PRIMARY KEY (created_at, event_id, consumer),
    CONSTRAINT outbox_delivery_lease_complete CHECK ((lease_id IS NULL) = (leased_until IS NULL))
) PARTITION BY RANGE (created_at);

-- The dispatcher's index: one consumer's unpublished deliveries, in claim order.
CREATE INDEX IF NOT EXISTS outbox_delivery_unpublished
    ON platform.outbox_delivery (consumer, priority, sequence) WHERE published = FALSE;

-- A default partition, for the reason platform.outbox has one: an append inside a domain
-- transaction must never fail for want of a partition.
CREATE TABLE IF NOT EXISTS platform.outbox_delivery_default
    PARTITION OF platform.outbox_delivery DEFAULT;

-- A delivery for each day partition the outbox already has, so the two stay in lockstep.
DO $$
DECLARE
    p RECORD;
    child TEXT;
BEGIN
    FOR p IN SELECT partition_name, range_start, range_end FROM platform.outbox_partition LOOP
        child := replace(p.partition_name, 'outbox_', 'outbox_delivery_');
        IF to_regclass(format('platform.%I', child)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE platform.%I PARTITION OF platform.outbox_delivery FOR VALUES FROM (%L) TO (%L)',
                child, p.range_start, p.range_end);
        END IF;
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- The outbox row is the event alone
-- ---------------------------------------------------------------------------

-- Publication state moves to the delivery. Kept here it would describe one consumer and mislead
-- every other (ADR-GLB-018 §5.2).
DROP INDEX IF EXISTS platform.outbox_unpublished; -- atlas:destructive-approved ADR-GLB-018: publication state is per delivery
ALTER TABLE platform.outbox DROP CONSTRAINT IF EXISTS outbox_lease_complete; -- atlas:destructive-approved ADR-GLB-018: the lease is per delivery
ALTER TABLE platform.outbox
    DROP COLUMN IF EXISTS published,       -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS published_at,    -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS attempts,        -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS last_error,      -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS failure_class,   -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS first_failed_at, -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS next_attempt_at, -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS lease_id,        -- atlas:destructive-approved ADR-GLB-018
    DROP COLUMN IF EXISTS leased_until;    -- atlas:destructive-approved ADR-GLB-018

-- ---------------------------------------------------------------------------
-- A dead letter is one consumer's
-- ---------------------------------------------------------------------------

-- Keyed by the event and the consumer that refused it (ADR-GLB-018 §5.3). A row from before
-- v0.2.8 has no consumer; NULLS NOT DISTINCT still holds it once per event.
ALTER TABLE platform.dead_letter DROP CONSTRAINT IF EXISTS dead_letter_pkey; -- atlas:destructive-approved ADR-GLB-018: the key becomes (event_id, consumer)
--
-- Added once rather than dropped and re-added: the set is applied on every deployment, and
-- re-creating a unique constraint rebuilds its index each time.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'platform.dead_letter'::regclass AND conname = 'dead_letter_delivery') THEN
        ALTER TABLE platform.dead_letter
            ADD CONSTRAINT dead_letter_delivery UNIQUE NULLS NOT DISTINCT (event_id, consumer);
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Partition maintenance covers both tables
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION platform.ensure_outbox_partitions(
    from_day DATE,
    through_day DATE
) RETURNS TABLE (partition_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    day_start DATE;
    child_name TEXT;
    delivery_child TEXT;
    range_from TIMESTAMPTZ;
    range_to TIMESTAMPTZ;
BEGIN
    IF from_day IS NULL OR through_day IS NULL OR through_day < from_day THEN
        RAISE EXCEPTION 'invalid outbox partition window';
    END IF;

    LOCK TABLE platform.outbox IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE platform.outbox_default IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE platform.outbox_delivery IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE platform.outbox_delivery_default IN ACCESS EXCLUSIVE MODE;

    FOR day_start IN
        SELECT from_day + day_offset
        FROM generate_series(0, through_day - from_day) AS days(day_offset)
    LOOP
        child_name := 'outbox_' || to_char(day_start, 'YYYYMMDD');
        delivery_child := 'outbox_delivery_' || to_char(day_start, 'YYYYMMDD');
        range_from := day_start::timestamp AT TIME ZONE 'UTC';
        range_to := (day_start + 1)::timestamp AT TIME ZONE 'UTC';

        IF to_regclass(format('platform.%I', child_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE platform.%I (LIKE platform.outbox INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)',
                child_name);
            EXECUTE format(
                'WITH moved AS (DELETE FROM platform.outbox_default WHERE created_at >= $1 AND created_at < $2 RETURNING *) INSERT INTO platform.%I SELECT * FROM moved',
                child_name) USING range_from, range_to;
            EXECUTE format(
                'ALTER TABLE platform.outbox ATTACH PARTITION platform.%I FOR VALUES FROM (%L) TO (%L)',
                child_name, range_from, range_to);
        END IF;

        IF to_regclass(format('platform.%I', delivery_child)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE platform.%I (LIKE platform.outbox_delivery INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)',
                delivery_child);
            EXECUTE format(
                'WITH moved AS (DELETE FROM platform.outbox_delivery_default WHERE created_at >= $1 AND created_at < $2 RETURNING *) INSERT INTO platform.%I SELECT * FROM moved',
                delivery_child) USING range_from, range_to;
            EXECUTE format(
                'ALTER TABLE platform.outbox_delivery ATTACH PARTITION platform.%I FOR VALUES FROM (%L) TO (%L)',
                delivery_child, range_from, range_to);
        END IF;

        INSERT INTO platform.outbox_partition (partition_name, range_start, range_end)
        VALUES (child_name, range_from, range_to)
        ON CONFLICT ON CONSTRAINT outbox_partition_pkey DO NOTHING;

        partition_name := child_name;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- A day is dropped once every delivery it owes is published or dead-lettered, which marks the
-- delivery published (ADR-GLB-018 §5.3). An event no consumer subscribed to owes nothing.
CREATE OR REPLACE FUNCTION platform.drop_outbox_partitions(retain_after TIMESTAMPTZ)
RETURNS TABLE (partition_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    candidate RECORD;
    delivery_child TEXT;
    owed BOOLEAN;
BEGIN
    IF retain_after IS NULL THEN
        RAISE EXCEPTION 'outbox retention boundary is required';
    END IF;

    FOR candidate IN
        SELECT p.partition_name
        FROM platform.outbox_partition p
        WHERE p.range_end <= retain_after
        ORDER BY p.range_start
    LOOP
        delivery_child := replace(candidate.partition_name, 'outbox_', 'outbox_delivery_');

        IF to_regclass(format('platform.%I', candidate.partition_name)) IS NULL THEN
            IF to_regclass(format('platform.%I', delivery_child)) IS NOT NULL THEN
                EXECUTE format('DROP TABLE platform.%I', delivery_child);
            END IF;
            DELETE FROM platform.outbox_partition p
            WHERE p.partition_name = candidate.partition_name;
            CONTINUE;
        END IF;

        owed := FALSE;
        IF to_regclass(format('platform.%I', delivery_child)) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (SELECT 1 FROM platform.%I WHERE published = FALSE)',
                delivery_child
            ) INTO owed;
        END IF;

        IF NOT owed THEN
            IF to_regclass(format('platform.%I', delivery_child)) IS NOT NULL THEN
                EXECUTE format('DROP TABLE platform.%I', delivery_child);
            END IF;
            EXECUTE format('DROP TABLE platform.%I', candidate.partition_name);
            DELETE FROM platform.outbox_partition p
            WHERE p.partition_name = candidate.partition_name;
            partition_name := candidate.partition_name;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;
