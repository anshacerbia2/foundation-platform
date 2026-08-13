-- Scope idempotency keys to the authenticated caller. A globally keyed table lets one
-- caller consume another caller's key, contradicting the middleware ordering contract.
ALTER TABLE platform.idempotency_key
    ADD COLUMN scope TEXT;

-- This repository has not released a tagged version yet, but keep an upgrade path for
-- development databases. Existing globally-scoped keys retain their mutual exclusion in
-- a reserved namespace; new claims must always provide an authenticated caller scope.
UPDATE platform.idempotency_key SET scope = '__legacy__' WHERE scope IS NULL;

ALTER TABLE platform.idempotency_key
    ALTER COLUMN scope SET NOT NULL;

ALTER TABLE platform.idempotency_key
    ADD CONSTRAINT idempotency_key_scope_valid
    CHECK (btrim(scope) <> '' AND octet_length(scope) <= 512),
    ADD CONSTRAINT idempotency_key_key_valid
    CHECK (btrim(key) <> '' AND octet_length(key) <= 255),
    ADD CONSTRAINT idempotency_key_digest_valid
    CHECK (btrim(request_digest) <> '' AND octet_length(request_digest) <= 256);

ALTER TABLE platform.idempotency_key
    DROP CONSTRAINT idempotency_key_pkey;

ALTER TABLE platform.idempotency_key
    ADD PRIMARY KEY (scope, key);

ALTER TABLE platform.processed_event
    ADD CONSTRAINT processed_event_consumer_valid
    CHECK (btrim(consumer) <> '' AND octet_length(consumer) <= 255);

-- Track only partitions created by the maintenance function. This avoids deriving
-- retention boundaries from names or PostgreSQL's rendered partition expressions.
CREATE TABLE platform.outbox_partition (
    partition_name TEXT PRIMARY KEY,
    range_start    TIMESTAMPTZ NOT NULL,
    range_end      TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (range_end > range_start)
);

CREATE OR REPLACE FUNCTION platform.ensure_outbox_partitions(
    from_day DATE,
    through_day DATE
) RETURNS TABLE (partition_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    day_start DATE;
    child_name TEXT;
    qualified_child TEXT;
BEGIN
    IF from_day IS NULL OR through_day IS NULL OR through_day < from_day THEN
        RAISE EXCEPTION 'invalid outbox partition window';
    END IF;

    LOCK TABLE platform.outbox IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE platform.outbox_default IN ACCESS EXCLUSIVE MODE;

    FOR day_start IN
        SELECT from_day + offset
        FROM generate_series(0, through_day - from_day) AS offset
    LOOP
        child_name := 'outbox_' || to_char(day_start, 'YYYYMMDD');
        qualified_child := format('platform.%I', child_name);

        IF to_regclass(qualified_child) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %s (LIKE platform.outbox INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)',
                qualified_child
            );

            EXECUTE format(
                'WITH moved AS (DELETE FROM platform.outbox_default WHERE created_at >= $1 AND created_at < $2 RETURNING *) INSERT INTO %s SELECT * FROM moved',
                qualified_child
            ) USING
                day_start::timestamp AT TIME ZONE 'UTC',
                (day_start + 1)::timestamp AT TIME ZONE 'UTC';

            EXECUTE format(
                'ALTER TABLE platform.outbox ATTACH PARTITION %s FOR VALUES FROM (%L) TO (%L)',
                qualified_child,
                day_start::timestamp AT TIME ZONE 'UTC',
                (day_start + 1)::timestamp AT TIME ZONE 'UTC'
            );
        END IF;

        INSERT INTO platform.outbox_partition (partition_name, range_start, range_end)
        VALUES (
            child_name,
            day_start::timestamp AT TIME ZONE 'UTC',
            (day_start + 1)::timestamp AT TIME ZONE 'UTC'
        )
        ON CONFLICT ON CONSTRAINT outbox_partition_pkey DO NOTHING;

        partition_name := child_name;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION platform.drop_outbox_partitions(retain_after TIMESTAMPTZ)
RETURNS TABLE (partition_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    candidate RECORD;
    unpublished BOOLEAN;
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
        IF to_regclass(format('platform.%I', candidate.partition_name)) IS NULL THEN
            DELETE FROM platform.outbox_partition p
            WHERE p.partition_name = candidate.partition_name;
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM platform.%I WHERE published = FALSE)',
            candidate.partition_name
        ) INTO unpublished;

        IF NOT unpublished THEN
            EXECUTE format('DROP TABLE platform.%I', candidate.partition_name);
            DELETE FROM platform.outbox_partition p
            WHERE p.partition_name = candidate.partition_name;
            partition_name := candidate.partition_name;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;
