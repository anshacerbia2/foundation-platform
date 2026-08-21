-- Platform schema: the propagation substrate shared by both control planes.
--
-- Specified by TDD-foundation-platform-001. Each consuming database carries its own copy
-- of this schema. The two are unrelated and never joined; the shared name reflects shared
-- code, not shared storage.
--
-- Row-Level Security is deliberately absent from every table here, and the reason is the
-- query model rather than an oversight. The dispatcher must claim every unpublished row
-- regardless of which tenant a payload concerns, and a tenant predicate bound from
-- app.tenant_id would return it nothing. STD-GLB-002 Multi-Tenancy & Isolation carves out exactly this case.
--
-- The consequence must be stated rather than left implicit: payload and envelope carry
-- tenant-scoped data, so this schema is a cross-tenant readable surface. Its protection
-- is the boundary in STD-GLB-002 Data Ownership & Access — the runtime role owns no table here, holds
-- neither SUPERUSER nor BYPASSRLS, and holds no DDL privilege; migration runs under a
-- separate role; and no interface exposes a platform table to a tenant-facing caller.

CREATE SCHEMA IF NOT EXISTS platform;

-- ---------------------------------------------------------------------------
-- Outbox
-- ---------------------------------------------------------------------------

-- A sequence rather than an identity column, so the value stays monotonic across
-- partitions. The append statement writes the same value into the CloudEvents
-- streamposition extension. It supplies dispatch ordering and the high-water mark a
-- projection consumer compares buffered events against. It is a stream position and not
-- a broker offset or entity identifier, which is
-- why the prohibition on exposing sequential identifiers in STD-GLB-002 does not reach it.
CREATE SEQUENCE IF NOT EXISTS platform.outbox_sequence AS BIGINT;

CREATE TABLE IF NOT EXISTS platform.outbox (
    event_id     UUID        NOT NULL DEFAULT gen_random_uuid(),
    sequence     BIGINT      NOT NULL DEFAULT nextval('platform.outbox_sequence'),
    event_type   TEXT        NOT NULL,
    aggregate_id UUID        NOT NULL,
    priority     SMALLINT    NOT NULL DEFAULT 100,
    payload      JSONB       NOT NULL,
    envelope     JSONB       NOT NULL,
    published    BOOLEAN     NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT,

    -- The branch the dispatcher took, named by the design's dispatch algorithm alongside
    -- last_error. Kept on the row rather than derived from the message, because the
    -- classification decides whether the row is retried or abandoned and an operator
    -- reading a stuck row needs to see which was chosen.
    failure_class TEXT,

    -- When this row first failed to publish. platform.dead_letter requires it NOT NULL,
    -- and by the time a row is dead-lettered the first failure is several attempts in the
    -- past, so it cannot be reconstructed at that moment.
    first_failed_at TIMESTAMPTZ,

    -- The earliest a failed row may be claimed again.
    --
    -- Backoff is mandated by STD-GLB-004 and cannot be expressed without it: a worker
    -- that slept instead would hold the row's lock while sleeping, which converts a
    -- delay for one event into a stall for the batch.
    next_attempt_at TIMESTAMPTZ,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (created_at, event_id)
) PARTITION BY RANGE (created_at);

-- Recorded deviation from STD-GLB-004, whose exception clause reads "None".
--
-- That standard names event_id as the outbox primary key. PostgreSQL requires the
-- partition key in the primary key of a partitioned table, and forbids a UNIQUE
-- constraint that omits it, so event_id alone cannot be enforced unique here.
-- ADR-GLB-003 requires partitioning, and the alternative is row-by-row deletion on the
-- hottest table in the schema, so partitioning wins.
--
-- The deviation is contained rather than resolved: consumers deduplicate against
-- platform.processed_event, where event_id is part of an enforced unique key. A duplicate
-- across two partitions would publish twice and be discarded once at each consumer.

-- The dispatcher's only index. It covers unpublished rows alone, so it stays small
-- regardless of history, and its column order matches the claim query's ORDER BY exactly.
CREATE INDEX IF NOT EXISTS outbox_unpublished
    ON platform.outbox (priority, sequence) WHERE published = FALSE;

-- A default partition, so an insert never fails for want of one.
--
-- The append happens inside the caller's domain transaction. A missing daily partition
-- would therefore abort a membership revocation, and losing a security state change
-- because a scheduled job did not run is a far worse outcome than the cost this partition
-- carries: attaching a new range partition must scan it for conflicting rows.
--
-- Rows landing here are an operational defect, not a resting place. The partition
-- creation job runs OUTBOX_PARTITION_AHEAD days in front, and a non-empty default
-- partition is alerted at the "missing future partition" threshold in the design's
-- operational notes.
CREATE TABLE IF NOT EXISTS platform.outbox_default PARTITION OF platform.outbox DEFAULT;

-- ---------------------------------------------------------------------------
-- Deduplication
-- ---------------------------------------------------------------------------

-- The key is (event_id, consumer) and not event_id alone.
--
-- One deployable runs several logical consumers over the same event: identity-control
-- applies a context projection, removes Keycloak sessions, and translates the event
-- onward. Under a single-column key the first of those to record its row would make every
-- other one observe a conflict and acknowledge the delivery without applying its effect.
-- For a revocation that is silent non-enforcement rather than an error, which is the
-- failure this key shape exists to prevent.
CREATE TABLE IF NOT EXISTS platform.processed_event (
    event_id     UUID        NOT NULL,
    consumer     TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);

-- ---------------------------------------------------------------------------
-- Dead letter
-- ---------------------------------------------------------------------------

-- A row here means an event was accepted by a domain transaction and never reached its
-- consumer. For a priority event that is a containment failure, so the alert on this
-- table is not a queue-depth alert.
--
-- Only a poison classification reaches this table from the priority lane. Broker
-- unavailability returns the row to the unpublished pool instead, because three attempts
-- then abandon is calibrated for an event that will never succeed, and applying it to an
-- outage discards a revocation that would have published a minute later.
--
-- Retention is bounded because envelope and payload carry restricted identity and
-- organization context, and EAD-003 §5.4 prohibits indefinite retention. Disposal
-- clears envelope and payload and keeps the incident fields, so the record of the failure
-- outlives the data it carried. An unresolved row is never disposed; its age is alerted
-- so the table forces escalation instead of accumulating undelivered security events.
CREATE TABLE IF NOT EXISTS platform.dead_letter (
    event_id         UUID        PRIMARY KEY,
    event_type       TEXT        NOT NULL,
    envelope         JSONB,
    payload          JSONB,
    consumer         TEXT,
    failure_class    TEXT        NOT NULL,
    failure_detail   TEXT        NOT NULL,
    attempts         INTEGER     NOT NULL,
    first_failed_at  TIMESTAMPTZ NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at      TIMESTAMPTZ
);

-- envelope and payload are nullable here while they are NOT NULL in the outbox, because
-- disposal must be able to remove them without removing the incident record. A NOT NULL
-- column would force the retention job to choose between deleting the row and keeping the
-- data, and both of those lose something the design requires keeping.

CREATE INDEX IF NOT EXISTS dead_letter_unresolved
    ON platform.dead_letter (dead_lettered_at) WHERE resolved_at IS NULL;

-- ---------------------------------------------------------------------------
-- Idempotency
-- ---------------------------------------------------------------------------

-- A repeated key carrying a different request_digest is rejected with 409. A repeated key
-- carrying the same digest replays the stored response without re-executing the operation.
CREATE TABLE IF NOT EXISTS platform.idempotency_key (
    key             TEXT        PRIMARY KEY,
    request_digest  TEXT        NOT NULL,
    response_status INTEGER,
    response_body   JSONB,
    claimed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
