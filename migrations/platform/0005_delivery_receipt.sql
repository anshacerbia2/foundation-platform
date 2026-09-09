-- ---------------------------------------------------------------------------
-- Delivery receipts
-- ---------------------------------------------------------------------------

-- What the producer witnessed for itself.
--
-- The dead-letter resolution contract needs to establish that a specific event reached a
-- specific consumer. Every other route to that rests on something the consumer reports about
-- its own progress; this one does not, because the producer's own dispatcher was the party
-- that delivered it and read the answer.
--
-- Keyed (event_id, consumer), the same shape as platform.processed_event on the consumer side.
-- The symmetry is deliberate: one row per event per consumer is what makes a receipt an
-- answer about a delivery rather than about an event, and platform.outbox's single `published`
-- flag is exactly what cannot express that.
--
-- # Why `evidence` is a column and not an assumption
--
-- A successful publication establishes one of two different facts. Today's transport is a
-- direct HTTP call to a consumer that applies the event inside the same transaction as its
-- inbox guard and only then answers, so its acknowledgement means applied. A broker
-- acknowledgement means the message was handed on, and the consumer may still be hours behind.
--
-- Only the first is resolution evidence. Storing which one was established means that
-- introducing a broker cannot silently weaken resolution: the receipts start recording
-- 'transport_accepted', the resolution predicate stops finding proof, and the failure is
-- visible instead of quiet. The enforcement of that lives in Go -- outbox.Receipt carries an
-- unexported field, so 'consumer_applied' is reachable only through the constructor that
-- requires the consumer's own marker -- and the CHECK here is the second copy of the rule,
-- for the same reason grants are: a value written by something other than the dispatcher must
-- not be able to name a class the contract does not define.
--
-- # Retention
--
-- Not bounded here, and that is a decision rather than an oversight. A receipt is the
-- evidence that an incident was resolved correctly, so disposing of it on the outbox's
-- schedule would remove the record of why a security event was closed while the dead-letter
-- row it justified is still present. Retention has to be at least as long as
-- platform.dead_letter's, and setting that number needs the resolution contract to exist
-- first. Recorded as P1.

CREATE TABLE IF NOT EXISTS platform.delivery_receipt (
    event_id     UUID        NOT NULL,
    consumer     TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    evidence     TEXT        NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),

    PRIMARY KEY (event_id, consumer),

    CONSTRAINT delivery_receipt_evidence_check
        CHECK (evidence IN ('consumer_applied', 'transport_accepted'))
);

-- statement_timestamp() rather than now(), for the reason 0003 records: the dispatcher's
-- transaction spans a publish attempt per row in the batch, so the transaction's start is not
-- when any particular row was delivered.

-- The resolution predicate asks "is there consumer_applied evidence for this event and this
-- consumer", so the index covers the discriminating column rather than the whole key.
CREATE INDEX IF NOT EXISTS delivery_receipt_applied
    ON platform.delivery_receipt (event_id, consumer)
    WHERE evidence = 'consumer_applied';
