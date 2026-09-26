-- ---------------------------------------------------------------------------
-- Dead-letter waiver
-- ---------------------------------------------------------------------------

-- A waiver is an operational exception, recorded, and kept apart from a closure.
--
-- Some incidents have no corrective path: the consumer that refused the event has been
-- decommissioned, so no replay can reach it and no receipt will ever justify closing it. Left
-- alone, such a row alerts as stale forever and keeps its restricted payload forever. An operator
-- needs a sanctioned way to say "known, accepted, until this date" -- and the only one available
-- was a closure, which says something different: that the authority the event carried reached
-- the consumer.
--
-- So a waiver has its own columns and never touches the four resolution columns. A host reading
-- debt from resolved_at keeps reading it from resolved_at, and a waiver cannot make an incident
-- look delivered. What a waiver may change is operational: whether the stale alert fires, and
-- whether the payload may be disposed. Which incidents may be waived, for how long, and by whom
-- is the consuming system's rule. This schema enforces only that a waiver is complete.
--
-- It expires. A waiver past waived_until stops silencing the alert, so an exception somebody
-- forgot becomes a question again rather than a permanent blind spot.

ALTER TABLE platform.dead_letter
    ADD COLUMN IF NOT EXISTS waived_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS waived_until  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS waived_by     TEXT,
    ADD COLUMN IF NOT EXISTS waiver_reason TEXT;

-- All four, or none, and the expiry after the waiver. coalesce for the same reason 0006 gives: a
-- CHECK evaluating to NULL passes, so a bare length() test would accept an omitted column.
ALTER TABLE platform.dead_letter
    DROP CONSTRAINT IF EXISTS dead_letter_waiver_complete;

ALTER TABLE platform.dead_letter
    ADD CONSTRAINT dead_letter_waiver_complete CHECK (
        (
            waived_at IS NULL
            AND waived_until IS NULL
            AND waived_by IS NULL
            AND waiver_reason IS NULL
        )
        OR
        (
            waived_at IS NOT NULL
            AND waived_until IS NOT NULL
            AND waived_until > waived_at
            AND coalesce(btrim(waived_by), '') <> ''
            AND coalesce(btrim(waiver_reason), '') <> ''
        )
    );
