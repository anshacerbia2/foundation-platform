-- ---------------------------------------------------------------------------
-- Dead-letter resolution record
-- ---------------------------------------------------------------------------

-- What closed an incident, who closed it, and on what evidence.
--
-- Until now the only thing distinguishing a closed incident from an open one was resolved_at.
-- A timestamp records that somebody acted; it records nothing about why the action was
-- justified, and an incident closed with no stated reason is indistinguishable from one closed
-- by mistake. The frontier stops reporting the debt either way.
--
-- # What this schema can enforce, and what it cannot
--
-- It cannot know whether a reason is correct. Whether REPLAYED is the right closure for a
-- Membership revocation depends on the projection contract, the monotonicity rule, and what
-- counts as applied evidence -- none of which belongs in a schema shared by systems that have
-- never heard of a Membership. The consuming system owns that judgement, and the values in
-- resolution_type are its vocabulary rather than this schema's.
--
-- What it can enforce is that a resolution is COMPLETE. Either the incident is open and every
-- resolution column is absent, or it is closed and all four are present. There is no third
-- state, and the CHECK below is what makes that structural.
--
-- That is not bookkeeping. `UPDATE platform.dead_letter SET resolved_at = now()` is a single
-- statement any role holding UPDATE can run, and before this constraint it closed an incident
-- silently and completely. Now the same statement fails: closing requires naming a reason, an
-- author, and a reference to the evidence. It does not make the reason true -- the consuming
-- system's predicate does that -- but it removes the option of closing without one.
--
-- resolution_reference is deliberately untyped. It points at whatever the evidence is in the
-- consuming system: a delivery receipt, a replayed event identifier, a snapshot mark, a
-- reconciliation operation. A foreign key would need this schema to know which of those exists,
-- which is exactly the coupling the boundary refuses.

ALTER TABLE platform.dead_letter
    ADD COLUMN IF NOT EXISTS resolution_type      TEXT,
    ADD COLUMN IF NOT EXISTS resolved_by          TEXT,
    ADD COLUMN IF NOT EXISTS resolution_reference TEXT;

-- Rows closed before this contract existed.
--
-- They carry a resolved_at and no account of why, which is the state the constraint below
-- forbids. Two options: reopen them, or record that they were closed without evidence.
--
-- Reopening is a policy decision about live incidents, and a migration is the wrong place to
-- take it -- it would re-block every consumer for incidents someone already judged closed.
-- Recording is honest and reversible: these rows say plainly that they predate the contract,
-- and `resolution_type = 'legacy'` is greppable by anyone auditing how incidents have been
-- closed in this estate.
UPDATE platform.dead_letter
   SET resolution_type      = 'legacy',
       resolved_by          = 'unknown',
       resolution_reference = 'closed before the resolution contract existed; no evidence recorded'
 WHERE resolved_at IS NOT NULL
   AND resolution_type IS NULL;

-- All four, or none.
--
-- Blank strings are refused alongside NULLs. A resolution recorded as an empty resolution_type is
-- the same absence wearing a value, and it would satisfy a NOT NULL check while telling an
-- auditor nothing.
--
-- coalesce rather than length(btrim(x)) > 0, and the difference is the whole constraint. A CHECK
-- that evaluates to NULL PASSES. Written as `length(btrim(resolution_type)) > 0`, a column that is
-- simply not set yields NULL, the branch yields NULL, `false OR NULL` is NULL, and the row is
-- accepted — so the constraint refused the typo and allowed the omission, which is the commoner
-- mistake by a wide margin. The tests beside this file found it; the reasoning that wrote it did
-- not.
ALTER TABLE platform.dead_letter
    DROP CONSTRAINT IF EXISTS dead_letter_resolution_complete;

ALTER TABLE platform.dead_letter
    ADD CONSTRAINT dead_letter_resolution_complete CHECK (
        (
            resolved_at IS NULL
            AND resolution_type IS NULL
            AND resolved_by IS NULL
            AND resolution_reference IS NULL
        )
        OR
        (
            resolved_at IS NOT NULL
            AND coalesce(btrim(resolution_type), '') <> ''
            AND coalesce(btrim(resolved_by), '') <> ''
            AND coalesce(btrim(resolution_reference), '') <> ''
        )
    );

-- The unresolved index already covers the frontier's debt query. This one covers the other
-- direction: an audit asking how incidents have been closed, which is the question the columns
-- above exist to answer.
CREATE INDEX IF NOT EXISTS dead_letter_resolved
    ON platform.dead_letter (resolution_type, resolved_at)
    WHERE resolved_at IS NOT NULL;
