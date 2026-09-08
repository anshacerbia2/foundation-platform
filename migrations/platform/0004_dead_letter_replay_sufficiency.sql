-- ---------------------------------------------------------------------------
-- Dead-letter replay sufficiency
-- ---------------------------------------------------------------------------

-- A dead letter could not be replayed from its own row.
--
-- Replaying an abandoned delivery means appending it to the outbox again. platform.outbox
-- requires aggregate_id NOT NULL, and takes priority to decide which lane carries it.
-- platform.dead_letter retained neither. It kept event_id, event_type, envelope and
-- payload -- everything except the two columns the destination demands.
--
-- So a replay had to read the original outbox row, and platform.outbox is partitioned with
-- retention. Once the partition holding it was dropped, the incident record survived and
-- the ability to act on it did not:
--
--   partition gone -> aggregate_id gone -> replay impossible
--
-- That matters more than it looks, because REPLAYED is the only first-hand evidence the
-- resolution contract has. The producer's own dispatcher witnesses the consumer accepting
-- the event; every other route to resolution rests on something the consumer reports about
-- itself. An old incident was therefore unresolvable through the one path that does not
-- require trusting the party being checked -- and with the waiver deferred out of P0, an
-- unresolvable authority-bearing incident blocks a live consumer permanently.
--
-- The two columns make the row self-sufficient. After this, outbox partition retention
-- stops being load-bearing for resolution, which is the property that lets a waiver stay
-- out of scope rather than becoming the only exit.
--
-- # Why nullable, and what NULL means
--
-- Deliberately nullable, and never defaulted. A guessed aggregate_id is worse than an
-- absent one: it names a real aggregate somewhere, and a replay dispatched under it
-- delivers a security event attributed to the wrong subject. NULL says "this row cannot
-- replay itself, an operator must reconstruct it", which is a true statement an alert can
-- act on.
--
-- After this migration NULL occurs only for rows written before it whose outbox partition
-- had already been dropped, because the backfill below recovers every row whose original is
-- still present, and the dispatcher populates both columns from then on.
--
-- Deriving aggregate_id from the payload instead was available and rejected: which field
-- names the aggregate is domain knowledge, and this schema does not know what a Membership
-- is. Reading it here would invert the dependency the platform boundary exists to hold.

ALTER TABLE platform.dead_letter
    ADD COLUMN IF NOT EXISTS aggregate_id UUID,
    ADD COLUMN IF NOT EXISTS priority     SMALLINT;

-- Recovers what is still recoverable. Joined on event_id alone rather than on the outbox's
-- (created_at, event_id) key: the created_at is not carried here, and event_id is unique
-- per event by construction -- the outbox's composite key exists for partitioning, not
-- because one event can appear twice.
--
-- Rows whose partition is already gone match nothing and stay NULL, which is the honest
-- outcome rather than a failure: the data to recover them does not exist any more.
UPDATE platform.dead_letter d
   SET aggregate_id = o.aggregate_id,
       priority     = o.priority
  FROM platform.outbox o
 WHERE o.event_id = d.event_id
   AND d.aggregate_id IS NULL;
