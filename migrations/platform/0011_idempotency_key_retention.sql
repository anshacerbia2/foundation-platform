-- Completed idempotency keys expire (STD-GLB-001 1.7.0, TDD-foundation-platform-001 §Idempotency).
--
-- Until this file nothing removed a key, so every response a command recorded was kept for ever.
-- The IETF draft asks for a published expiry: "The resource MAY require time based idempotency keys
-- to be able to purge or delete a key upon its expiry. The resource SHOULD define such expiration
-- policy and publish it in the documentation" (draft-ietf-httpapi-idempotency-key-header-07 §2.3).
-- Each consuming service publishes its window and calls this function from its maintenance stage.
--
-- Every statement here is re-runnable, because the set is applied whole on every deployment.

-- The purge reads completed keys by age. A partial index, because only completed keys are ever
-- purged and an uncompleted key is never read by this predicate: "A partial index is an index built
-- over a subset of a table; the subset is defined by a conditional expression" (PostgreSQL 17
-- §11.8).
CREATE INDEX IF NOT EXISTS idempotency_key_completed
    ON platform.idempotency_key (completed_at)
    WHERE completed_at IS NOT NULL;

-- purge_idempotency_keys deletes completed keys older than retain_for and returns how many.
--
-- The window is an interval, not a timestamp, so the boundary is computed by the database clock
-- that wrote completed_at. A boundary computed by the caller's clock would make the floor below
-- depend on the skew between two machines.
--
-- now() is the transaction's start, so the boundary is never later than the statement's own time:
-- a long maintenance transaction keeps slightly more, never less.
--
-- The floor is 24 hours. A key purged while a client still retries is a new request to the next
-- retry, which executes the command again. Stripe states the same consequence of its own pruning:
-- "you can remove keys from the system automatically after they're at least 24 hours old. We
-- generate a new request if a key is reused after the original is pruned" (Stripe, Idempotent
-- requests). A service publishes a window at least as long as any of its clients retries.
--
-- An uncompleted key is never purged. It is a claim whose effect may have committed without its
-- response; removing it would let a retry execute the command twice.
CREATE OR REPLACE FUNCTION platform.purge_idempotency_keys(retain_for INTERVAL)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    purged BIGINT;
BEGIN
    IF retain_for IS NULL THEN
        RAISE EXCEPTION 'idempotency key retention is required';
    END IF;
    IF retain_for < INTERVAL '24 hours' THEN
        RAISE EXCEPTION 'idempotency key retention % is shorter than 24 hours', retain_for;
    END IF;

    DELETE FROM platform.idempotency_key
    WHERE completed_at IS NOT NULL
      AND completed_at <= now() - retain_for;
    GET DIAGNOSTICS purged = ROW_COUNT;
    RETURN purged;
END;
$$;
