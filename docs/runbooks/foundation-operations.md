# Foundation Platform Operations

These procedures run in each consuming system independently. Never query one control
system's `platform` schema from the other.

## Dead-letter triage and replay

1. Page immediately for any priority row and use the normal lifecycle threshold for
   standard rows
2. Read only incident fields first: `event_id`, `event_type`, `failure_class`, attempts,
   and timestamps. Do not paste `payload` or `envelope` into tickets or chat
3. Correct the schema, adapter, or downstream condition that made the event poison
4. Replay through the owning system's broker adapter with the original `event_id`
5. Confirm the consumer recorded `(event_id, consumer)` and the intended effect exists
6. Set `resolved_at`; never delete the incident row manually

## Dispatcher stall

1. Compare oldest unpublished age by lane with worker health and database acquisition
   errors
2. Verify workers can claim with `FOR UPDATE SKIP LOCKED` and are not blocked by a long
   broker publish
3. Restart only after capturing the blocked query and worker stack
4. Confirm priority age falls first and no row was closed without `published_at` or a
   dead-letter incident

## Broker outage and drain

1. Keep priority dispatch enabled; unavailable priority rows must remain unpublished
2. Reduce standard workers if retries threaten database capacity
3. Restore the broker adapter and watch oldest priority age, then total backlog
4. Scale workers within broker and database limits; do not bypass the outbox
5. Confirm attempts stop rising and published throughput exceeds incoming throughput

## Partition maintenance failure

1. Alert when fewer than two UTC daily partitions exist ahead
2. Run `migrations.EnsureOutboxPartitions` with the migration role, never the runtime role
3. If the default partition is non-empty, allow the function to move matching rows while
   it holds the required lock; schedule this away from peak mutation traffic
4. Run `migrations.DropPublishedOutboxPartitions` only with the approved retention bound
5. Investigate every retained old partition; an unpublished row is intentionally a hard
   stop on dropping it

## Duplicate-effect investigation

1. Confirm the handler called `inbox.Guard` in the same transaction as its effect
2. Check the composite key includes both `event_id` and the logical consumer name
3. Compare effect commit time with broker acknowledgement and process restarts
4. Repair the effect through the owning domain; do not edit `processed_event` to hide it

## Pool exhaustion

1. Attribute the alert by pool name, deployable, and system
2. Inspect transaction age, blocked statements, and binder failures
3. Cancel leaked request contexts and stop the source of unbounded concurrency
4. Raise pool size only after proving database headroom; a larger wait queue is not a fix

## Sustained load shedding

1. Confirm shed responses are `503` with `Retry-After` and occur before authentication
2. Compare in-flight work, handler latency, pool acquisition, and downstream latency
3. Reduce admitted traffic or remove the slow dependency before increasing the ceiling
4. Verify shed rate returns to baseline and authentication capacity was preserved

## Telemetry export failure

1. Keep request processing available; exporter failure must not become an API outage
2. Check endpoint reachability, certificate validity, queue depth, and dropped signal count
3. Rotate exporter credentials through the secret manager without logging their values
4. Confirm new spans, metrics, and logs carry `deployable`, `system`, and correlation data
