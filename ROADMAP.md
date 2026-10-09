# Foundation Platform — Roadmap

Execution tracker for this repository only. Architecture lives in
`scnehaux-architecture`; nothing here overrides a SAD, an ADR, or a standard.

Week numbers are relative to the first build week, not calendar dates.

## Current position

| Package | State | Evidence |
| :-- | :-- | :-- |
| `id` | **done** | UUIDv7 per RFC 9562, monotonic counter in `rand_a`, nil identifiers rejected |
| `event` | **done** | CloudEvents 1.0 envelope and validated type; 90.8% coverage |
| `tools/archcheck` | **done** | 18 tests over fixtures, including that each rule rejects a known violation, and that an external test package is checked like any other |
| `.github/workflows/ci.yml` | **done** | Race detector, PostgreSQL service, coverage floor, boundaries, schema compatibility, tidy, scheduled `govulncheck` on a floating toolchain patch; third-party actions pinned by commit |
| `db` | **done** | `InTx` and session binding semantics unit-tested; typed-nil transaction handles rejected safely |
| `db/dbtest` | **done** | Records exec, query-row, and result-set behavior without leaking the driver above `db/` |
| `migrations` | **done** | Embedded schema plus UTC daily partition creation, default-row relocation, and published-only retention drop |
| `outbox` | **done** | Append, two-lane dispatcher, retry/dead letter, persisted-error redaction, retention helpers, and 10,000-row priority proof |
| `inbox` | **done** | Transactional composite-key guard over `(event_id, consumer)`; 100% unit coverage |
| `idempotency` | **done** | Caller-scoped claim, digest conflict, in-progress state, completion, and stored-response replay |
| `httpapi` | **done** | Fixed middleware order, correlation, shedding, timeout propagation, recovery, server defaults, and a closed RFC 9457 registry of fourteen types |
| `observability` | **done** | OpenTelemetry spans/metrics, redacted structured logging, broker propagation, and explicit producer-consumer links |
| `redact` | **done** | Shared credential redaction for text and structured `slog` attributes |
| `contracts/events` | **done** | Temporary registry and compatibility gate; event definitions remain owned by publishing systems |
| `verify` | **done** | Local token verification: JWKS caching with rate-limited refetch, `PS256` only, exact issuer, audience, bounded skew, and a mandatory consumer claim rule; 90.2% coverage. STD-IAM-002 §3.5 step 8, the current-state check, is the resource's, after `Verify`: it names `tenant_id` and reads the resource's own records, neither of which this module may do (package doc) |
| `e2e` | **done** | Tests only. The Week 2 correlation chain, HTTP request to consumer span, against a throwaway database |

`arch.json` already declares the internal edges for every package above, so an
accidental coupling introduced while writing them fails the build rather than
accumulating.

Coverage is measured against an 80% floor and excludes `tools/`. The unit-only run is
83.9%, so a missing PostgreSQL service can no longer hide behind integration coverage.
CI additionally runs every PostgreSQL behavior test with `REQUIRE_INTEGRATION=1`.

That is a property of the code rather than a gap in it: claim ordering, `SKIP LOCKED`
disjointness, backoff scheduling, and dead-letter routing are all statements about what
PostgreSQL does, and a fake asserting them would only be asserting itself.

**The database-specific claims have a gate.** CI runs a `postgres:17-alpine`
service container, applies the shipped schema, and asserts what only a database can
answer: that the DDL parses, that the driver encodes an identifier as `uuid` and a
`[]byte` as `jsonb`, that the column defaults leave a row unpublished with zero attempts,
that sequence advances across partitions, that the default partition is drained into a
daily partition, that retention cannot drop an unpublished row, and that a failure
injected after the append leaves nothing behind.

`REQUIRE_INTEGRATION=1` is set in CI so a service container that never came up fails the
build. Without it a skipped suite and a passing suite are the same colour, and the skip is
the more likely of the two to go unnoticed.

This module still owns no database, and that is the design rather than a gap. It is a
library with no deployable, EAD-003 forbids cross-domain persistence, and the `platform`
schema exists once inside each consuming database. What CI runs is a throwaway server for
the duration of a job, which is a test fixture and not a dependency.

`db.Open`, `db.Ping`, and `db.Close` are the uncovered remainder, and they are the reason
`db` reports the lowest figure of any shipped package. They do execute in CI — the
integration suite opens a real pool through `db.Open` — but coverage is attributed to the
package whose own tests ran, so exercising them from `outbox` moves nothing. The
transaction semantics that matter are exercised against a fake connection source.

Adding an integration test inside `db` would raise the number. It is not worth writing one
solely for that: the figure would improve and nothing would become more certain, which is
the failure mode a coverage floor invites.

The first CI run rejected the push: `govulncheck` traced GO-2026-5970 from `db.Open`
through `pgxpool.NewWithConfig` into `norm.Form.Properties`, an infinite loop on invalid
input in `golang.org/x/text`. pgx normalises credentials with SASLprep during SCRAM
authentication, so the path was live rather than merely present in the module graph.
Raised to v0.39.0.

Worth recording because the finding required no commit on our part and the same is true
of the next one. CI now runs the supply-chain job weekly in addition to push and pull
request, so new advisories do not wait for an unrelated code change.

The next one arrived at the release gate. `govulncheck` reported GO-2026-6090,
GO-2026-6088, and GO-2026-5972 — reachable paths into `crypto/tls`, `encoding/xml`, and
`encoding/asn1`, all three fixed in go1.26.6. Nothing in this module could clear them,
because the vulnerable code is the standard library the toolchain shipped with.

`GO_VERSION` asks for `1.26`, so the patch was never chosen: the runner had go1.26.5
cached and `check-latest` was unset. Pinning `1.26.6` would have moved the problem to the
next advisory and made clearing it depend on someone remembering to raise a number.

The supply-chain job now sets `check-latest: true` and `verify` keeps `check-latest:
false`. Test results have to be reproducible and a vulnerability scan has to be current,
which are opposite requirements, so the two jobs disagree deliberately. An advisory whose
fix is a patch release now resolves without a commit, and what stays red is what needs a
decision — a vulnerability in a dependency, or one with no fix yet.

The durable form of that reasoning, including what the `go` directive does and does not
promise and how to raise `GO_VERSION` when a minor bump becomes necessary, is in
[README.md](README.md#go-versions). What is recorded here is only that it happened.

## Environment findings

Recorded because each one changes how a step is verified, and a future engineer hitting
the same wall should not have to rediscover it.

| Finding | Consequence |
| :-- | :-- |
| `proxy.golang.org` fails TLS verification on this network; `sum.golang.org` and `github.com` do not | Build with `GOPROXY=direct`, which fetches from VCS and still verifies checksums against the working sum database |
| Go 1.26.5 installed at `D:\Go1.26.5`; `D:\Go\bin` remains first on the machine PATH and resolves to 1.24.2 | Prepend `D:\Go1.26.5\bin` per session, or replace the machine PATH entry with an elevated shell |
| `winget` is disabled by Group Policy | Toolchains are installed by extracting an archive, not by a package manager |
| The C toolchain at `C:\MinGW` is MinGW.org GCC 6.3.0, which cannot emit 64-bit code | Resolved. MinGW-w64 GCC 16.2.0 extracted to `D:\mingw64`; prepend `D:\mingw64\bin` to PATH ahead of `C:\MinGW\bin` |
| Atlas and Docker are absent on this workstation | The integration suite skips locally and runs in CI against a service container. Migrations are applied there, so the schema is exercised on every push rather than only when someone remembers |
| Atlas is now present locally at `D:\Atlas\atlas.exe`, and PostgreSQL 15.5 is installed | The migration pipeline can be run end to end locally. It found the migration-idempotency defect below, which CI could not have found |

### The platform migration set was not re-runnable

This package ships migrations as embedded SQL and **no revision table**, so a consumer's
migration command applies the whole set on every invocation. `identity-migrate` says so in
its package comment and relies on every statement being idempotent.

`0002_idempotency_scope_and_maintenance.sql` used bare `ADD COLUMN` and `ADD CONSTRAINT`.
The first deployment therefore succeeded and every deployment after it aborted with
`column "scope" of relation "idempotency_key" already exists`.

Every other integration suite in this repository drops and rebuilds the `platform` schema
before it runs, which is correct for isolation and is precisely why no test could see this:
the set was only ever applied to an empty schema. Found by running `identity-control`'s
database pipeline a second time.

Fixed in `0002` with `IF NOT EXISTS` and `pg_constraint` guards — PostgreSQL has no
`ADD CONSTRAINT IF NOT EXISTS` — and `migrations/integration_test.go` now applies the set
three times and asserts the resulting schema. Three rather than two, because a primary key
dropped and re-added on each run looks correct on the second application and fails on the
third. The guard was verified to fire by restoring the original statement.

That suite rolls its whole transaction back, including the schema drop. `go test ./...` runs
package binaries concurrently against one `TEST_DATABASE_URL`, so a suite that committed
would delete the schema the `outbox` suite is mid-way through using.

The race detector now runs locally. It required a matching C compiler, because the
detector is implemented in C and reached through cgo, and the toolchain that shipped
with this workstation was a 32-bit build from 2016 against a `windows/amd64` Go.

That the suite passes under `-race` was verified to mean something: a deliberate
unsynchronised increment across eight goroutines was compiled in a throwaway module and
reported, with the conflicting addresses and the line that wrote them. A detector that
reports nothing because it is not armed is indistinguishable from correct code, and the
only way to tell them apart is to make it fire.

`go test ./... -race` remains a CI gate regardless. Local availability shortened the loop
while the dispatcher was written, whose workers contend by design, but the gate that
decides whether a change lands is the one in CI.

## Position in the build order

**This library lands first.** `identity-control` and `organization-control` import it
in their first commit, and neither can write a domain mutation without the transaction
manager and the outbox append that make propagation atomic.

Nothing in this repository waits on the Keycloak proof-of-concept. It touches Keycloak
nowhere.

## Design status

| TDD | Subject | Status |
| :-- | :-- | :-- |
| `TDD-foundation-platform-001` | Outbox, dispatcher, and event envelope | approved |
| `TDD-foundation-platform-002` | HTTP substrate, persistence, and telemetry | approved |

Both designs are complete. The second resolves the one tension the first left open:
`db` must support the `SET LOCAL` binding that Row-Level Security requires, in a
library forbidden from naming a tenant. It does so through a `SessionBinder` supplied
at the composition root, so this library provides the call site and
`organization-control` provides the statement.

## Week 1 · Propagation substrate

- ✅ `id` — UUIDv7 with a monotonic counter, the identifier every table references
- ✅ CloudEvents 1.0 envelope construction and the type naming rule
- ✅ CI gate and `archcheck`, landed before the code they constrain
- ✅ `db.Tx`, `db.Pool`, and the `SessionBinder` the transaction manager invokes
- ✅ `platform.outbox` with partitioning and the global sequence
- ✅ `platform.processed_event`, `platform.dead_letter`, `platform.idempotency_key`
- ✅ `outbox.Append(ctx, tx, aggregateID, envelope, opt…)` — takes a transaction handle,
  so publication outside a domain transaction fails to compile
- ✅ Dispatcher: `FOR UPDATE SKIP LOCKED`, reserved priority lane, the row lock as lease
- ✅ Three local retries with exponential backoff and equal jitter, then dead-letter
- ✅ Empty-poll backoff, so an idle dispatcher stops waking the database on a timer
- ✅ `inbox.Guard` deduplication, transactional with the effect

**Exit:** a domain mutation and its outbox append commit atomically, proven by injecting
a failure between them; a lifecycle backlog of ten thousand rows does not delay a
priority event beyond budget; duplicate delivery produces one effect.

All three library guarantees are implemented. Atomicity is asserted against PostgreSQL,
the 10,000-row backlog test claims the priority event first within budget, and
`inbox.Guard` uses one `INSERT ... ON CONFLICT` in the caller's effect transaction.
Consuming systems still own their end-to-end test that the domain effect and guard commit
together, because the effect itself deliberately does not exist in this repository.

## Week 2 · Technical substrate

- ✅ Pool construction and the transaction manager
- ✅ RFC 7807 problem serialization and the error taxonomy
- ✅ Middleware and server substrate: correlation, logging, recovery, timeout, shedding
- ✅ OpenTelemetry tracing, metrics, and redacted structured logging
- ✅ Correlation propagation and producer-consumer span links across the broker boundary

**Exit:** a correlation identifier survives from an inbound HTTP request through a
domain transaction, into an outbox row, across the broker, and into the consumer's
span.

**Met.** `e2e/correlation_test.go` drives every hop in one process against a throwaway
PostgreSQL database: a request through `httpapi.Chain`, a domain row and `outbox.Append` in
one transaction, the dispatcher publishing through `outbox/httpdelivery`, and a consumer
that restores `observability.Metadata` from the payload, opens its span with
`StartConsumer`, and applies behind `inbox.Guard`. It asserts the client's value on the
response, the domain row, the outbox envelope, and the consumer span's `correlation_id`.
Forcing the middleware to mint a fresh identifier, or dropping the identifier in
`ContextWithMetadata`, turns it red.

The identifier crosses the broker in the payload, where every consumer reads it. The test also
asserts the delivery's `X-Correlation-Id` header.

- ✅ **A dispatched delivery carries `X-Correlation-Id`** (TDD-001 2.4.0 §HTTP Delivery). Writing
  the test showed that it never did. The dispatcher publishes on its own context, which carries no
  correlation, and `httpdelivery` read the context alone. It now falls back to the envelope's
  `data.correlation_id`. The context still wins when it carries one, and a malformed value sends
  no header and does not fail the delivery. The change is additive, with no API change.
  **Unreleased:** consumers get it with the next patch tag, which needs the owner's approval.

## Week 3 · Hardening and release

- ✅ Backoff behaviour on empty polls
- ✅ Two-replica dispatcher contention — two dispatchers claim disjoint halves of a batch
  while both hold their claims open, so the assertion is about `SKIP LOCKED` rather than
  about one finishing before the other starts
- ✅ Partition creation API, default-row relocation, and published-only retention drop
- ✅ A lifecycle backlog of ten thousand rows does not delay a priority event beyond budget
- ✅ Ordering guarantee across partition boundaries
- ✅ Tag `v0.1.0`, annotated at `dac9e9d` and pushed
- ✅ `identity-control` and `organization-control` both build against a tag (table below)

**Exit:** both consuming repositories build against a tagged version rather than a
branch.

**Met.** Every consumer builds against a tag, with no `replace` directive. Read from each
`go.mod` on `main` on 2026-10-09:

| Consumer | Pins |
| :-- | :-- |
| `organization-control` | `v0.4.1` |
| `foundation-reference` | `v0.3.1` |
| `identity-control` | `v0.4.0` |

Two tags followed `v0.1.0`. `v0.2.0` added `verify`; `v0.2.1` made the platform migration set
re-runnable, which is the defect recorded under Environment findings above — the first
deployment succeeded and every one after it aborted, and only a consumer running the pipeline
twice could have found it.

## Week 4 · Delivery evidence and dead-letter correctness

Built for `organization-control`'s dead-letter resolution (`TDD-organization-control-005`),
which closed on 2026-09-24. `TDD-001` is the current statement of each item.

| Tag | Adds |
| :-- | :-- |
| `v0.2.3` | `dead_lettered_at` names the transition (`statement_timestamp()`); a dead letter retains `aggregate_id` and `priority`, so it can replay itself |
| `v0.2.4` | `platform.delivery_receipt`, with typed evidence: `consumer_applied` only from the consumer's marker, else `transport_accepted` |
| `v0.2.5` | Dispatcher preflight: the database contract is verified before any worker starts |
| `v0.2.6` | Preflight requires `SELECT` beside `INSERT` on `delivery_receipt`, because `ON CONFLICT` needs it |
| `v0.2.7` | The dead-letter resolution record: type, actor, and reference, all or nothing |
| `v0.2.8` | Delivery-receipt retention: `PruneDeliveryReceipts` and `ReceiptReference`. A dead letter names the consumer that refused it |
| `v0.2.9` | Dead-letter waivers (`0007`): who, why and until when, kept apart from the closure. A waiver silences the stale alert until it expires, permits payload disposal, and does not hold receipt pruning |
| `v0.2.13` | `verify.Config.RequireAccessTokenType`: the header `typ` must be `at+jwt`, so an ID token cannot pass as an access token (RFC 9068 §4, STD-IAM-002 §3.5 step 5). `Claims.TokenType()` reports each token's `typ` for the rollout. `ErrTokenType` |
| `v0.2.12` | No row is dead-lettered for unavailability, in either lane. A standard row used to be dead-lettered at its third attempt, and the system proof's outage phase measured the cost: a two-second consumer restart dead-lettered every grant queued behind it, which organization-control counts as security debt. Dead-lettering is now for poison alone. `organization-control` backlog item 15 |
| `v0.2.11` | `observability.Export`: OTLP/HTTP metric and trace exporters for the Collector STD-GLB-003 requires. `New` started none, and no deployable did, so every recorded metric went to the no-op providers. `organization-control` backlog item 14 |
| `v0.2.10` | The outbox lease (`0008`): a short transaction leases a batch, publication happens outside any transaction, and each outcome commits alone, fenced on the lease. Preflight requires the lease columns. `LeaseDuration`, default 30 s, is also how long a crashed dispatcher's rows wait |

Open, owned here:

- ✅ **`platform.delivery_receipt` retention**, `v0.2.8`. `PruneDeliveryReceipts` deletes a receipt
  past the boundary only while no incident is open and only when no closure cites it;
  `ReceiptReference` is the citation form it protects (TDD-001 §Delivery Receipt). The host
  runs it: `organization-control`'s backlog item 7.
- ✅ **`first_failed_at` measured from the failure**, `v0.2.10`. It still takes `now()`, but
  `now()` is now the start of the outcome's own transaction, which begins after the publication
  returned, rather than the start of the claim. Recorded P1 in RESPONSE-16 and RESPONSE-17.
- ✅ **The dispatcher no longer holds a transaction across each delivery**, `v0.2.10`.
  - A short transaction leases the batch.
  - Publication happens outside any transaction.
  - Each outcome commits in its own transaction, fenced on the lease.
  - A crash therefore loses at most the outcome being written.
  - The price: a crashed dispatcher's rows wait up to `LeaseDuration` rather than being released
    at once.

  TDD-001 §Dispatch covers the full design. Recorded P1 in RESPONSE-15, RESPONSE-16 and
  RESPONSE-17. `organization-control` backlog item 11.

## Week 5 · Per-consumer delivery

Built for `ADR-GLB-018`, so `organization-control` can deliver its authority to more than one
consumer: `foundation-reference`, and the Identity Control Service for provider grants and
activations (`ADR-ORG-002 §5.3`). `TDD-001 §Per-Consumer Delivery` is the current statement.

| Tag | Adds |
| :-- | :-- |
| `v0.3.0` | `platform.subscription` and `platform.outbox_delivery` (`0009`). `Append` writes one delivery per subscriber in the event's own transaction, under a shared advisory lock that `Subscribe` takes exclusive. The dispatcher claims `Config.Consumer`'s deliveries alone. A dead letter is keyed `(event_id, consumer)`. `To(consumer)` owes a replay to one consumer and returns `ErrNotSubscribed` when it would owe nobody. **Breaking**: the outbox loses its publication columns, `0001` and `0008` are guarded so the set stays re-runnable, and the dispatch role needs `SELECT, UPDATE` on `platform.outbox_delivery` and only `SELECT` on `platform.outbox`. Drain the outbox before applying `0009`: an unpublished event has no delivery afterwards |

| `v0.3.1` | `Abandon(tx, consumer, reason)` closes a retired consumer's owed deliveries as `abandoned`, so they stop holding retention (`ADR-GLB-018 §5.5`). `Append` reads nothing back from the outbox, so a publishing role needs no `SELECT` on it: `INSERT` on `platform.outbox` and `platform.outbox_delivery`, and `SELECT (consumer, event_types, retired_at)` on `platform.subscription` (§5.6) |

| `v0.4.0` | `clientauth`: the client credentials grant with a `private_key_jwt` assertion (RFC 6749 §4.4, RFC 7523), a cached token, and `Invalidate` for a 401. `outbox/httpdelivery`: the Direct Durable Delivery publisher, moved from foundation-reference, now taking a `TokenSource`, so a producer's dispatcher authenticates as its workload (ADR-GLB-018 §5.4, STD-IAM-001 §3). A 401 drops the cached token and retries. `StaticToken` is kept for a local proof |

| `v0.4.1` | `httpapi.PayloadTooLarge`, `https://problems.scnehaux.com/payload-too-large`, 413: a well-formed request over a declared limit, which RFC 7644 §3.7.4 requires for a bulk request and which `organization-control`'s Membership batch was answering as `validation-failed` (TDD-002 §Problem Type Registry). `0010` indexes `platform.outbox_delivery (event_id)`, so one event's deliveries are read without scanning every partition (TDD-001 §Per-Consumer Delivery). No grant changes. The index build blocks writes to `platform.outbox_delivery` while it runs |

Consequences for consumers, owned by them:

- `organization-control` subscribes each named consumer, drops `consumer_single_active`, and reads
  frontier, debt, replay and closure per consumer from `platform.outbox_delivery`.
- A host that owns a dispatcher runs one per consumer, each with its own endpoint and credential.

## Decisions this repository does not make

| Decision | Owner |
| :-- | :-- |
| Broker product | ADR-GLB-003 §5 and STD-GLB-004 §3 — settled, see below |
| Token lifetime classes | STD-IAM-002 |
| Which events exist and what they mean | The publishing system's designs |
| Enforcement budget targets | `TDD-organization-control-002` |

**The broker is settled and it is the Kafka protocol.** It was the one open item that
touched this code. Nothing in this module changes as a result: the dispatcher publishes
through the `Publisher` interface, the adapter satisfying it belongs to each consuming
system, and no Kafka client appears in `go.mod`.

Two consequences are worth recording because they constrain the adapter rather than this
module, and an adapter author reading only this repository would not otherwise see them:

- The reserved priority lane is a **separate topic** with its own consumer group and
  partition allocation. `priority = 0` selects that topic; it is not a priority field
  inside a shared one.
- Producers partition by `aggregate_id`. Kafka preserves order only within a partition,
  and `sequence` is publisher-global, so per-aggregate ordering is what the broker
  actually provides. The design already tolerates cross-aggregate reordering, so this
  costs nothing here — but an adapter partitioning on any other key silently removes the
  per-aggregate guarantee while every test in this repository keeps passing.

`contracts/events` and `tools/schemacheck` stay as the interim registry. STD-GLB-004 now
names a Kafka-ecosystem schema registry as the target and permits source-controlled
schemas while the compatibility check runs in CI. That interim state is debt, and it is
recorded here rather than left to be rediscovered.

## Not this library

Recorded so scope creep is visible rather than convenient:

- No domain type, no domain constant, no domain field name.
- No authorization decision.
- No direct Keycloak client.
- No shared state between the two consuming systems.

A pull request adding any of these is rejected on principle, not on review preference.

## Gates

✅ **Design gate.** Both designs are approved, `TDD-001` at `2.4.0` and `TDD-002` at
`1.4.0`, and the broker adapter interface, `outbox.Publisher`, is fixed in TDD-001 §Go
Surface.

✅ **Release gate.** The design gate, plus: ✅ partition lifecycle exercised end to end,
✅ dead-letter and substrate runbooks written, ✅ dispatcher contention proven under two
replicas, ✅ `v0.1.0` tagged and pushed with CI green, and ✅ a tagged version consumed by
both control repositories.

Every clause is met. Both control repositories pin a tag; the Week 3 table records which.

## Departures from the designs, recorded

Kept here as an index. Each is argued where it applies, in the design itself, so a reader
of the design never has to know this file exists.

| Departure | Where |
| :-- | :-- |
| `Append` takes `aggregateID` as a parameter, not an `Option` | TDD-001 §Go Surface |
| The handle is typed `db.Tx`, not `pgx.Tx` | TDD-001 §Go Surface |
| A `DEFAULT` partition exists, so an append never fails for want of one | TDD-001 §Data Model |
| Three columns added: `next_attempt_at`, `first_failed_at`, `failure_class` | TDD-001 §Data Model |
| A released priority row keeps its attempt count rather than resetting it | TDD-001 §Dispatch |
| `event_id` is not enforced unique in the outbox | TDD-001 §Data Model, already recorded in the design |
| `inbox.Guard` takes `event.Type` because `processed_event.event_type` is mandatory | TDD-001 §Go Surface |
| Idempotency is keyed by authenticated caller scope as required by TDD-002 middleware order | TDD-001 §Idempotency |

The last one predates implementation. The rest were found by writing the code, which is
the usual way a design's internal contradictions surface.

### `request-in-progress` added to the problem registry

`identity-control` was answering an in-flight retry with `state-transition-refused`, because the
registry offered nothing closer. Recorded there as a finding, and the finding stayed in the
consumer: nothing here said the registry was short a type, so whoever worked on this module next
would not have known.

Both types answer 409 and they carry opposite advice. A refused transition means the caller asked
for something the record cannot do, and no retry will change that. An in-progress request means the
caller asked for something that is happening, and retrying after a moment is the correct response.
A client that cannot tell them apart gives up when it should wait.

The registry is closed and compiled precisely so a handler cannot invent a type, which is the right
constraint and is also why the gap had to be closed here rather than worked around downstream.
`TestEveryProblemTypeIsRegistered` now walks the constant range against the map, so a constant
added without an entry fails rather than reaching a caller as an empty document with a zero status.

### `payload-too-large` added to the problem registry

`organization-control` was answering a Membership batch over its 500-item limit with
`validation-failed`, a 400, because the registry offered nothing closer. RFC 7644 §3.7.4 is not
optional about it: "If either limit is exceeded, the service provider MUST return HTTP response
code 413 (Payload Too Large)."

The two statuses carry different advice. A 400 says the request is wrong and sending it again in
any shape will not help; a 413 says it is well formed and too large, and splitting it will. The
type is declared in status order, between `precondition-unmet` and `rate-limited`, so the numeric
values of the constants after it moved. Nothing serializes those values: a document carries the
URI, and consumers refer to the constants by name.
