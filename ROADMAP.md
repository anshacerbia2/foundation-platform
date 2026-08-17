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
| `httpapi` | **done** | Fixed middleware order, correlation, shedding, timeout propagation, recovery, server defaults, and RFC 7807 registry |
| `observability` | **done** | OpenTelemetry spans/metrics, redacted structured logging, broker propagation, and explicit producer-consumer links |
| `redact` | **done** | Shared credential redaction for text and structured `slog` attributes |
| `contracts/events` | **done** | Temporary registry and compatibility gate; event definitions remain owned by publishing systems |

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

## Week 3 · Hardening and release

- ✅ Backoff behaviour on empty polls
- ✅ Two-replica dispatcher contention — two dispatchers claim disjoint halves of a batch
  while both hold their claims open, so the assertion is about `SKIP LOCKED` rather than
  about one finishing before the other starts
- ✅ Partition creation API, default-row relocation, and published-only retention drop
- ✅ A lifecycle backlog of ten thousand rows does not delay a priority event beyond budget
- ✅ Ordering guarantee across partition boundaries
- ✅ Tag `v0.1.0`, annotated at `dac9e9d` and pushed
- Pin both consumers to it

**Exit:** both consuming repositories build against a tagged version rather than a
branch.

`v0.1.0` exists and CI is green on `main`. The remaining half of this step does not
belong to this repository: `identity-control` and `organization-control` hold designs and
no Go module yet, so there is nothing to pin the tag into. This exit criterion closes when
they take their first commit, not when anything further lands here.

## Decisions this repository does not make

| Decision | Owner |
| :-- | :-- |
| Broker product | `identity-control` and `organization-control` SADs |
| Token lifetime classes | STD-IAM-002 |
| Which events exist and what they mean | The publishing system's designs |
| Enforcement budget targets | `TDD-organization-control-002` |

The broker choice is the one open item that touches this code. The reserved priority
lane is the deciding requirement: it must be expressible as a separate topic or stream
with its own consumer group and its own capacity, without head-of-line blocking behind
lifecycle traffic. The dispatcher is written against an interface so the choice lands
in one adapter.

## Not this library

Recorded so scope creep is visible rather than convenient:

- No domain type, no domain constant, no domain field name.
- No authorization decision.
- No direct Keycloak client.
- No shared state between the two consuming systems.

A pull request adding any of these is rejected on principle, not on review preference.

## Gates

**Design gate.** Both designs at `1.0.0`, with the broker adapter interface fixed.

**Release gate.** The design gate, plus: ✅ partition lifecycle exercised end to end,
✅ dead-letter and substrate runbooks written, ✅ dispatcher contention proven under two
replicas, ✅ `v0.1.0` tagged and pushed with CI green, and a tagged version consumed by
both control repositories.

Consumer pinning is the one clause still open, and it is held by the consuming
repositories rather than by this one. Everything this library owes the gate is done.

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
