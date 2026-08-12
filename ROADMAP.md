# Foundation Platform — Roadmap

Execution tracker for this repository only. Architecture lives in
`scnehaux-architecture`; nothing here overrides a SAD, an ADR, or a standard.

Week numbers are relative to the first build week, not calendar dates.

## Current position

| Package | State | Evidence |
| :-- | :-- | :-- |
| `id` | **done** | UUIDv7 per RFC 9562, monotonic counter in `rand_a`; 96.8% coverage |
| `event` | **done** | CloudEvents 1.0 envelope and validated type; 90.8% coverage |
| `tools/archcheck` | **done** | 16 tests over fixtures, including that each rule rejects a known violation |
| `.github/workflows/ci.yml` | **done** | gofmt, vet, build, race, coverage floor, archcheck, tidy, govulncheck |
| `db` | **done** | `InTx` semantics unit-tested with no database; 75.7% coverage |
| `outbox` | next | `db.Tx` now exists, so `Append` can require the handle |
| `inbox` | not started | — |
| `idempotency` | not started | — |
| `httpapi` | not started | No database dependency; can proceed in parallel with `db` |
| `observability` | not started | No database dependency; can proceed in parallel with `db` |
| `contracts/events` | not started | Holds schemas until the enterprise registry exists |
| `migrations/platform` | not started | Atlas; needs Atlas installed |

`arch.json` already declares the internal edges for every package above, so an
accidental coupling introduced while writing them fails the build rather than
accumulating.

Shipped-package coverage is **88.0%** against an 80% floor. Coverage excludes `tools/`,
which is verified by its own tests rather than by a percentage; including a large tool
would let it depress a figure meant to describe the library.

`db.Open`, `db.Ping`, and `db.Close` are the uncovered remainder. They need a running
PostgreSQL and are covered by integration tests once Docker exists, which is why `db`
sits lower than the other two rather than because its logic is untested — the
transaction semantics that matter are exercised against a fake connection source.

## Environment findings

Recorded because each one changes how a step is verified, and a future engineer hitting
the same wall should not have to rediscover it.

| Finding | Consequence |
| :-- | :-- |
| `proxy.golang.org` fails TLS verification on this network; `sum.golang.org` and `github.com` do not | Build with `GOPROXY=direct`, which fetches from VCS and still verifies checksums against the working sum database |
| Go 1.26.5 installed at `D:\Go1.26.5`; `D:\Go\bin` remains first on the machine PATH and resolves to 1.24.2 | Prepend `D:\Go1.26.5\bin` per session, or replace the machine PATH entry with an elevated shell |
| `winget` is disabled by Group Policy | Toolchains are installed by extracting an archive, not by a package manager |
| The C toolchain at `C:\MinGW` is MinGW.org GCC 6.3.0, which cannot emit 64-bit code | Resolved. MinGW-w64 GCC 16.2.0 extracted to `D:\mingw64`; prepend `D:\mingw64\bin` to PATH ahead of `C:\MinGW\bin` |
| Atlas and Docker are absent | Migrations can be authored but not applied; integration tests are CI-only until both exist |

The race detector now runs locally. It required a matching C compiler, because the
detector is implemented in C and reached through cgo, and the toolchain that shipped
with this workstation was a 32-bit build from 2016 against a `windows/amd64` Go.

That the suite passes under `-race` was verified to mean something: a deliberate
unsynchronised increment across eight goroutines was compiled in a throwaway module and
reported, with the conflicting addresses and the line that wrote them. A detector that
reports nothing because it is not armed is indistinguishable from correct code, and the
only way to tell them apart is to make it fire.

`go test ./... -race` remains a CI gate regardless. Local availability shortens the loop
while `outbox` is written — its dispatcher workers share lease state by design — but the
gate that decides whether a change lands is the one in CI.

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
- `db.Tx`, `db.Pool`, and the `SessionBinder` the transaction manager invokes
- `platform.outbox` schema with daily partitioning and the global sequence
- `platform.processed_event`, `platform.dead_letter`, `platform.idempotency_key`
- `outbox.Append(ctx, tx, envelope)` — takes a transaction handle, so publication
  outside a domain transaction fails to compile
- Dispatcher: `FOR UPDATE SKIP LOCKED`, priority lane, database-backed lease
- Three local retries with exponential backoff and jitter, then dead-letter
- `inbox.Guard` deduplication, transactional with the effect

**Exit:** a domain mutation and its outbox append commit atomically, proven by injecting
a failure between them; a lifecycle backlog of ten thousand rows does not delay a
priority event beyond budget; duplicate delivery produces one effect.

## Week 2 · Technical substrate

- Pool construction and the transaction manager
- RFC 7807 problem serialization and the error taxonomy
- Routing and middleware: correlation, request logging, panic recovery
- OpenTelemetry tracing, metrics, structured logging
- Correlation propagation across the broker boundary

**Exit:** a correlation identifier survives from an inbound HTTP request through a
domain transaction, into an outbox row, across the broker, and into the consumer's
span.

## Week 3 · Hardening and release

- Partition creation job and retention drop
- Backoff behavior on empty polls
- Two-replica dispatcher contention tests
- Ordering guarantee across partition boundaries
- Tag `v0.1.0` and pin both consumers to it

**Exit:** both consuming repositories build against a tagged version rather than a
branch.

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

**Release gate.** The design gate, plus: partition lifecycle exercised end to end,
dead-letter triage runbook written, dispatcher contention proven under two replicas,
and a tagged version consumed by both control repositories.
