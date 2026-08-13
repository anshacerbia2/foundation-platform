# Foundation Platform

Shared Go substrate for the Scnehaux control plane. It is a versioned library, not a
service: nothing here is deployed, and nothing here holds state of its own.

Two systems compile it — the Identity Control Service (SAD-001) and the Organization &
Tenancy Control application (SAD-004). They share no process, no database, and no
transaction. They share this code because the behavior it implements has to be
identical on both sides.

## Why this exists as one module

The outbox dispatcher is the only mechanism by which a revocation reaches enforcement.
The propagation budget in `TDD-organization-control-002` — accept to outbox commit
within 100 ms, outbox commit to dispatch claim within 1 s — is a property of the code
here, not of either consuming system.

Two divergent copies would produce two different enforcement intervals while both
services reported compliance. That is the failure this module exists to prevent, and
it is worth more than the coupling one shared dependency introduces.

## Packages

| Package | Contents |
| :-- | :-- |
| `id` | UUIDv7 generation, parsing, and ordering — the canonical identifier form under STD-GLB-002 |
| `outbox` | Outbox append, dispatcher, claim and retry policy, dead-letter routing |
| `migrations` | The `platform` schema, embedded so consumers receive it through `go.mod` |
| `event` | CloudEvents 1.0 envelope construction, type naming, schema version binding |
| `inbox` | Deduplication guard over `platform.processed_event` |
| `idempotency` | Idempotency key claim, conflict detection, stored-response replay |
| `httpapi` | Routing, middleware, RFC 7807 problem serialization |
| `db` | Pool construction and the transaction manager that binds outbox writes to domain writes |
| `db/dbtest` | A `db.Tx` that records statements, so packages forbidden from naming the driver can still be tested |
| `observability` | Tracing, metrics, structured logging, correlation propagation |
| `redact` | Credential redaction for logs, problem documents, and persisted failure detail |
| `contracts/events` | Versioned event schemas exchanged between the two systems |

## The one rule

**No package here may contain a domain concept.** No `Principal`, no `Membership`, no
`Tenant`, no `Organization` — not as a type, not as a constant, not as a field name.

This is not stylistic. `identity-control` and `organization-control` are separate
authorities that communicate only through versioned events. A domain type living in a
shared library is a back channel between two authorities that are supposed to have
none, and it would erode exactly the boundary the architecture is built on.

A type that names a business concept belongs in the system that owns it.

## Governance lineage

Strategic and system architecture is owned by the `scnehaux-architecture` repository.
This repository owns technical designs and source code only.

```text
PAD-PLT-001              PAD-PLT-002
    ↓                        ↓
SAD-001                  SAD-004
    └────────┬───────────────┘
             ↓
   TDD-foundation-platform-*
             ↓
        Source code
```

This library carries no SAD of its own. It is not a deployed system, and EAD-002 §6.1
requires a SAD for deployed systems. Its designs name both consuming systems in
`parent_sad`, because both are accountable for the behavior it implements.

## Repository map

| Repository | Role |
| :-- | :-- |
| `identity-kernel` | Keycloak extensions, realm configuration, login theme, image build |
| `identity-control` | Identity Control Service, holds the Keycloak Admin credential |
| `organization-control` | Organization, Tenant, Workspace, Membership authority |
| **`foundation-platform`** | **This library** |
| `identity-experience` | Identity administration UI and its BFF |
| `organization-experience` | Organization administration UI and its BFF |

## Layout

| Path | Contents |
| :-- | :-- |
| `outbox/`, `event/`, `inbox/`, `idempotency/` | Propagation substrate |
| `httpapi/`, `db/`, `observability/`, `redact/` | Technical substrate |
| `db/dbtest/` | A `db.Tx` that records statements, for packages forbidden from naming the driver |
| `migrations/platform/` | The platform schema, embedded and shipped to consumers |
| `contracts/events/` | Versioned event schemas |
| `docs/designs/` | Technical Design Documents |

## The platform schema

This module owns no database. It is a library with no deployable, and EAD-003 forbids
cross-domain persistence, so there is nowhere for one to live.

The `platform` schema exists once inside **each** consuming database — `identity-control`'s
and `organization-control`'s. Those two copies are unrelated and never joined. The shared
name reflects shared code, not shared storage.

The SQL lives here because it must move in lockstep with the code that queries it. A
column added to `platform.outbox` and a change to `outbox.Append` are one change, and
splitting them across repositories permits a deployment where one has shipped and the
other has not.

Consumers receive it through `go.mod` rather than by copying:

```go
import "github.com/anshacerbia2/foundation-platform/migrations"

set, err := migrations.PlatformMigrations()  // ordered by file name
// or hand migrations.Platform, an fs.FS, to Atlas or another runner
```

The migration job also owns daily partition maintenance. Both helpers require an
explicit transaction opened with the migration role:

```go
err := migrationPool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
    if _, err := migrations.EnsureOutboxPartitions(ctx, tx, today, today.AddDate(0, 0, 7)); err != nil {
        return err
    }
    _, err := migrations.DropPublishedOutboxPartitions(ctx, tx, today.AddDate(0, 0, -30))
    return err
})
```

This package applies nothing automatically. Schema application and partition maintenance
are DDL, and TDD-foundation-platform-001 requires both to run under a role distinct from
the runtime role, which holds no DDL privilege. That role belongs to the consuming
system's migration job, not to its application process.

## Designs

| TDD | Subject | Status |
| :-- | :-- | :-- |
| `TDD-foundation-platform-001` | Transactional outbox, dispatcher, and enterprise event envelope | approved |
| `TDD-foundation-platform-002` | HTTP substrate, persistence, and telemetry | approved |

## Publishing an event

There is one publication path, and it takes a transaction handle it does not open. A
service that mutates state and publishes in two transactions does not compile.

```go
err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
    if err := memberships.Save(ctx, tx, m); err != nil {   // the domain mutation
        return err
    }

    envelope, err := event.New(source, typ, m.RevokedAt(), payload)
    if err != nil {
        return err
    }

    return outbox.Append(ctx, tx, m.ID(), envelope, outbox.Priority())
})
```

`outbox.Priority()` routes the event to the reserved dispatch lane, which exists so a
lifecycle backlog cannot delay a revocation. Use it for security state changes and nothing
else; a lane everything is urgent in reserves nothing.

The dispatcher is constructed by the composition root and driven by `Run`, which blocks
until its context is cancelled and then waits for every worker. It starts nothing on
import, because a package that starts a goroutine when it is linked cannot be shut down by
the process that linked it.

```go
dispatcher, err := outbox.NewDispatcher(pool, broker, outbox.Config{})
if err != nil {
    return err
}
go func() { errs <- dispatcher.Run(ctx) }()
```

`broker` is anything satisfying `outbox.Publisher`. This module holds no broker client: the
interface is declared here because this package is what needs it, and the adapter that
satisfies it belongs to the consuming system, whose SAD records which broker it is.

A publisher signals an unfixable failure by wrapping `outbox.ErrPoison`. Everything else is
treated as a broker that may return, which is deliberate — an unrecognised error is never
poison, because discarding a revocation costs more than three wasted retries.

## Consuming an event

The inbox guard and the effect share one transaction. The composite key includes the
logical consumer, so several consumers in one deployable may each process one event once:

```go
err := pool.InTx(ctx, func(ctx context.Context, tx db.Tx) error {
    first, err := inbox.Guard(ctx, tx, consumerName, envelope.ID, envelope.Type)
    if err != nil || !first {
        return err
    }
    return projection.Apply(ctx, tx, envelope)
})
```

`idempotency.Claim` similarly scopes an HTTP mutation key to the authenticated caller.
The claim, mutation, outbox append, and `idempotency.Complete` belong in one transaction.
A matching completed claim returns a stored response; a different digest returns
`idempotency.ErrConflict`; an unfinished matching claim returns
`idempotency.ErrInProgress`.

## HTTP and telemetry

`httpapi.Chain` fixes middleware order while the consumer supplies authentication,
authorization, and mutation idempotency hooks. `httpapi.NewServer` applies bounded
transport defaults, and every error written by `httpapi.Problem` comes from the compiled
RFC 7807 registry.

`observability.New` receives process-owned OpenTelemetry providers and a `slog.Logger`.
It starts no exporter. Correlation is propagated or generated at the HTTP boundary,
embedded in event data through `observability.MetadataFromContext`, restored by the
consumer, and linked to the producer span by `StartConsumer`.

## Prerequisites

| | Required for | Notes |
| :-- | :-- | :-- |
| Go 1.26 | everything | `go.mod` declares 1.25 as the language floor; CI builds on 1.26 |
| A C compiler matching `GOARCH` | `go test -race` only | The race detector is implemented in C and reached through cgo |
| `govulncheck` | the vulnerability scan | `go install golang.org/x/vuln/cmd/govulncheck@latest` |
| PostgreSQL 17 | the integration suite only | Optional locally; CI runs one as a service container |

The unit suite needs no database and no container runtime. The integration suite needs
one, and skips itself when `TEST_DATABASE_URL` is unset.

The C compiler is the only prerequisite that catches people out, because a compiler
being present is not the same as it being usable. It must emit binaries for the same
architecture as the Go toolchain.

```sh
go env GOARCH      # amd64
gcc -dumpmachine   # must agree, e.g. x86_64-w64-mingw32
```

`x86_64` or `aarch64` in the compiler triple must line up with `amd64` or `arm64` in
`GOARCH`. A mismatch fails at link time with `64-bit mode not compiled in` or a similar
message, which reads like a broken build rather than a missing prerequisite.

On Windows, MinGW.org is a dead project and its last GCC is 32-bit only. Use MinGW-w64:
extract a [WinLibs](https://winlibs.com) archive and put its `bin` ahead of any older
toolchain on `PATH`. On Linux and macOS the system compiler already matches.

Environment-specific findings for this workstation — proxy behaviour, install paths —
are recorded in [ROADMAP.md](ROADMAP.md) rather than here, because they describe a
machine rather than the repository.

## Verifying a change

These are the same checks CI runs, in the same order. `.github/workflows/ci.yml` is
authoritative; this list exists so a failure is found before a push, not after.

```sh
gofmt -l .                                      # must print nothing
go vet ./...
go build ./...
go test ./... -race -count=1
go run ./tools/archcheck
go run ./tools/schemacheck
go mod tidy && git diff --exit-code go.mod go.sum
govulncheck ./...
```

`go test` with no argument tests only the current directory, and the module root holds
no Go files — hence `no Go files in ...`. Always pass `./...`.

`-count=1` disables the test result cache. Without it a passing run may be a replay of
an earlier one, which is the wrong thing to trust when the point is to check the code
in front of you.

## Testing conventions

**Every check must be able to fail.** A gate that has never rejected anything is
indistinguishable from no gate, so each one here has been made to fire deliberately at
least once:

- `tools/archcheck` has a fixture per rule that violates it, and the test asserts the
  rule rejects it. See `tools/archcheck/testdata/`.
- The race detector was verified armed by compiling a deliberate unsynchronised write
  and confirming it was reported.

**Tests do not require infrastructure.** `db` exercises transaction semantics against a
fake whose `pgx.Tx` is embedded but not implemented, so any unconfigured call panics on a
nil interface. `db/dbtest` records execs, rows, and result sets for packages above the
driver boundary. These are structural assertions rather than behaviour-rich mocks.

`db.Open`, `db.Ping`, and `db.Close` are consequently uncovered. They need a running
PostgreSQL and belong to integration tests, not to this suite.

**Coverage floor is 80%**, measured over shipped packages only:

```sh
go test $(go list ./... | grep -v '/tools/') -covermode=atomic -coverprofile=coverage.out
go tool cover -func=coverage.out
```

`tools/` is excluded because it is verified by its own tests rather than by a
percentage, and a large build tool would otherwise move a number that is meant to
describe the library. The floor is a floor, not a target — coverage that rises by
testing getters buys nothing.

**Integration tests answer what a double cannot.** Whether the driver encodes a string as
`uuid` and a `[]byte` as `jsonb`, and whether the shipped DDL parses at all, are defects
that survive every test written against a fake. Point the suite at a throwaway database:

```sh
docker run --rm -d -p 5432:5432 \
  -e POSTGRES_USER=platform -e POSTGRES_PASSWORD=platform -e POSTGRES_DB=platform_test \
  postgres:17-alpine

TEST_DATABASE_URL='postgres://platform:platform@localhost:5432/platform_test?sslmode=disable' \
  go test ./... -count=1
```

The suite drops and rebuilds the `platform` schema on every run, so it never passes
against state an earlier run left behind.

Set `REQUIRE_INTEGRATION=1` to turn a skip into a failure. CI sets it, because a service
container that never came up would otherwise leave every database test skipped and the
run green — indistinguishable from having checked something.

**Architecture rules are asserted against the package graph**, not against text.
`tools/archcheck` reads `go list -json`, so an import introduced through an alias, a
blank identifier, or a test file is caught on the same basis as a direct one. The rules
it enforces are declared in `arch.json` and derive from STD-GLB-BE-001.

## Versioning

Consumers depend on a tagged version, never on a branch. A change to the envelope, the
outbox schema, or the dispatcher contract is a minor version at minimum, and both
consumers upgrade deliberately rather than by rebuild.

`contracts/events` holds event schemas until the enterprise Schema Registry required
by STD-GLB-004 is operational. `tools/schemacheck` rejects removed properties, changed
types, newly required properties, unsafe paths, and malformed registry entries. The
location changes later; the compatibility rule does not.
