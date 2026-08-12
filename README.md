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
| `outbox` | Outbox table access, dispatcher, lease management, retry and dead-letter routing |
| `event` | CloudEvents 1.0 envelope construction, type naming, schema version binding |
| `inbox` | Deduplication guard over `platform.processed_event` |
| `idempotency` | Idempotency key claim, conflict detection, stored-response replay |
| `httpapi` | Routing, middleware, RFC 7807 problem serialization |
| `db` | Pool construction and the transaction manager that binds outbox writes to domain writes |
| `observability` | Tracing, metrics, structured logging, correlation propagation |
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
| `httpapi/`, `db/`, `observability/` | Technical substrate |
| `contracts/events/` | Versioned event schemas |
| `docs/designs/` | Technical Design Documents |

## Designs

| TDD | Subject | Status |
| :-- | :-- | :-- |
| `TDD-foundation-platform-001` | Transactional outbox, dispatcher, and enterprise event envelope | approved |
| `TDD-foundation-platform-002` | HTTP substrate, persistence, and telemetry | approved |

## Prerequisites

| | Required for | Notes |
| :-- | :-- | :-- |
| Go 1.26 | everything | `go.mod` declares 1.25 as the language floor; CI builds on 1.26 |
| A C compiler matching `GOARCH` | `go test -race` only | The race detector is implemented in C and reached through cgo |
| `govulncheck` | the vulnerability scan | `go install golang.org/x/vuln/cmd/govulncheck@latest` |

Nothing else. No database is needed to run the test suite, and no container runtime —
`db` is tested against a fake connection source rather than a live PostgreSQL.

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
fake whose `pgx.Tx` is embedded but not implemented, so any call beyond commit and
rollback panics on a nil interface. That is a structural assertion, not a mock: it
proves `InTx` touches nothing else, and it keeps failing if that stops being true.

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

**Architecture rules are asserted against the package graph**, not against text.
`tools/archcheck` reads `go list -json`, so an import introduced through an alias, a
blank identifier, or a test file is caught on the same basis as a direct one. The rules
it enforces are declared in `arch.json` and derive from STD-GLB-BE-001.

## Versioning

Consumers depend on a tagged version, never on a branch. A change to the envelope, the
outbox schema, or the dispatcher contract is a minor version at minimum, and both
consumers upgrade deliberately rather than by rebuild.

`contracts/events` holds event schemas until the enterprise Schema Registry required
by STD-GLB-004 is operational. The CI compatibility check runs against the committed
history in the meantime; the location changes later, the rule does not.
