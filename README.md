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

## Building locally

Go alone needs no C compiler. The race detector does, because it is implemented in C
and reached through cgo, and it must target the same architecture as the Go toolchain.

```sh
gofmt -l .            # must print nothing
go vet ./...
go build ./...
go test ./... -race -count=1
go run ./tools/archcheck
go mod tidy && git diff --exit-code go.mod go.sum
govulncheck ./...
```

`go test` with no argument tests only the current directory, and the module root holds
no Go files. `./...` is what CI runs and what a local check should match.

## Versioning

Consumers depend on a tagged version, never on a branch. A change to the envelope, the
outbox schema, or the dispatcher contract is a minor version at minimum, and both
consumers upgrade deliberately rather than by rebuild.

`contracts/events` holds event schemas until the enterprise Schema Registry required
by STD-GLB-004 is operational. The CI compatibility check runs against the committed
history in the meantime; the location changes later, the rule does not.
