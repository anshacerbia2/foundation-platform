---
doc_meta:
  id: TDD-foundation-platform-002
  title: HTTP Substrate, Persistence, and Telemetry
  owner: Core Platform Team
  version: 1.4.0
  status: approved
  classification: restricted
  review_cycle_days: 90
  created_date: 2026-08-11
  last_reviewed: 2026-10-07
  parent_sad:
    - SAD-001
    - SAD-004
---

# HTTP Substrate, Persistence, and Telemetry

## Purpose

Specify the three packages every consumer of this library depends on but which
`TDD-foundation-platform-001` names without designing: the HTTP middleware chain and
its RFC 7807 error contract, the connection pool and transaction manager that make the
outbox append atomic, and the telemetry that carries correlation from an inbound
request through a domain transaction and across the broker.

These are shared for the same reason the dispatcher is shared. An error taxonomy that
differs between two control services produces two client integration surfaces. A
transaction manager that differs produces two atomicity guarantees, one of which will
be weaker.

## Scope

**In scope**

- The middleware chain, its fixed order, and the reason for that order.
- RFC 7807 problem serialization and the problem type registry.
- Pool construction, the transaction manager, and the session-binding hook.
- Tracing, metrics, structured logging, and correlation propagation.
- Redaction, and where it is enforced.

**Out of scope**

- Outbox, dispatcher, envelope, and deduplication — owned by
  `TDD-foundation-platform-001`.
- Which routes exist and what they mean — owned by each consuming system.
- Row-Level Security policies and the tenant predicate — owned by
  `TDD-organization-control-001`. This library supplies the mechanism and names no
  tenant.
- Authentication and authorization decisions, which belong to the consuming system.

## Technical Context

This library serves two systems that share no process, no database, and no domain. The
constraint from `TDD-foundation-platform-001` applies here without exception: **no
package may contain a domain concept.**

That constraint is load-bearing in one specific place. `TDD-organization-control-001`
requires every tenant-scoped transaction to issue `SET LOCAL app.tenant_id`, and
requires that statement to appear in exactly one package. That package is `db`, here,
in a library that must not know what a tenant is.

The resolution is a hook rather than a feature: the pool accepts a session binder
supplied at the composition root. This library invokes it; the consuming system decides
what it sets. `foundation-platform` never names `app.tenant_id`, and
`organization-control` never issues a raw `SET LOCAL` of its own.

## Component Design

### Middleware Chain

Order is fixed, and each position is a consequence rather than a preference:

```text
1. panic recovery
2. correlation
3. request logging
4. timeout and load shedding
5. authentication context extraction
6. idempotency claim          (mutations only)
7. handler
```

| Position | Why there |
| :-- | :-- |
| Recovery outermost | A panic in any later layer still produces a response and an event. Recovery inside logging means a panic in logging escapes the process. |
| Correlation second | Everything after it can log, trace, and emit with an identifier. A correlation assigned after logging produces the first log line of every request without one, which is the line you need during an incident. |
| Logging third | It records the outcome of everything inside it, including shed requests. |
| Shedding before authentication | Rejecting overload should not first cost a token validation. ADR-GLB-005 makes load shedding an enterprise invariant. |
| Idempotency after authentication | A key is claimed per authenticated caller, so an unauthenticated request cannot consume another caller's key. |

Consuming systems add their own authorization middleware after position 5. This
library provides none, because an authorization decision made in shared code would be
a decision made outside the domain that owns it.

### Package Surface

```go
package httpapi

// Chain builds the fixed middleware order. A consumer supplies its authentication
// extractor and its authorization middleware; neither is implemented here.
func Chain(opts Options) func(http.Handler) http.Handler

// Problem writes an RFC 7807 document. It is the only sanctioned error response
// path; a handler writing an error body directly fails the architecture test.
func Problem(w http.ResponseWriter, r *http.Request, p ProblemType, detail string)
```

```go
package db

// SessionBinder is invoked at the start of every transaction opened through this
// pool. The library supplies the call site; the consumer supplies the statement.
// This is how tenant scope is bound without this package naming a tenant.
type SessionBinder interface {
    Bind(ctx context.Context, tx pgx.Tx) error
}

// InTx runs fn inside a transaction, invoking the pool's SessionBinder first.
// It is the only path that yields a transaction handle, which is what makes
// outbox.Append impossible to call outside a domain transaction.
//
// fn receives a derived context and must propagate it. That context carries a
// marker recording that a transaction is open, so a service opening a second
// one on the same call path is refused rather than silently acquiring a
// second connection and losing atomicity between the two.
//
// The marker is a fact, not a handle. It grants no capability and cannot be
// used to run a query, so it does not reintroduce the implicit transaction
// STD-GLB-BE-001 rule 6 prohibits.
func (p *Pool) InTx(ctx context.Context, fn func(context.Context, Tx) error) error
```

`InTx` yielding the only transaction handle in the system is deliberate. Together with
`outbox.Append(ctx, tx, ...)` from design 001, it means publication outside a domain
transaction does not compile. The atomicity guarantee is enforced by the type system
rather than by review.

### Workload Client Credentials

`clientauth` gives a service an access token as its own workload: the OAuth 2.0 client
credentials grant, authenticated with a signed JWT client assertion (`private_key_jwt`). A
producer's dispatcher presents this token to a consumer's acceptance API (`ADR-GLB-018 §5.4`), and
any service calling another as itself uses the same package.

```go
package clientauth

// LoadKey reads an RSA private key in PKCS#8 or PKCS#1 PEM and refuses one under 3072 bits.
func LoadKey(path string) (*Key, error)

// Assertion signs an RFC 7523 client assertion, PS256, with a fresh jti.
func (k *Key) Assertion(clientID, audience string, now time.Time) (string, error)

// NewTokens builds a token source for one client at one token endpoint.
func NewTokens(cfg Config) (*Tokens, error)

// Token returns a cached access token, or acquires one when none is held or it is within the
// leeway of its expiry.
func (t *Tokens) Token(ctx context.Context) (string, error)

// Invalidate drops the cached token, for a resource that refused it with 401.
func (t *Tokens) Invalidate()
```

The rules, each from the standard it implements:

- **The grant is client credentials.** A client uses it "when the client is requesting access to
  the protected resources under its control, or those of another resource owner that have been
  previously arranged with the authorization server", and it "MUST only be used by confidential
  clients" (RFC 6749 §4.4 [R1]). A workload holding a private key is a confidential client.
- **The assertion.** `client_assertion_type` is
  `urn:ietf:params:oauth:client-assertion-type:jwt-bearer`, and for client authentication "the
  subject MUST be the 'client_id' of the OAuth client". The client is also the issuer, and the
  audience "identifies the authorization server as an intended audience" (RFC 7523 §2.2, §3
  [R2]). The audience is the configured issuer, not an address derived from the token endpoint:
  a service reaches the kernel on an internal address, and an assertion naming it is refused.
- **Single use and short-lived.** Each assertion carries a fresh `jti` and expires one minute after
  it is signed, so the kernel's replay check has a short window to cover (OpenID Connect Core §9,
  `STD-IAM-001` [R6]).
- **The key.** RSA of at least 3072 bits, signed `PS256`, with a `kid` that is the key's RFC 7638
  thumbprint (`STD-IAM-001 §3`, `STD-IAM-002 §3.2.2`). The key is read from a file and never
  logged.
- **The token stays in memory.** It is cached until `Leeway` (default 30 s) before its stated
  expiry. A response with no `expires_in` is used once and not cached. A 400, 401 or 403 from the
  token endpoint is reported as `ErrRejected` and not retried. Every other failure is
  `ErrUnavailable`.

It is the same mechanism `identity-control` uses for its Admin API clients
(`internal/keycloak/clientkey.go`). That copy stays where it is until it has a reason to move.

**The tradeoff.** Every caller needs a client registered in the kernel and a key file to rotate,
where a shared bearer token needs neither. `STD-IAM-001 §3` prohibits the long-lived static secret
when a workload identity is available, and a token the resource can verify names the workload,
where a shared secret names only whoever holds it.

| Ref | Source |
| :-- | :-- |
| R1 | IETF, *RFC 6749 The OAuth 2.0 Authorization Framework*, §4.4, <https://www.rfc-editor.org/rfc/rfc6749#section-4.4>, accessed 2026-10-02 |
| R2 | IETF, *RFC 7523 JSON Web Token (JWT) Profile for OAuth 2.0 Client Authentication and Authorization Grants*, §2.2, §3, <https://www.rfc-editor.org/rfc/rfc7523#section-3>, accessed 2026-10-02 |

## Data Model

### Problem Type Registry

Every RFC 7807 `type` is a stable URI drawn from a registry compiled into the library.
A handler cannot invent one.

```text
https://problems.scnehaux.com/validation-failed             400
https://problems.scnehaux.com/authentication-required       401
https://problems.scnehaux.com/forbidden                     403
https://problems.scnehaux.com/not-found                     404
https://problems.scnehaux.com/version-conflict              409
https://problems.scnehaux.com/idempotency-key-conflict      409
https://problems.scnehaux.com/state-transition-refused      409
https://problems.scnehaux.com/request-in-progress           409
https://problems.scnehaux.com/precondition-unmet            412
https://problems.scnehaux.com/payload-too-large             413
https://problems.scnehaux.com/rate-limited                  429
https://problems.scnehaux.com/overloaded                    503
https://problems.scnehaux.com/dependency-unavailable        503
https://problems.scnehaux.com/internal                      500
```

A registry rather than free-form strings, because a client that cannot enumerate the
error space pattern-matches on `detail` text, and `detail` is prose that changes.

**`payload-too-large` is not `validation-failed`.** A request that is well formed and larger than
a limit the service declares, such as the number of items one batch may carry, is answered 413.
HTTP defines the status as a refusal "because the request content is larger than the server is
willing or able to process" (RFC 9110 §15.5.14 [R3]), which a client can act on by splitting the
request and sending it again. A 400 says the request itself is wrong, and splitting it would not
help. SCIM makes the distinction mandatory for a bulk request: "If either limit is exceeded, the
service provider MUST return HTTP response code 413 (Payload Too Large). The returned response
MUST specify the limit exceeded in the body of the error response" (RFC 7644 §3.7.4 [R4]). The
`detail` therefore names the limit, for example `A batch carries at most 500 items`. The URI keeps
RFC 7644's name for the status; RFC 9110 later renamed it Content Too Large, and the code is the
same.

| Ref | Source |
| :-- | :-- |
| R3 | IETF, *RFC 9110 HTTP Semantics*, §15.5.14 413 Content Too Large, <https://www.rfc-editor.org/rfc/rfc9110#section-15.5.14>, accessed 2026-10-07 |
| R4 | IETF, *RFC 7644 System for Cross-domain Identity Management: Protocol*, §3.7.4 Maximum Operations, <https://www.rfc-editor.org/rfc/rfc7644#section-3.7.4>, accessed 2026-10-07 |

```json
{
  "type": "https://problems.scnehaux.com/version-conflict",
  "title": "The record changed since it was read",
  "status": 409,
  "detail": "Expected version 14, found 15.",
  "instance": "/v1/memberships/019235f5-...",
  "correlation_id": "019235f6-..."
}
```

`correlation_id` is the only extension member. It is what lets a caller reporting an
error be joined to the trace that produced it.

### Redaction

Redaction is applied by the serializer, not by the caller:

| Class | Treatment |
| :-- | :-- |
| Token, credential, key material, cookie value | Never serialized, under any field name |
| Identifiers ending `_id` | Emitted |
| Free-text `detail` | Emitted, and must carry no value read from a request body |
| Request body | Never emitted |

Enforcing at the serializer is the point. A rule that each caller must remember is a
rule one caller will forget, and the forgotten instance is the one in the error path
nobody exercises.

## API / Interface

### Telemetry Contract

Every span, metric, and log line carries:

```text
deployable        identity-control | organization-control
system            SAD-001 | SAD-004
correlation_id    propagated inbound, generated when absent
causation_id      the event or request that caused this one
```

`deployable` and `system` are what make load, latency, and error rates attributable
per system while both run the same substrate code.

### Correlation Across the Broker

```text
inbound HTTP request
    → correlation_id assigned or propagated
    → domain transaction
    → outbox row, envelope carries correlation_id and causation_id
    → dispatcher publishes
    → consumer reads the envelope
    → consumer span links to the producer span
```

The link is explicit rather than inferred from timing. A revocation that fails to
enforce is investigated by following one identifier from the operator's click to the
Keycloak call, across two services and a broker, and that path exists only if the
identifier survives every hop.

## Algorithms / Logic

### Transaction and Binding

```text
InTx(ctx, fn):
    acquire a connection from the pool
    BEGIN
    if the pool has a SessionBinder:
        binder.Bind(ctx, tx)          -- consumer issues SET LOCAL here
        on error: ROLLBACK and return
    fn(tx)
    on error: ROLLBACK and return
    COMMIT
```

`SET LOCAL` reverts at commit or rollback, so a pooled connection cannot carry one
transaction's binding into the next. That property is what makes connection pooling and
Row-Level Security safe together, and it is why the binder runs inside the transaction
rather than on connection acquisition.

A pool configured with a binder that fails is a pool that opens no transaction. An
unbound transaction against an RLS-protected table would raise on the first query
anyway; failing at `Bind` fails earlier and names the cause.

### Load Shedding

```text
on inbound request:
    if in-flight requests exceed the configured ceiling:
        respond 503 with problem type overloaded and Retry-After
        record the shed
    else:
        proceed
```

Shedding happens before authentication so that rejecting overload costs no token
validation, and before the handler so it costs no database connection. ADR-GLB-005
makes this an enterprise invariant rather than a local optimisation.

### Timeout Propagation

Every inbound request carries a deadline into its context. The transaction manager, the
outbox append, and every outbound call inherit it. A handler that outlives its deadline
is cancelled rather than left to complete work whose caller has gone.

Downstream timeouts are configured below the caller's budget, per SAD-004 §9.1.3, so a
dependency's slowness surfaces as a dependency error rather than as a caller timeout
that names nothing.

## Configuration

| Variable | Default | Purpose |
| :-- | :-- | :-- |
| `HTTP_READ_TIMEOUT` | `10s` | Inbound read deadline |
| `HTTP_WRITE_TIMEOUT` | `30s` | Inbound write deadline |
| `HTTP_MAX_IN_FLIGHT` | `256` | Load-shedding ceiling |
| `HTTP_SHUTDOWN_GRACE` | `20s` | Drain before exit |
| `DB_MAX_CONNS` | `20` | Pool ceiling, set per pool by the consumer |
| `DB_MAX_CONN_LIFETIME` | `30m` | Connection recycling |
| `DB_ACQUIRE_TIMEOUT` | `3s` | Bound on waiting for a connection |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | none | The Collector's OTLP/HTTP base URL. The composition root passes it to `observability.Export`. A deployment must set it; unset, a deployable exports nothing and logs a warning at startup, and the absent-telemetry alert fires |
| `LOG_LEVEL` | `info` | Structured log level |

Each consuming deployable supplies its own values. This library reads no configuration
directly; the composition root constructs and injects it.

## Testing Strategy

### Middleware

- A panic in any layer produces a `500` problem document and an event carrying the
  correlation identifier.
- The first log line of a request already carries the correlation identifier.
- An inbound correlation identifier is propagated; its absence generates one.
- A shed request is rejected before authentication runs, asserted by an authentication
  extractor that fails the test if invoked.
- An idempotency key claimed by one authenticated caller cannot be claimed by another.

### Problem Documents

- Every error response validates against RFC 7807.
- Every `type` resolves to a registry entry; an unregistered type fails to compile.
- Every declared constant has a complete registry entry (`TestEveryProblemTypeIsRegistered`).
- `payload-too-large` answers 413 with a `detail` naming the limit, and is distinguishable from
  `validation-failed` (`TestPayloadTooLargeAnswers413`).
- No response body, at any status, contains a token, credential, key, or cookie value,
  asserted by fuzzing handler errors with credential-shaped input.
- A `detail` string never echoes a request body value.

### Transaction Manager

- `outbox.Append` cannot be called without a transaction handle, asserted by a
  compilation test.
- `InTx` invokes the `SessionBinder` before `fn` and inside the transaction.
- A binder returning an error rolls back and opens no work.
- `SET LOCAL` issued by a binder does not survive into the next transaction on the same
  pooled connection.

### Telemetry

- Every span, metric, and log line carries `deployable` and `system`.
- A correlation identifier survives from inbound HTTP through the domain transaction,
  into the outbox envelope, across the broker, and into the consumer span.
- The consumer span links to the producer span explicitly.
- No log line contains a token, credential, key, or unrestricted personal data.
- `observability.Export` delivers a recorded metric to `/v1/metrics` and an ended span to
  `/v1/traces` on the Collector, flushed by `Shutdown`, and refuses an endpoint that is not an
  http(s) URL or telemetry with no `deployable` or `system`.

## Security Notes

Redaction at the serializer rather than at the caller is the single most consequential
choice here. Error paths are the least exercised code in any service, and an error path
that echoes a request body is how a credential reaches a log aggregator that a wider
audience can read than the database ever had.

Load shedding before authentication is a deliberate ordering: it means an unauthenticated
flood costs a counter increment rather than a signature verification, which is what
keeps the authentication path available during one.

This library holds no credential and makes no authorization decision. A pull request
adding either is rejected on principle. Authorization in shared code is a decision made
outside the domain that owns it, and it would be invisible to the review that should
have caught it.

## Performance Notes

The middleware chain adds a fixed, small per-request cost dominated by correlation
generation and structured log formatting. Load shedding is a counter comparison.

`InTx` adds one round trip per transaction when a binder is configured, which is the
cost of Row-Level Security being enforced by the database rather than by the
application. That cost is accepted by `TDD-organization-control-001` and is not
re-litigated per query.

Pool sizing is per consumer. `organization-control` runs two pools with different
ceilings, and the provider pool is deliberately small so a runaway cross-tenant job
exhausts its own capacity rather than tenant-facing capacity.

## Operational Notes

| Signal | Warning | Critical |
| :-- | :-- | :-- |
| Shed request rate | above baseline | sustained |
| Pool acquisition timeout | any occurrence | sustained |
| Panic recovered | any occurrence | more than one per hour |
| Correlation identifier absent on a span | — | any occurrence |
| Credential-shaped value detected in a log | — | any occurrence |

Runbooks required before release: pool exhaustion, sustained shedding, and telemetry
export failure.

## Traceability

| Relationship | Target |
| :-- | :-- |
| Parent system | SAD-001 — Scnehaux Identity Runtime |
| Parent system | SAD-004 — Scnehaux Organization Control |
| Governed by | ADR-GLB-005 — load shedding and resilience invariants |
| Conforms to | STD-GLB-001 — RFC 7807 problem details; JSON over HTTP; path-level versioning |
| Conforms to | STD-GLB-003 — observability |
| Enterprise constraint | EAD-005 §5.3 — OpenTelemetry-compatible instrumentation and vendor-neutral export |
| Related design | `TDD-foundation-platform-001` — outbox, dispatcher, envelope |
| Consumed by | `TDD-organization-control-001` — the session binder is how tenant scope is bound |
| Consumed by | Every HTTP surface in `identity-control` and `organization-control` |
