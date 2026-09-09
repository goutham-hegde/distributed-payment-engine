# ADR 0005 — Stateless JWT authentication, and where authorization actually happens

**Status:** accepted (M5, part 1)
**Supersedes in part:** the `X-Client-Id` header introduced in [ADR 0002](0002-idempotency.md)

---

## Context

Before M5 every endpoint in this system was open. Three of them were worse than merely open:

| Endpoint | What an anonymous caller could do |
|---|---|
| `POST /api/v1/transfers` | move money out of **any** account, by naming it in the body |
| `POST /admin/dead-letters/{id}/replay` | **republish a payment command** |
| `POST /transfers` (account-service) | write ledger entries directly, bypassing the saga |

And the idempotency namespace — `idempotency_records.client_id` — came from a header the caller
chose. Sending another tenant's client id with their key returns *their* stored response body,
which in this system is another customer's transfer receipt.

Two questions had to be answered, and they are not the same question.

1. **Who is calling?** (authentication)
2. **May *this* caller move money out of *that* account?** (authorization)

The second is the hard one here, because **the service that accepts the request does not own the
data the decision depends on**: `POST /api/v1/transfers` arrives at payment-orchestrator, and
`accounts.owner_id` lives in account-service's database.

## Decision

### 1. Stateless JWT, validated locally in all three services

A signed token carries the identity; each service verifies the signature and the claims itself,
with no call to an auth server and no shared session store.

The alternative — server-side sessions — puts a store on the hot path of every request in every
service, and makes instances non-interchangeable. Statelessness is the property being bought.

**The cost, stated plainly: a stateless token cannot be revoked.** It is valid until it expires.
Every mitigation walks back toward state — short TTLs (15 minutes here), a `jti` denylist, token
versioning. This system takes the short TTL and mints `jti` into every token so a denylist is
possible later without invalidating tokens already issued.

Three claims are checked beyond the signature, and each answers a question the signature cannot:
`exp`/`nbf` (when), `iss` (who minted it), `aud` (who it was minted for). The algorithm is
**pinned by the decoder**, never read from the token — which is what makes `alg: none` and the
RS256-verified-as-HS256 confusion attacks structurally impossible rather than merely unlikely.

### 2. HS256 now, RS256 in part 2

A shared secret, distributed to all three services. This is knowingly wrong in one specific way:
**with a symmetric algorithm the verification key and the signing key are the same bytes, so every
service that can validate a token can also forge one.** `TokenIssuer` lives in the shared library
and is registered in all three services precisely so that this is visible rather than hidden.

Part 2 moves to RS256: the orchestrator holds a private key and signs; the others hold only a
public key and can check but not produce. The difference stops being a convention and becomes
mathematics.

### 3. A development token endpoint, not an auth service

`POST /auth/token` in payment-orchestrator, against a directory of demo users in configuration.
It stands in for Keycloak/Auth0/an internal SSO, and swapping it for one changes exactly one thing
in the other services: which key to trust. That is the payoff of local validation.

What it deliberately lacks: hashed passwords, rate limiting, refresh tokens, lockout, MFA. It is a
demo credential source and is documented as one.

**Not** a fourth service, though that would model the trust boundary more honestly — the machine
this runs on has a 16 GB RAM budget that the whole project has been careful with, and a fourth JVM
buys realism the reader can already infer.

### 4. Ownership is projected locally, and checked twice

The decision that took the most thought.

- **account-service publishes `AccountOpened`** to its outbox when an account is opened, in the
  same transaction as the account row.
- **The orchestrator projects it** into `account_owners` and checks that table at the API edge,
  answering **403 immediately** for an account the caller does not own.
- **account-service checks again** when it handles `ReserveFunds`, against the authoritative
  column, under the row lock that is about to move the money. The command carries `initiatedBy`
  for that purpose.

Rejected alternatives:

| Option | Why not |
|---|---|
| Synchronous HTTP call from the orchestrator to account-service | Makes accepting a transfer depend on account-service being up. The saga exists so a participant can be down without the front door closing; this hands that property straight back. |
| Check only in account-service | The caller gets 202 and then a FAILED saga, so an authorization error is indistinguishable from insufficient funds and there is no 403 anywhere in the system. |
| Check only at the edge | The projection is a copy, and `dpe.account.commands.v1` is reachable by anything that can produce to it — a replayed dead letter, a hand-produced message, a compromised producer. "It came through the API" is an assumption. |

**Why a projection is safe here, and the condition on that answer.** The projection is
asynchronous and therefore always slightly behind. It is trustworthy for this decision because
**ownership is immutable in this system** — an account is opened once, by one owner, and there is
no transfer-of-ownership operation. So the table can only ever be *missing* a row, never holding a
wrong one, and a missing row denies. **Staleness fails closed.** The cost is real and bounded: a
transfer out of an account opened milliseconds ago may be refused until the event lands.

If ownership ever becomes mutable, this reasoning collapses and the edge check becomes advisory
only. That question — *can the projected fact change?* — is what decides whether authorizing from
a read model is sound or negligent.

### 5. Two roles, and an operator cannot move money

`USER` owns accounts and may spend from them. `OPERATOR` reads operational surfaces and replays
dead letters. The sets are disjoint, and `AccountOwnershipGuard` has no role bypass: nothing in
this system lets one human move another human's money by holding a role.

No `SERVICE` role. The services talk over Kafka, where the trust boundary is the broker's; a role
for callers that do not exist would be an unused key with production privileges.

### 6. `denyAll()` as the catch-all, not `authenticated()`

They differ only for endpoints nobody wrote a rule for — which is exactly the endpoint someone
adds next month. `authenticated()` exposes it to every customer holding a token; `denyAll()`
exposes it to nobody and surfaces as a 403 in a test.

Consequences of applying that rule honestly:

- **account-service's `POST /transfers` is now unreachable over HTTP.** It writes ledger entries
  with no saga, no idempotency key and no ownership check. There is no role that should have it.
  The service behind it stays — it is the ledger core and the M1 concurrency tests drive it
  directly — only the HTTP door is closed.
- **The gateway's `POST /admin/simulation` is now operator-only.** It was open, with the argument
  that it is a test affordance on a simulated third party. That is no longer good enough: it can
  take the payment path down for every customer at once, and the M6.5 console will call it from a
  browser. An availability control is a security control.

### 7. The signing key has no default anywhere in the repository

`dpe.security.secret` is absent from every `application.yml`. It comes from `DPE_SECURITY_SECRET`,
and a service started without it **refuses to boot**, naming the key and the minimum length.

A default would be a published signing key, and "it is only the dev default" stops being true the
first time a deployment inherits it. Refusing to start is the correct third option next to
inventing a key or running open.

## Consequences

**What got better**

- The idempotency namespace is asserted by the issuer instead of chosen by the caller. The column
  did not change; only where the value comes from did.
- Transfers now record who asked for them (`transfers.initiated_by`) — an audit trail that starts
  at M5, since requests before it carried no identity at all.
- `GET /api/v1/transfers/{id}` is scoped to its initiator, and answers **404** for somebody else's
  transfer. 403 there would confirm the transfer exists to whoever is walking UUIDs — the opposite
  call from the write path, where the caller supplied the account id and 403 is the honest refusal
  of a claim they made.

**What got worse, or is still owed**

- Three services now share a signing secret (until part 2).
- `/actuator/prometheus` is operator-only, so the M6 scraper will need a credential or a network
  exemption. Left closed rather than pre-opened.
- The chaos suite (M7) and k6 (M8) must obtain a token; `scripts/token.sh` exists for that.
- Nothing rate-limits `/auth/token`.
- A newly opened account is briefly unspendable while its `AccountOpened` event is in flight.
