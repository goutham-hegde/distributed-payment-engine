# ADR 0007 — The console is a reader, and one origin sits in front of three services

**Status:** accepted (M6.5)
**Supersedes:** nothing
**Related:** ADR 0005 (JWT authentication), ADR 0006 (trace context in the outbox)

## Context

Everything this system does correctly is invisible. A saga compensating, an inbox absorbing a
duplicate, a hold sitting in CLEARING, the relay lag between a row committing and a message
leaving — all of it is true in Postgres and in Kafka and nowhere a person can look. The evidence
so far has been `psql`, `rpk`, a Grafana dashboard of aggregates, and a shell script that prints
five PASS lines.

M6.5 adds a browser console over the read endpoints M6 part 3 built: track a transfer, watch the
system, and turn the gateway's failure injectors on to make compensation happen on demand.

Three questions had to be settled before any of it was written.

## Decision 1 — one origin, not CORS

nginx serves the built app and reverse-proxies `/api/orchestrator`, `/api/accounts` and
`/api/gateway` to the three services, stripping the prefix. The browser makes same-origin requests
and no service knows it is behind anything.

The alternative was to let the browser call `localhost:8081`, `:8082` and `:8083` directly with
CORS enabled on all three. Rejected for two reasons:

1. It puts a browser concern into three server configurations, where it will be copied forward.
2. CORS is a policy about who may **read a response**. It is routinely mistaken for an
   authorization mechanism, and this repository has spent M5 putting every such decision in one
   place per service. Adding a second, weaker, browser-enforced one next to it would be the
   beginning of a confusion, not a feature.

A reverse proxy is also the shape a real edge has, which makes the console's deployment story the
same as the system's.

The proxy holds no credential. It forwards the browser's `Authorization` header because forwarding
request headers is what a proxy does by default — there is deliberately no `proxy_set_header
Authorization` line, because writing one would imply this component is trusted with something.

## Decision 2 — the console never parses the token

The roles and the lifetime come from the body of `POST /auth/token`, which the server puts there
precisely so that a client need not open its own credential to use it. There is no JWT decoding
anywhere in `ui/`.

Decoding it would have been one line and would have created two problems. It is a second, subtly
different reading of a claim the server has already interpreted — and the `ROLE_` prefix is exactly
where such readings diverge, as `AccountController.isOperator` notes on the server side. And it
creates the impression that the client is making an authorization decision.

Roles in the UI drive **affordance only**: which tabs are offered, and what a disabled tab's
tooltip says. A tab is disabled rather than hidden, because "OPERATOR sees every operational
surface and cannot move money" is a true and interesting statement about this system, and hiding
the tab would replace it with the impression that different users get different products.

Every rule that matters is enforced on the server against the signature. Editing the URL hash
produces 403s, not access.

## Decision 3 — the five lights are joined in the browser, and I3 gets no light

The console fetches `/admin/invariants` from **both** services and renders the union. It does not,
and must not, call one endpoint that answers all five.

I1, I2, I3 and I5 are statements about `accounts_db`; I4 is a statement about `payments_db`. A
service that could answer all five would hold credentials to both databases — a shared-database
architecture reintroduced through the monitoring door, and the widest-access component in the
estate, added for a dashboard. So each service answers for its own database, and the join happens
in a browser holding nothing but the operator's own token.

Two consequences the UI has to respect rather than smooth over:

- **`Promise.allSettled`, not `Promise.all`.** If one service is down, the other half is still
  worth showing; an all-or-nothing fetch would blank the panel that says which half is broken.
- **I3 is rendered as a total, never as a green light, and I4 is never rendered red.** Conservation
  compares two instants and a page sees one — it cannot know when a run began or whether an account
  was legitimately funded since. I4 counts sagas in flight, which is what a working system under
  load looks like; it is a violation only after quiescence, and a browser cannot know when that is.
  Five green lights with a sixth that quietly asserts something it could not check would be a lie
  in the one place this system claims to prove something, and a panel that flashes red under normal
  load teaches its operators to ignore it.

## Decision 4 — Prometheus is proxied unauthenticated, and the bound is written down

The System view's backlog, outbox-age, DLQ-depth and in-flight tiles come from PromQL, not from
repository calls issued by the page. That follows the M6 gauge rule one level up: a console
querying the payment write path every five seconds would be the observability becoming the outage.

It means one nginx route reaches Prometheus with no authentication. This is accepted, and bounded:

- Prometheus has no authentication to pass through; it never has, by design.
- It is already published on `:9090` to the host by Compose, so this widens nothing on this machine.
- What it exposes is aggregate operational data and never a ledger row — and it cannot expose one,
  because the M6 cardinality rule forbids `transfer_id`, `account_id` or an idempotency key as a
  label. A decision made to protect Prometheus's memory happens to bound this too.
- Only `query`, `query_range`, `rules` and `alerts` are routed. The admin API is unreachable
  even though it is also disabled.

It is the first thing to close if this ever leaves a laptop.

## Consequences

- `ui/` is **not** a Maven module. It is absent from the parent POM and from the root `Dockerfile`'s
  `COPY` lists, and has its own `ui/Dockerfile`. The project rule "add every new module to the
  Dockerfile and the parent POM" is about the Maven reactor; applying it here breaks the build for
  all three services.
- A fourth Compose container, ~15 MB resident. The node build stage is discarded; a node process
  serving the app at runtime would have cost ~100 MB and bought nothing, since there is nothing to
  render on a server.
- The wire types in `ui/src/api/types.ts` are hand-transcribed from the Java records and **nothing
  checks them against the server**. A renamed field is `undefined` at runtime, not a build failure.
  Each type names the record it mirrors so the counterpart is findable. If M9 produces an OpenAPI
  document, these should be generated from it and this paragraph deleted.
- Polling, not websockets. Pushing these numbers would mean a publisher on the write path — a
  second write next to the business transaction, which is the dual-write problem arriving through
  the UI door.
