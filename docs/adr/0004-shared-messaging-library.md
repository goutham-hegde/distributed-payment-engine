# ADR 0004 — A shared messaging library, but never a shared domain

**Status:** accepted (M3)
**Supersedes:** nothing
**Related:** ADR 0003 (shared contracts)

## Context

Until M3 only account-service produced messages, so the transactional outbox — the entity, the
repository, the writer, the relay and its scheduler — lived inside that service. The saga changes
that. All three services become both producers and consumers:

| Service | Produces | Consumes |
|---|---|---|
| `payment-orchestrator` | `ReserveFunds`, `ChargeGateway`, `CommitFunds`, `ReleaseFunds` | every reply |
| `account-service` | `FundsReserved`, `ReserveRejected`, `FundsCommitted`, `FundsReleased` | the account commands |
| `payment-gateway` | `GatewayApproved`, `GatewayDeclined` | `ChargeGateway` |

So each of them needs an outbox, a relay and an inbox. That is roughly 600 lines of infrastructure
whose logic — claim `FOR UPDATE SKIP LOCKED`, publish, mark published, absorb duplicates on the
way in — is identical everywhere and has nothing to do with what any service does.

## Options considered

**1. Copy it into each service.** The textbook microservice answer: no shared library, no shared
release cadence, genuinely independent deployables. Rejected because the duplication is not
incidental. A defect in the relay's failure handling — the batch-reordering case documented in
`OutboxRelay` — would have to be found and fixed three times, and the three copies would drift
under exactly the pressure that makes drift dangerous: nobody reads the other two while fixing
one.

**2. Extract everything shared, including the contracts and the handlers.** Rejected. It ends with
a shared domain, which is the failure mode that gives shared libraries their bad name in this
architecture.

**3. Extract only the infrastructure.** Chosen.

## Decision

`common-messaging` holds the outbox and inbox mechanics and nothing else. The test it has to pass
is: **does this module know what a transfer is?** It does not, and must not.

- It holds no domain type, no money, no saga state.
- The payload is an opaque `String` on the way out and an opaque `String` on the way in. The relay
  never deserializes; it routes on what the row says.
- It declares no topics. Which topics a service participates in is a statement about its
  contracts, and each service makes it in its own `KafkaTopicsConfig`.

**Each service keeps its own `outbox` and `inbox` tables**, in its own database, created by its own
Flyway migration. The DDL is duplicated across three migration files on purpose. A shared table
would be a shared database, which is the coupling database-per-service exists to prevent — and it
is the one kind of duplication that is not worth removing.

## Consequences

**Accepted cost.** Three services now share a library, so a change to it forces all three to
rebuild, and a breaking change to it is a coordinated deployment. That is real. It is accepted
because the shared surface is infrastructure with no business meaning: adding a field for one
service's benefit cannot change another service's behaviour, because there are no fields to add.

**A Spring wiring cost, paid once per service.** Boot scans for entities and repositories starting
from the package of the `@SpringBootApplication` class, and `com.dpe.messaging` is not under
`com.dpe.account`. So each application class carries `@Import(MessagingConfig.class)` plus
`@EntityScan` and `@EnableJpaRepositories` naming **both** its own package and the library's.

Both packages must be named together, and this is the trap rather than a detail: `@EntityScan` and
`@EnableJpaRepositories` **replace** the default scan rather than adding to it. `MessagingConfig`
declaring `@EnableJpaRepositories("com.dpe.messaging")` on a service's behalf would silently
unregister that service's own repositories — the library would work and the application would
break. Having the service name both keeps the fact where a reader looks first.

**The alternative rejected here too:** having the library scan the common ancestor `com.dpe`. It
works, and it makes the library quietly responsible for packages it does not own, so a service that
later moves out from under `com.dpe` breaks with an invisible missing-repository error. Explicit
lists fail at the point of the mistake.

## Revisit if

- A fourth service needs a materially different relay (different batching, a different broker).
  At that point the shared relay is being bent rather than reused, and copying is cheaper.
- The library starts accumulating anything a service would describe as *behaviour* rather than
  *plumbing*. That is the signal it has become a shared domain.
