# ADR 0011 — Tests have no route to the running stack

**Status:** accepted (M8)
**Supersedes:** relying on `spring.kafka.listener.auto-startup=false` to keep test consumers quiet
**Related:** none

## Context

Every service's `application.yml` defaults its infrastructure addresses to `localhost` — the broker,
Postgres and, in the orchestrator, Redis. That is convenient: a service started from the IDE finds
the Compose stack, which publishes all three on `localhost`.

It also meant any test context without a Testcontainers connection for one of them talked to the
**live** one whenever the stack was up. During M8, one full `./mvnw verify` with the Compose stack
running joined the live `account-service` consumer group 18 times and the live
`payment-orchestrator` group 24 times.

A test consumer in a live group is not harmless. The group coordinator assigns it partitions, it
reads live commands, processes them against the *test* database, and commits their offsets. The
running service never sees those messages. No traffic was flowing at the time, and the next load
run's quiescence check (zero lag, nothing unpublished, no saga in flight) passed, so nothing is
believed lost — but that was timing, not design.

There were two causes, and the second is why the obvious fix does not work:

1. account-service's Postgres test base never set `spring.kafka.listener.auto-startup=false`.
2. The orchestrator's test base **did** set it, and its consumers started anyway. Spring Framework 7
   pauses cached test contexts and restarts them when a later test class reuses them
   (`spring.test.context.cache.pause`, default `ON_CONTEXT_SWITCH`). spring-kafka 4.1's listener
   registry starts **every** container on a `start()` that follows the context's refresh —
   `alwaysStartAfterRefresh` defaults to `true` — whatever `auto-startup` said. `auto-startup=false`
   only governs a context's *first* start. Both behaviours were confirmed from the bytecode
   (spring-framework 7.0.9, spring-kafka 4.1.1).

## Decision

**No test has a default that reaches anything.** Every module's test-classpath
`application.properties` points each piece of infrastructure at `127.0.0.1:9` — the discard port,
where nothing listens:

```properties
spring.kafka.bootstrap-servers=127.0.0.1:9
spring.kafka.admin.auto-create=false
spring.datasource.url=jdbc:postgresql://127.0.0.1:9/no-route-to-the-dev-stack
```

plus Redis in the orchestrator, the only service that uses it.

- A test base that starts a container supplies the real address through `@ServiceConnection`, which
  takes precedence over properties. Nothing about those tests changes.
- `spring.kafka.admin.auto-create=false`, because otherwise `KafkaAdmin` waits out its timeout
  against port 9 in every context. The Kafka test bases turn it back on.
- The values live in the test-classpath properties file rather than in any `@SpringBootTest`
  annotation, for a reason found at M4: `@SpringBootTest` properties do not merge down a class
  hierarchy, so a key set on a base class disappears from any subclass that declares its own.

The rule is general: **a test's safety must not depend on which of its components happen to start.**
Suppressing a component is a statement about one lifecycle path, and the framework has more of them
than the test author does. Removing the route is a statement about every path.

## Consequences

**A test that silently depended on the live stack now fails**, loudly, instead of passing on a
developer's machine and failing in CI. That is the point.

**Measured after the change:** a full verify with the stack up made zero contacts with the live
broker, and the restarted listeners can be seen in the test logs failing to reach port 9 instead of
joining a group. The same verify also ran faster (4:42 against 6:05). A plausible cause is that
contexts without Kafka no longer create topics or join groups at startup; it has not been isolated.

**The listeners still restart**, and now fail to connect. That is noise in the test log, accepted in
exchange for a guarantee that does not depend on it.

**account-service's Postgres test base also sets `auto-startup=false` now**, like the other two
services'. It no longer protects anything on its own, but it stops a context's first start from
spending time on consumers nobody asked for.

## Alternatives rejected

- **`auto-startup=false` everywhere.** Tried; see cause 2. It governs one lifecycle path.
- **`spring.test.context.cache.pause=never`.** Removes the restart that defeated `auto-startup`, and
  leaves every default address still pointing at the live stack. The next framework mechanism that
  starts a component would reopen the hole.
- **Unique consumer group ids per test run.** Protects the live group's offsets and still points a
  test consumer at the live broker, reading live traffic into a test database.
- **Moving the Compose stack to unusual host ports.** Moves the collision rather than removing it,
  and a service run from the IDE needs to find the stack.
