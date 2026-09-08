# ADR 0002 — Idempotency is a Postgres constraint; Redis is a cache in front of it

**Status:** accepted (M4)
**Supersedes:** the original brief, which specified "Redis distributed locks to prevent duplicate
charges"
**Related:** ADR 0004 (shared messaging library) — the inbox solves the same-shaped problem one
layer down

## Context

A client that does not receive a response cannot tell what happened. A timeout is
indistinguishable from success: the request may have been lost on the way out, or the response
lost on the way back, and no amount of client-side cleverness separates those two. So the client
retries, correctly, and the server has to make the retry harmless.

This is the single most consequential decision in the project, because it is the one where the
obvious answer is wrong in a way that only shows up under the conditions you built it for.

The endpoint at issue is `POST /api/v1/transfers`, and the failure it must prevent is Bob being
paid twice.

## Options considered

**1. A Redis distributed lock around the handler (the original brief).** Acquire a lock keyed on
the idempotency key, do the work, release. This is what most tutorials show and it is what the
brief asked for. Rejected — see below.

**2. `SELECT` the key, and `INSERT` if absent.** Rejected immediately. There is a window between
the two statements, and a retry storm — the exact condition this code exists to handle — is a
machine for finding it. Two requests read "absent", both proceed, and the check has bought
nothing but the belief that it worked.

**3. A unique constraint, claimed by the same transaction that does the work.** Chosen.

**4. Chosen, plus Redis in front as a cache and an anti-stampede lock.** Chosen — with Redis
placed where its failure mode is harmless.

## Why not the Redis lock

Redlock's safety rests on assumptions about time. Martin Kleppmann's critique is the canonical
statement of the problem, and it does not require a Redis failure at all:

```
1. Process A acquires the lock, TTL 10s.
2. Process A hits a stop-the-world GC pause of 15s.
3. The lock expires. Redis hands it to process B — correctly, by its own rules.
4. Process B begins charging the card.
5. Process A wakes, still believing it holds the lock, and also charges the card.
```

Nothing broke. Redis did exactly what it promised. The flaw is structural: **a lease measured in
time cannot protect a resource that does not check the lease.** The same argument runs from clock
drift between nodes, from a long network stall, from a paused VM.

For a cache stampede that risk is fine — the worst case is duplicated work. For money it is not.
The failure is silent, produces a real double payment, and is discovered at reconciliation.

The deeper point is that a lock is the wrong shape of tool. A lock asks "may I proceed?", which is
a question about *now*. Idempotency asks "has this intent already been carried out?", which is a
question about the past, and the past belongs in a durable store.

## Decision

**The guarantee is `PRIMARY KEY (client_id, idempotency_key)` on `idempotency_records`.**

The row is claimed with `INSERT ... ON CONFLICT DO NOTHING` **in the same transaction that writes
the transfer, the saga instance and the `ReserveFunds` outbox row.** One commit. The claim and the
work are the same fact, and there is no window in which one exists without the other:

- claimed, work rolled back → the claim rolled back with it; the key is free and a corrected
  retry is honoured;
- work committed, claim lost → impossible, they are one commit.

`client_id` is part of the key so keys live in per-client namespaces. Without it, a common key
like `"1"` collides across tenants — and a collision does not merely fail, it replays one
client's response body to another.

The request is fingerprinted (SHA-256 of the canonical fields) and the fingerprint is compared on
every replay path. A key reused with a different body is `409`, never a replay: answering the
second request with the first's receipt tells a client its payment succeeded when what exists is
somebody else's amount.

### What the constraint gives that the lock was wanted for

This is the part worth understanding, because it is why nothing is lost by dropping the lock.

When a duplicate arrives while the first request's transaction is still open, the second
`INSERT` does **not** fail and does not proceed. Postgres finds an uncommitted index tuple for the
same key and **blocks the second inserter until the first transaction ends**:

- first commits → the second sees the conflict, reads the stored response, replays it;
- first rolls back → the tuple never existed, the second's insert succeeds, and it does the work
  itself.

That is mutual exclusion between concurrent duplicates, with correct handoff on failure, out of an
index. No lease, no TTL, no assumption about clocks or GC pauses. The database was always going to
be in the transaction; the exclusion is free.

### Where Redis goes

Still used, and still worth building — placed where its failure mode is harmless:

| Component | Role | Is it the guarantee? |
|---|---|---|
| Postgres `PRIMARY KEY` | source of truth for "have I seen this intent?" | **Yes.** ACID, no clock assumptions. |
| Redis response cache | answers retries without a database transaction | No. |
| Redis `SET NX PX` lock | keeps a retry storm from all reaching Postgres at once | No. |

Rules the implementation holds to:

1. **Every Redis failure degrades to a miss.** Unreachable, slow, full, unparseable — all mean
   "no answer", never an exception. A cache whose exceptions propagate does not degrade, it
   *amplifies*: the system then fails when either component is down, and adding a component to
   make it faster has made it less available.
2. **The cache is written after commit**, never before. Caching a response that then rolls back is
   the dual-write bug in new clothes, and the fast path would serve that lie to every retry.
3. **The cached entry always has a TTL**, no longer than the row's retention. A cache that outlives
   its row becomes the authority on a promise the database has stopped keeping.
4. **A lock that cannot be acquired is advice, not a refusal.** The caller waits briefly for the
   winner's answer and then goes to Postgres regardless.
5. **The lock is released by compare-and-delete in Lua**, never a plain `DEL`. A stalled holder
   whose lease expired would otherwise delete its *successor's* lock — the same Kleppmann failure,
   one level down, inside the optimization.

## Consequences

**The claim can be tested rather than asserted.** `IdempotencyWithoutRedisTest` enables the cache
and points it at a dead port: 100 concurrent identical requests still produce exactly one transfer.
The anti-stampede lock is never acquired even once, and the result does not change — because the
exclusion was never Redis's to provide. Operationally, `docker compose stop redis` is a supported
state.

**Idempotency is now a published part of the API contract**, not an implementation detail:
`Idempotency-Key` is required on `POST /transfers` (a money-movement request that cannot be
retried safely is not one we should accept), keys are honoured for 24 hours, and
`Idempotency-Replayed` on the response says which requests were retries.

**Accepted cost: a row per write request.** `idempotency_records` grows with traffic and needs the
expiry sweep to be running. A key that has expired is a key that will be executed again, which is
correct only because 24 hours is far beyond any client's retry budget — it is a policy number, and
it is in `application.yml` rather than in a migration for that reason.

**Accepted cost: failures do not burn the key.** A validation failure rolls back the claim, so the
same key can be retried with a corrected body. Caching failures (as Stripe does) requires claiming
the key in a *separate* transaction that survives the rollback, plus a decision about what to
answer a duplicate that arrives mid-flight. That is a bigger machine than this API needs, and the
smaller one was chosen deliberately rather than by omission.

**What this buys in an interview.** "How do you prevent double charges?" — a unique constraint on
`(client_id, idempotency_key)`, claimed in the same transaction as the work. Not a Redis
distributed lock, because Redlock's safety depends on bounded clock drift and bounded GC pauses,
and neither is a safe assumption for money. Redis is a fast path; delete it and the system is
still correct.

## Revisit if

- **The write rate outgrows one Postgres primary.** The constraint is per-database; sharding by
  `client_id` keeps it, sharding any other way does not.
- **A caller needs failures to be idempotent too** — that forces the separate-transaction claim
  and an explicit `IN_PROGRESS` state.
- **An endpoint appears whose work is not a local commit** (a synchronous call to an external
  PSP, say). The single-transaction argument above depends on the work being local, and that
  endpoint needs the two-phase record this one does not.
