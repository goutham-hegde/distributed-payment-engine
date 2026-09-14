# ADR 0009 — Admission control bounds work in flight; a bulkhead bounds requests

**Status:** accepted (M8). Limit re-derived twice since; still 150.
**Supersedes:** nothing
**Related:** ADR 0001 (orchestration — the saga deadline), ADR 0002 (idempotency — the claim this
check sits behind)

> The saga deadline this record calls 30 s became **60 s at M10** ([ADR 0013](0013-kubernetes.md),
> decision 11). The measurements below were taken at 30 s and are left as they were; the admission
> limit is sized against a 10 s wait target, not against the deadline, and stays 150. Also since
> M8: chaos scenario 06 with Redis off answers 46 of 100 duplicates with this bulkhead's 503 — see
> `progress.md` Session 25, part 2.

## Context

`POST /transfers` answers `202 Accepted` as soon as three rows commit — the transfer, the saga and
the `ReserveFunds` outbox row. Money moves later, through the saga. Nothing on the request path
could see that pipeline.

The first load test (M8, open model, 5 → 40 arrivals/s) found out what that costs. The pipeline
settled about 15 transfers/s; the edge accepted 20–26/s, with **zero HTTP errors in 248,628
requests**. The difference piled up as outbox rows and consumer lag, sagas crossed their 30 s
deadline, and the sweeper compensated **1,401 payments that had never failed — they had waited**.
Then it got worse:

```
                   31:20 31:40 32:00 32:20 32:40 33:00 33:20 33:40 34:00 34:20
GET poll/s           144   237   452   569   843  1131  1424  1531  1616  1857
settled/s             17     8    14    16    14    17    21     5     1     5
orch pool waiting      0     0     0     0     0     0    59   187   187   145
```

Customers poll until their payment settles, so slow settlement means more polling. Status reads
rose to 97% of all traffic and took all ten of the orchestrator's database connections, which the
outbox relay and the reply listener draw from too. With nothing sent and nothing read, nothing
completed, and every timeout added compensation messages to the starved pipeline. Arrivals fell to
1.5/s and still every one timed out. That is a **metastable failure**: the system's own recovery
work and its clients' reaction to its slowness kept it down after the load that caused it was gone.

All five ledger invariants held throughout. Every paisa was conserved; customers were just told
"failed" about payments that would have succeeded.

Two separate defects are visible in it, and they need two separate fixes.

## Decision 1 — admission control: bound sagas in flight

`AdmissionControl` refuses **new** transfers while `saga_instances` already holds
`max-in-flight` non-terminal sagas.

**Why in flight, not requests per second.** Little's law: *in-flight = throughput × time in
system*. At a fixed throughput, a bound on the number in flight *is* a bound on how long an admitted
payment waits — the quantity the 30 s deadline cares about. A rate limit bounds arrivals and knows
nothing about whether the pipeline is keeping up: a gateway running at half speed sails under it
while the queue grows exactly as before. Counted from the database, the bound is global across
instances.

**Where it is checked: after the idempotency claim, never before it.** Only new work reaches the
check. A retry of a payment that was already accepted is replayed with its original 202 at any
load. A 503 says "this payment was not accepted", and saying it about one that *was* invites the
client to pay again under a fresh key. The refusal throws inside the claim's transaction, so the
key is rolled back and stays usable.

**What the client sees.** `503` with `Retry-After` and code `AT_CAPACITY`, and a body telling the
client to retry with the *same* `Idempotency-Key`. `503` rather than `429`, because the refusal is
about server state, not the client's behaviour.

**How it counts.** At most once per second, lazily, by whichever admission arrives first after the
count goes stale, inside that request's own transaction — no query per request, no extra
connection, no scheduler thread. Between counts the estimate is the last count plus this instance's
admissions since, which errs toward refusing early. It is a soft bound, and its overshoots are
bounded: at most one per request permit on one instance, and N instances can together overshoot by
N × one second's admissions.

## Decision 2 — the limit is 150, and it is a measurement

The target is a worst-case wait of about a third of the deadline (10 s), so no healthy saga comes
near the sweeper.

| When | Arithmetic | Measured | Kept |
|---|---|---|---|
| First derivation, knee ~15/s | 15/s × 10 s = 150 | worst window p99 13.3 s, 5,741 of 5,741 completed | 150 |
| After the gateway and reply consumers were lifted, knee ~35/s | 30/s × 10 s = 300 | at 300: worst p99 **19.9 s** for **+1.7%** completions; at 150: 9.1 s | **150** |
| After CLEARING was sharded and the JVMs rebudgeted, knee ~38/s | — | run to 60/s: plateau at the gateway's ceiling, in-flight 148, p99 ≤ 5.2 s | 150 |

The arithmetic overshot for two reasons, and both generalise:

- **Little's law gives the mean.** The 10 s target is a tail target; the mean has to be aimed lower.
- **Capacity is not a constant — it falls as the queue deepens.** Every waiting customer polls, so
  reads scale with the number in flight (46/s at low load, ~500/s at 286), into a Postgres all
  three services share. account-service's time per command went from 11 ms to 18–23 ms. Throughput
  at 286 in flight was ~22/s, against 36/s at ~110. The number to divide by is the throughput
  measured *at the queue depth the limit allows*, not the peak.

So the limit is an upper bound on queue, not a throughput knob. Once the bottleneck is saturated,
raising it buys waiting and almost nothing else. Its floor is capacity × the unloaded settle time
(~2 s), about 60 here; below that it starves the bottleneck.

## Decision 3 — a request bulkhead in front of the connection pool

A fair semaphore on `/api/*` and `/admin/*`, placed just after Spring Security (so an
unauthenticated request is answered 401 without spending a permit): **6 permits against a pool of
12**. A request that cannot get a permit within 1 s is answered 503.

The guarantee is arithmetic. Each request holds at most one connection at a time, so six permits
hold at most six, and the other six are reserved for what must never queue behind reads: the
scheduler thread (relay, sweeper and metrics share Boot's single default scheduler thread), three
reply-listener threads, the dead-letter listener and the health check. `BulkheadConfig` refuses to
boot unless `permits + 3 + reply threads ≤ pool`. Its first version checked only `permits < pool`,
which kept passing when the reply listener went from one thread to three and the reserve was two
connections short.

**Why a semaphore and not two connection pools.** Routing reads and the pipeline to separate Hikari
pools means defining `DataSource` beans, and in Spring Boot 4 that switches off the connection-
details auto-configuration — including `@ServiceConnection` in every Testcontainers test. The
semaphore gives the same isolation with one pool.

## Evidence: three runs of the same knee profile, one fix switched off at a time

| | Neither | Bulkhead only | Both |
|---|---|---|---|
| Accepted | 5,382 | 6,211 | 5,741 |
| Completed | 3,943 (73%) | 4,554 (73%) | **5,741 (100%)** |
| Compensated or failed after acceptance | 1,439 | 1,657 | **0** |
| Worst per-window settle p99 | 91.6 s | 59.9 s | **13.3 s** |
| Settled/s at 40/s offered | ~3 | ~2 | **15.5–17.8** |
| Orchestrator pool, threads waiting | 187 | 0 | 0 |
| Refused up front | — | — | 14,495 responses |
| I1–I5, S1–S4 | pass | pass | pass |

- **Admission control is what prevents the collapse.** In-flight flattened at the limit and
  throughput held at the pipeline's capacity under twice the load it can carry.
- **The bulkhead removes the starvation, and on its own that is not enough.** No thread queued for a
  connection and the reply listener kept up, but with nothing bounding accepted work the queue still
  outgrew the deadline. *Bounding concurrency does not bound work.*

At M8's final 1,000-user run at twice capacity: 17,974 of 17,974 accepted payments settled, server-
side settle p99 ≤ 5.4 s in every window, and the excess was refused as 48,345 503s.

## Consequences

- **Overload is pushed back to the client, visibly.** At 2× capacity, 22.7% of payments were
  abandoned after five refusals. That is the honest outcome: a latency target cannot be met at twice
  capacity without more capacity, and "not accepted, try later" is a better answer than "accepted"
  followed by a timeout 30 s later.
- **The limit rots when throughput changes.** Faster, and it wastes capacity; slower, and it admits
  more than can finish in time, which the sweeper then compensates. It is re-derived with a knee run
  whenever the pipeline's throughput changes, in either direction — never by arithmetic alone.
- **It is not load-bearing for correctness.** Money is protected by the ledger's constraints; this
  only decides how much work is let in. Switching it off (`DPE_ADMISSION_ENABLED=false`) makes the
  system slower under overload, never wrong — the ablation run above is the proof.
- **No gauge for the estimate.** It is recounted lazily, so on an idle system it would sit at its
  last value indefinitely. `dpe.saga.inflight` is the refreshed number.
- **The bulkhead's reserve is spent by anything that takes a second connection** — `REQUIRES_NEW`,
  a query outside the open transaction, a new thread that touches the database. A controller mapped
  outside `/api/*` and `/admin/*` is not covered until added to the filter's registration.

## Alternatives rejected

- **A bigger connection pool.** The pool ran out two minutes after the knee: a consequence, not the
  trigger. A bigger one lets the status reads take more connections.
- **A rate limit at the edge.** Bounds arrivals, not work; blind to a slow downstream.
- **Checking capacity before the idempotency gate**, to shed earlier. Answers "not accepted" about
  accepted payments (Decision 1).
- **A longer saga deadline.** Moves the collapse later and makes every stuck payment wait longer to
  be told.
