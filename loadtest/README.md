# Load test

k6 against the running Compose stack, judged the same way the chaos suite is: by the invariants
once the system is at rest, not by the HTTP status codes.

```bash
docker compose -f infra/docker-compose.yml up -d
./loadtest/run.sh                                       # PROFILE=smoke: 5 customers, 30 s
PROFILE=knee  ./loadtest/run.sh                         # stepped arrival rate, 5 -> 40 /s
PROFILE=users ./loadtest/run.sh                         # 1,000 concurrent customers
PROFILE=users THINK_MIN=5 THINK_MAX=15 ./loadtest/run.sh   # the same thousand, six times busier
```

Exit status: `0` correct and within SLO, `1` an invariant refuted, `2` refused to start (system not
at rest, or another chaos/load run holds the lock), `3` correct but the latency SLO was missed.
Each run writes `loadtest/results/<id>/` - the full console log, k6's summary JSON and HTML report,
the seeded accounts, and container CPU/memory sampled every 5 s.

## Why the API's latency is not the answer

`POST /api/v1/transfers` answers `202 Accepted` once the transfer, the saga and the first command's
outbox row are committed. No money has moved yet. So the edge's latency describes the edge, and the
customer's actual wait is the saga: reserve, charge, commit, across three services and six topic
hops. Every iteration therefore does what a customer does - pay, then poll until the payment settles
- and the run reports two latencies:

| Metric | Measures | Source |
|---|---|---|
| `http_req_duration{op:create}` | the edge: accept latency | k6 |
| `transfer_settle_ms` | what the customer waits, including polling | k6 |
| accept-to-terminal per 30 s window | the same, exactly, for every saga started | `saga_instances`, after the run |

When the asynchronous half falls behind, the first stays flat while the other two climb. That gap is
what the test exists to find.

## The profiles

**`knee` - open model.** Customers arrive on a schedule (`STAGES="rate:seconds,..."`) whether or not
the last ones have been served. This is how a payment API is loaded in reality, and the only model
that measures latency honestly: a closed model slows its own arrivals down when the system slows
down, and so under-reports exactly the latency it is supposed to catch (coordinated omission). Its
job is to find the throughput at which the per-window settle time stops being flat.

**`users` - closed model.** `VUS` concurrent customers (default 1,000), each paying, waiting for the
payment to settle, then thinking `THINK_MIN`-`THINK_MAX` seconds. "1,000 concurrent" is a statement
about sessions, not throughput. Little's law converts it: offered load is roughly
`VUS / (think + settle)`, so the default 30-90 s think time offers about 16 payments/s, and 5-15 s
offers about 100/s. Same thousand users, different system - always quote the think time with the
user count.

A small fraction of payments (`RETRY_RATE`, default 2%) is re-sent with the same `Idempotency-Key`,
as a client retrying a response it never received. Each must come back as the original transfer,
marked `Idempotency-Replayed: true`; `replay_consistent` is a threshold at exactly 100%.

## What the runner does

It sources `chaos/lib.sh` and follows the same skeleton as a chaos scenario, because a load run
is a chaos scenario with no fault:

1. take the chaos lock, reset the gateway's fault knobs, refuse to start unless quiescent;
2. open and fund 200 accounts, then wait until the orchestrator's ownership projection has all of
   them (a transfer from an account it has not heard of yet is a 403);
3. record the I3 baseline - after funding, which is money entering the ledger;
4. run k6 inside the Compose network, so it measures the services rather than Docker Desktop's
   host port forwarder;
5. wait for the pipeline to drain (up to `DRAIN_TIMEOUT`, 900 s) - under overload, how long that
   takes is part of the result;
6. report outcomes, per-window settle percentiles from `saga_instances`, Prometheus peaks (outbox
   backlog and age, consumer lag, pool waits, GC), and container CPU;
7. run I1-I5 and S1-S4 (new violations only).
