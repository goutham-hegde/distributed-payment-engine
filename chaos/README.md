# Chaos suite

Each scenario states a hypothesis about behaviour under one injected fault, injects it against the
running Compose stack, waits for the system to settle, and then checks it. A scenario that does not
end by asserting the invariants proves nothing, so every one of them ends in
`scripts/verify-invariants.sh`, plus four checks the five invariants cannot make on their own.

```bash
docker compose -f infra/docker-compose.yml up -d
./chaos/04-gateway-declines-everything.sh     # one scenario
./chaos/run-all.sh                            # all of them, sequentially

# The same suite against Apache Kafka instead of Redpanda. The harness detects which broker is
# running (BROKER=redpanda|kafka) and kills, pauses and reads lag from that one.
docker compose -f infra/docker-compose.yml -f infra/docker-compose.kafka.yml up -d
./chaos/01-broker-dies.sh
```

Exit status is `0` if the hypothesis held, `1` if it was refuted, and `2` if the scenario refused to
start. It refuses when the system is not quiescent: a run that inherits in-flight sagas from the
previous one is asserting about both faults at once.

**Quiescent** means three things at once: no saga in a non-terminal state, no unpublished row in any
of the three outboxes, and zero consumer lag in all three consumer groups. The third was added after
a consumer silently stopped fetching and the system met the first two with twelve commands unread in
a topic.

## The scenarios

| # | Fault | Hypothesis |
|---|---|---|
| 1 | Broker killed while transfers are accepted (`OUTAGE=15`, or `45` to outlast the saga deadline) | API keeps accepting; commands wait in the outbox and publish on recovery |
| 2 | account-service SIGKILLed past the saga deadline (`WHEN=after-charge` or `before-reserve`) | The sweeper times the saga out and the system converges |
| 3 | Orchestrator killed with committed, unpublished outbox rows (broker paused to widen the window) | The restarted relay publishes every stranded command, once |
| 4 | Gateway declines 100% | Every transfer compensated; each sender restored exactly, per account |
| 5 | Gateway sends every callback twice; then goes silent and its dead letters are replayed | No double credit; a silent PSP is compensated |
| 6 | One `Idempotency-Key` sent 100 times concurrently (`REDIS=off` removes the lock) | Exactly one transfer; every caller told about it |
| 7 | 90 concurrent transfers from an account with room for 60 | Exactly 60 complete, 30 refused, balance exactly zero, no deadlocks |
| 8 | account-service cut off **inside an open transaction** (the cut is aimed: pause, check `pg_locks`, cut). `MODE=crash` also kills it while cut off | The orphaned transaction is reaped by Postgres within 30 s; recovers without a hand-terminated backend |

## Beyond the five invariants

The five invariants are statements about one database at a time, and some faults do their damage
where none of them look. After quiescence the harness also checks:

| Check | What it catches |
|---|---|
| **S1** no `ACTIVE` hold | Money parked in CLEARING with no live saga left to move it. I3 counts held money as conserved - correctly, mid-run - which is exactly why a hold stranded forever is invisible to it. |
| **S2** every approved PSP charge belongs to a `COMPLETED` transfer | The customer refunded in our ledger while the card network kept the charge. A `VOIDED` charge is not approved - the PSP holds nothing for it. |
| **S3** every `COMPLETED` transfer has an approved charge | Money moved without the PSP agreeing. |
| **S4** no transfer `PENDING` under a terminal saga | A customer told "processing" about something that has finished. |

A violation of S1–S4 persists until somebody reconciles it, so each scenario snapshots the violations
present when it starts and fails only on new ones; inherited ones are printed as a note, never hidden.

The queries live in `scripts/lib/stranded.sh`. `scripts/verify-invariants.sh` runs them too, as
absolute checks (the harness calls it with `--no-stranded` and judges them itself), and
`scripts/reconcile.sh` turns S1 and S2 into work: for each transfer it calls
`POST /admin/transfers/{id}/reconcile`, which finishes the compensation of a transfer that already
ended FAILED or COMPENSATED. It is a dry run without `--apply`.

The harness may join databases where a service may not: it is an operator's tool holding a superuser
connection for the length of a test, not a component on the payment path.

## Rules the harness enforces

- **One scenario at a time.** `begin_scenario` takes `chaos/.lock` and a second scenario exits 2.
  Two harnesses on one stack do not error - they baseline, pause and cut under each other and
  report plausible, wrong verdicts. A lock whose pid is no longer alive is treated as stale.
- **Wait for terminal sagas before restoring a fault.** The API answers `202` before the gateway is
  reached, so restoring straight after the last POST tests a system that has already recovered.
- **Record the I3 baseline after the accounts are opened.** Funding an account issues new money.
- **A fault injector must fail loudly.** `gateway_set` requires a 200 and logs the knobs the gateway
  reports *after* the call. The first version warned and carried on; see `progress.md`.
- **Assert the outcome the design promises, not only that nothing contradicts it.** S1–S4 and I1–I5
  pass for a saga that compensated when it should have completed. Scenario 2 therefore asserts
  `COMPLETED` for the timing after the gateway charge and `FAILED`-with-the-sender-whole for the
  timing before it; scenario 3 asserts nothing timed out while the orchestrator was deaf; scenario 5
  asserts the dead-letter replay charged nobody.
