# ADR 0010 — CLEARING is sharded, and each hold records its shard

**Status:** accepted (M8, account-service migration `V8__sharded_clearing.sql`)
**Supersedes:** the single-CLEARING-account assumption of `V3__holds_and_inbox.sql`
**Related:** ADR 0001 (the saga — reserve, commit, release)

## Context

Money in flight lives in a CLEARING account (M3). A reserve debits the sender and credits CLEARING;
a commit debits CLEARING and credits the recipient; a release debits CLEARING and credits the sender
back. This is what keeps every ledger invariant true while a transfer is half-done: there is no such
place as "in transit" in double-entry unless one is made.

It also made one row that every saga transaction in the system writes. Each reserve, commit and
release locks the CLEARING row with `SELECT … FOR UPDATE` and holds that lock to commit — through
the ledger inserts, the hold, the inbox and outbox rows and the WAL flush. However many threads
account-service ran, its money-moving work ran one transaction at a time.

The load test found it once the stages in front of it were lifted: account-service's single
consumer was the knee (~35 transfers/s, 90–99% busy). Before changing anything, the hypothesis was
measured — account-service given three consumers with still one CLEARING row, and a sampler reading
`pg_locks` and `pg_stat_activity` during a knee run:

| | One CLEARING row | Eight shards |
|---|---|---|
| Sessions waiting on an account-row lock (mean, of active) | **1.24 of 2.03** | 0.33 of 1.78 |
| …of which waiting on CLEARING | **≥ 0.81** | 0.08 |
| Settle p99 in the ~30/s windows | 4.0–4.7 s | **2.8–3.1 s** |
| Completed | 7,852 of 7,852 | 7,944 of 7,944 |
| I1–I5, S1–S4 | pass | pass |

Roughly one thread ran while the others queued, mostly on CLEARING.

## Options considered

**1. A derived CLEARING balance** — stop maintaining the column and compute it from ledger entries.
Removes the row lock, and changes what invariant I2 (every balance equals the sum of its entries)
means for exactly the account where it matters most.

**2. Lock CLEARING last**, in a new global lock order, to shorten how long it is held. A change to
the deadlock-avoidance rule every money-moving path depends on, and it still serializes: the lock is
held until commit, and the commit includes the WAL flush.

**3. Shard it.** N CLEARING accounts; each transfer parks its money in one of them. Chosen.

## Decision

**Eight CLEARING accounts, as rows.** The original keeps its id (`…0002`) and becomes shard 0, so
every hold and ledger entry written before the migration still names a real account; the others are
`…0021` to `…0027`. The count is data, not configuration. With three consumer threads, two
concurrent transfers meet on one shard 1 time in 8 instead of every time.

Nothing about the accounting changes, because nothing ever needed "the" clearing balance — only
that money in flight is somewhere, and counted:

- **I1** — every step still writes a matching pair of entries.
- **I2** — each shard is an ordinary account whose balance is the sum of its own entries.
- **I3** — sums customer balances and `ACTIVE` holds; the shards are in neither, as CLEARING was.

**The shard is chosen once, at reserve, and recorded on the hold.** A reserve picks
`floorMod(transferId.hashCode(), shards)` among the CLEARING accounts in the transfer's currency and
writes it to `holds.clearing_account_id`. Commit and release **read it from the hold**. They never
recompute it.

This is the load-bearing part of the decision. Commit and release both write the identical ledger
leg `(transfer_id, clearing_account, DEBIT)`, and a UNIQUE constraint on it is what makes completing
and compensating the same transfer mutually exclusive — structurally, not by the orchestrator's
promise. That holds only if both find the *same* clearing account. A recomputed shard is the same
shard only while the list of shards never changes: add one, and every hold reserved before the change
settles against an account it was never parked in. I1 would not notice (the legs still balance); one
shard would drift positive and another negative forever, and the commit/release exclusion would
silently stop covering those transfers. *A decision that has to come out the same at two moments is
stored, not re-derived.*

**The database guarantees a hold is parked in a CLEARING account.** A plain foreign key would accept
a customer account, whose balance would then carry someone else's money in flight. The key is
composite: `(clearing_account_id, clearing_account_type) → accounts (id, account_type)`, with the
type column pinned to `'CLEARING'` by a CHECK.

**Existing holds were backfilled** to shard 0, then the column made `NOT NULL` with no default. A
default would quietly supply shard 0 to any future insert that forgot the column — the
recompute-instead-of-record bug in another form.

**A currency with no CLEARING account is refused as `CURRENCY_MISMATCH`, before any lock** — the
answer the reserve already gave when the single CLEARING account had a different currency.

**account-service runs three consumers**, one per partition of its command topic. Only worth it
once CLEARING stopped serializing every transaction.

## Consequences

**There is no "the" clearing account any more.** `AccountType.CLEARING_ACCOUNT_ID` is shard 0 only.
Totals sum `account_type = 'CLEARING'`.

**The invariant checks got stronger, because they had to.** `LedgerInvariants.assertAll` now checks,
per shard, that the shard's balance equals the `ACTIVE` holds naming it. A hold settled against the
wrong shard leaves the total right and two shards wrong in opposite directions, and neither I1 nor I2
can see that. When the commit was mutated to recompute the shard, the test did fail — but on
`accounts_customer_balance_non_negative`, which despite its name is
`balance_minor >= 0 OR account_type = 'SYSTEM'` and so covers CLEARING: the wrong shard happened to be
empty. Under load it would usually hold other transfers' money and the debit would succeed. The
per-shard identity is the check that catches that case.

**The lock order is now exercised for real, and it has to be global.** With three consumers,
transactions genuinely interleave. Every path locks accounts in one order: `UUID.compareTo`, which
compares *signed* halves, so about half of random customer ids sort below the shards — "the shard is
locked first" is not true and nothing may assume it. The concurrency test is a **ring** (A pays B,
B pays C, C pays A, 45 transfers in parallel), because with disjoint senders and recipients a
per-role order (sender, then clearing, then recipient) happens to be global and passes by accident.
Mutated to a per-role order, the ring test fails with 9 deadlocks reported by Postgres; with the
global order it passes. Chaos scenario 7 (one hot sender) passed after the change: exactly 60
completed, 30 refused, no deadlocks in the Postgres log.

**Adding shards later is a data change** (insert rows; no code change), and safe precisely because
holds record theirs. Removing a shard is not safe while any `ACTIVE` hold names it.

## Revisit if

- account-service concurrency grows well past eight. Pairwise collisions rise with the square of the
  number of concurrent transfers; add shards before adding threads.
- A second currency is introduced. Each currency needs its own shards (the lookup is per currency),
  and a currency with none refuses every reserve.
