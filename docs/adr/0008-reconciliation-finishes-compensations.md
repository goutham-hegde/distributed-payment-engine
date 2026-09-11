# ADR 0008 — Reconciliation finishes a compensation; it never originates a movement

**Status:** accepted (M7)
**Supersedes:** nothing
**Related:** ADR 0002 (idempotency), ADR 0005 (JWT authentication — "an operator sees everything
and moves nothing")

## Context

The M7 chaos suite found five ways a saga could finish while a participant still held money for it,
and the fixes that followed (forward recovery after the gateway charge, truthful replies,
transfer-addressed compensations with tombstones, a sweeper that does not run while deaf) stop every
one of them from happening again.

They do not repair what already happened. A saga that ended `FAILED` or `COMPENSATED` is terminal,
the sweeper never claims it again, and the first chaos run had left behind:

- **26 holds still `ACTIVE`** under `FAILED` sagas — 110,000 paise of customers' money parked in
  CLEARING with nothing alive that would ever move it;
- **22 charges still `APPROVED`** at the PSP for transfers the customer had been told failed, whose
  holds had been released — refunded in our ledger, charged on their card.

The five ledger invariants passed throughout. Both conditions are only visible to checks that join
databases (S1 and S2 in `scripts/lib/stranded.sh`).

Two constraints shaped the answer. **No service may read another's database**, so no service can
compute the list of affected transfers. And **an operator moves no money** (ADR 0005): the
`OPERATOR` role reads everything and has no path to a ledger write, deliberately.

## Decision 1 — an endpoint on the orchestrator, per transfer

`POST /admin/transfers/{transferId}/reconcile`, operator-only by the existing `/admin/**` rule.

It is accepted only for a saga that is **terminal and not `COMPLETED`**. A `COMPLETED` saga is
refused with 409: there the recipient has the money and the PSP has the charge, both correctly, and
reversing either would be a movement a person chose. A live saga is refused with 409 too: the
sweeper owns it, and a person racing the sweeper helps nobody.

**Why this does not break "an operator moves no money".** That rule is about *originating* a
movement — choosing an amount, a source, a destination. This endpoint chooses none of them. It
applies only to a transfer the system has already declared did not happen, and it can only send the
compensation that verdict already implies, to the participants the transfer already named, through
the same idempotent commands the sweeper sends. The rule's purpose — a compromised operator token
cannot pay anyone — survives: the worst it can do is return a sender's own money for a transfer that
had already failed.

There is deliberately **no bulk form**. Which transfers need it is a three-database question, so the
list is built by an operator tool (`scripts/reconcile.sh`, which may join databases because it is
not a service) and each transfer is named individually.

## Decision 2 — sequenced: ask account-service, then void

The obvious implementation sends `ReleaseFunds` and `VoidCharge` together. It is wrong in exactly one
case, and that case is the one that costs money: a hold that turns out to have been **committed**.
The recipient then has the money, and voiding the charge pays them out of our own books.

The orchestrator cannot see holds, so it cannot tell. It asks first:

1. The request writes a transfer-addressed `ReleaseFunds(reason = RECONCILIATION)` and a
   `Reconcile STARTED` step, in one transaction.
2. account-service answers with what is true of the hold — the M7 rule that a reply is a statement
   of fact. It releases an `ACTIVE` hold, reports a `RELEASED` one, records a tombstone if there
   was never a hold, or reports a `COMMITTED` one.
3. The orchestrator acts on the answer:
   - `FundsReleased` or `ReserveRejected(TRANSFER_VOIDED)` — the sender is whole, so the PSP must
     hold nothing either: a `VoidCharge` follows (which reverses an approved charge, or leaves a
     tombstone that refuses a late one), and the step is `Reconcile SUCCEEDED`.
   - `FundsCommitted` — nothing more is sent. `Reconcile FAILED`, logged at ERROR. That transfer
     needs a person, not a compensation.

"Outstanding" is counted from the steps (STARTED against SUCCEEDED + FAILED) rather than stored in a
column: no migration, and the request and its answer can land in the same microsecond without the
order mattering. A reply that arrives with no request outstanding is handled exactly as before —
reconciliation is opt-in per transfer.

## Consequences

- Repeatable. Every command involved is idempotent at its participant, so a second request for the
  same transfer ends in the same state with a second pair of steps.
- The saga's status does not change; reconciliation is recorded as steps on the timeline, beside the
  history it corrects.
- **Known inaccuracy:** a hold released before account-service migration V7 has no stored release
  reason, so its truthful reply echoes the command's reason, and the step says "released
  (RECONCILIATION)" for a hold released long before. The money is right; the label is not.
- Run against the M7 leftovers: 48 requests, 48 × 202; 26 holds released (110,000 returned),
  22 charges reversed, 26 gateway tombstones left for transfers that were never charged; I1–I5 and
  S1–S4 all pass; a second run finds nothing to do.

## Alternatives rejected

- **A one-off SQL script.** Writes ledger rows by going around the only code that knows how to write
  balanced pairs — exactly how I1 and I2 can be broken (they are enforced only by that discipline).
- **Letting the sweeper re-claim terminal sagas.** Turns a one-off repair into a permanent load on
  the write path, and makes "terminal" mean "terminal unless".
- **A reconciliation service with access to all three databases.** The shared-database architecture
  reintroduced for the rarest operation in the system, as the most privileged component in it.
