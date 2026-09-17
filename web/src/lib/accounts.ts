import type { PoolClient } from "pg";
import { randomUUID } from "crypto";
import { emit, shardFor } from "./events";
import { CLEARING_SHARDS, CURRENCY } from "./session";

/**
 * The money. Everything in this file runs inside the caller's transaction, alongside the inbox row
 * that records having consumed the message - so the work and the record of having done it commit
 * together.
 *
 * The three operations write these legs, and nothing else ever touches a balance:
 *
 *   reserve   DEBIT sender   / CREDIT clearing    hold ACTIVE
 *   commit    DEBIT clearing / CREDIT recipient   hold COMMITTED
 *   release   DEBIT clearing / CREDIT sender      hold RELEASED
 *
 * Commit and release write the IDENTICAL leg - (transfer, clearing, DEBIT) - so the unique
 * constraint makes completing and compensating one payment mutually exclusive in the schema,
 * rather than by the orchestrator's promise.
 */

type Row = Record<string, any>;

/**
 * A transfer-scoped advisory lock, taken before any row lock.
 *
 * The reserve path and the release path read different tables in opposite orders (reserve: void
 * then hold; release: hold then void), so no unique constraint spans them. Message ordering makes
 * the interleaving rare, not impossible. This makes it impossible.
 */
async function lockTransfer(c: PoolClient, transferId: string): Promise<void> {
  await c.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [transferId]);
}

/** Locks accounts in one global id order, so deadlock is structurally impossible, not merely detected. */
async function lockAccounts(c: PoolClient, ids: string[]): Promise<Map<string, Row>> {
  const out = new Map<string, Row>();
  for (const id of [...new Set(ids)].sort()) {
    const { rows } = await c.query("SELECT * FROM accounts WHERE id = $1 FOR UPDATE", [id]);
    if (rows[0]) out.set(id, rows[0]);
  }
  return out;
}

async function postLeg(
  c: PoolClient,
  sessionId: string,
  transferId: string,
  accountId: string,
  amount: number,
  type: "DEBIT" | "CREDIT"
): Promise<void> {
  await c.query(
    `INSERT INTO ledger_entries (session_id, transfer_id, account_id, amount_minor, entry_type, currency)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [sessionId, transferId, accountId, type === "DEBIT" ? -amount : amount, type, CURRENCY]
  );
  await c.query(
    "UPDATE accounts SET balance_minor = balance_minor + $1 WHERE id = $2",
    [type === "DEBIT" ? -amount : amount, accountId]
  );
}

/**
 * A participant must always answer, even when it will not act.
 *
 * Replying with silence to a command about an already-settled hold leaves the saga able to learn
 * only from its own deadline - and by then the deadline may already have passed. A COMMITTED hold
 * answers FundsCommitted whichever command asked; a RELEASED one answers FundsReleased. A reply is
 * a statement of fact, not an acknowledgement.
 */
async function answerWithWhatHappened(
  c: PoolClient,
  sessionId: string,
  hold: Row
): Promise<void> {
  if (hold.status === "COMMITTED") {
    await emit(c, sessionId, hold.transfer_id, "FundsCommitted", {
      transferId: hold.transfer_id,
      holdId: hold.id,
      note: "hold was already committed",
    });
  } else if (hold.status === "RELEASED") {
    await emit(c, sessionId, hold.transfer_id, "FundsReleased", {
      transferId: hold.transfer_id,
      holdId: hold.id,
      reason: hold.release_reason ?? "ALREADY_RELEASED",
    });
  }
}

export async function reserve(
  c: PoolClient,
  sessionId: string,
  p: { transferId: string; fromAccountId: string; toAccountId: string; amountMinor: number }
): Promise<void> {
  await lockTransfer(c, p.transferId);

  // A compensation may have arrived before this reserve did, and left a tombstone.
  const voided = await c.query("SELECT 1 FROM transfer_voids WHERE transfer_id = $1", [p.transferId]);
  if (voided.rowCount) {
    await emit(c, sessionId, p.transferId, "ReserveRejected", {
      transferId: p.transferId,
      reason: "TRANSFER_VOIDED",
    });
    return;
  }

  const existing = await c.query("SELECT * FROM holds WHERE transfer_id = $1", [p.transferId]);
  if (existing.rowCount) {
    await answerWithWhatHappened(c, sessionId, existing.rows[0]);
    return;
  }

  const shard = shardFor(p.transferId, CLEARING_SHARDS);
  const { rows: shardRows } = await c.query(
    `SELECT id FROM accounts WHERE session_id = $1 AND account_type = 'CLEARING' AND owner_id = $2`,
    [sessionId, `clearing-${shard}`]
  );
  const clearingId: string = shardRows[0].id;

  const locked = await lockAccounts(c, [p.fromAccountId, clearingId]);
  const sender = locked.get(p.fromAccountId);

  if (!sender) {
    await emit(c, sessionId, p.transferId, "ReserveRejected", {
      transferId: p.transferId,
      reason: "NO_SUCH_ACCOUNT",
    });
    return;
  }

  // Insufficient funds is a BUSINESS failure, so it commits its reply. Throwing here would roll
  // back the inbox row with it, the message would be redelivered, and we would have built an
  // infinite retry loop around a condition that is never going to change.
  if (Number(sender.balance_minor) < p.amountMinor) {
    await emit(c, sessionId, p.transferId, "ReserveRejected", {
      transferId: p.transferId,
      reason: "INSUFFICIENT_FUNDS",
      balanceMinor: Number(sender.balance_minor),
    });
    return;
  }

  await postLeg(c, sessionId, p.transferId, p.fromAccountId, p.amountMinor, "DEBIT");
  await postLeg(c, sessionId, p.transferId, clearingId, p.amountMinor, "CREDIT");

  const holdId = randomUUID();
  await c.query(
    `INSERT INTO holds (id, session_id, transfer_id, from_account_id, to_account_id,
                        clearing_account_id, amount_minor, currency, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ACTIVE')`,
    [holdId, sessionId, p.transferId, p.fromAccountId, p.toAccountId, clearingId, p.amountMinor, CURRENCY]
  );

  await emit(c, sessionId, p.transferId, "FundsReserved", {
    transferId: p.transferId,
    holdId,
    clearingAccountId: clearingId,
    shard,
  });
}

export async function commit(
  c: PoolClient,
  sessionId: string,
  p: { transferId: string }
): Promise<void> {
  await lockTransfer(c, p.transferId);

  const { rows } = await c.query("SELECT * FROM holds WHERE transfer_id = $1", [p.transferId]);
  const hold = rows[0];
  if (!hold) {
    await emit(c, sessionId, p.transferId, "ReserveRejected", {
      transferId: p.transferId,
      reason: "NO_SUCH_HOLD",
    });
    return;
  }
  if (hold.status !== "ACTIVE") {
    await answerWithWhatHappened(c, sessionId, hold);
    return;
  }

  const amount = Number(hold.amount_minor);
  await lockAccounts(c, [hold.clearing_account_id, hold.to_account_id]);

  await postLeg(c, sessionId, p.transferId, hold.clearing_account_id, amount, "DEBIT");
  await postLeg(c, sessionId, p.transferId, hold.to_account_id, amount, "CREDIT");

  await c.query("UPDATE holds SET status = 'COMMITTED' WHERE id = $1", [hold.id]);

  await emit(c, sessionId, p.transferId, "FundsCommitted", {
    transferId: p.transferId,
    holdId: hold.id,
  });
}

export async function release(
  c: PoolClient,
  sessionId: string,
  p: { transferId: string; reason: string }
): Promise<void> {
  await lockTransfer(c, p.transferId);

  const { rows } = await c.query("SELECT * FROM holds WHERE transfer_id = $1", [p.transferId]);
  const hold = rows[0];

  // A compensation addressed by transfer, arriving before the reserve it compensates. Leave a
  // tombstone so the late reserve is refused rather than stranding the money forever.
  if (!hold) {
    await c.query(
      `INSERT INTO transfer_voids (transfer_id, session_id, reason)
       VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
      [p.transferId, sessionId, p.reason]
    );
    await emit(c, sessionId, p.transferId, "ReserveRejected", {
      transferId: p.transferId,
      reason: "TRANSFER_VOIDED",
    });
    return;
  }

  if (hold.status !== "ACTIVE") {
    await answerWithWhatHappened(c, sessionId, hold);
    return;
  }

  const amount = Number(hold.amount_minor);
  await lockAccounts(c, [hold.clearing_account_id, hold.from_account_id]);

  await postLeg(c, sessionId, p.transferId, hold.clearing_account_id, amount, "DEBIT");
  await postLeg(c, sessionId, p.transferId, hold.from_account_id, amount, "CREDIT");

  await c.query("UPDATE holds SET status = 'RELEASED', release_reason = $1 WHERE id = $2", [
    p.reason,
    hold.id,
  ]);

  await emit(c, sessionId, p.transferId, "FundsReleased", {
    transferId: p.transferId,
    holdId: hold.id,
    reason: p.reason,
  });
}
