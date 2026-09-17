import type { PoolClient } from "pg";
import { randomUUID } from "crypto";
import { emit } from "./events";

/**
 * A simulated external payment provider, with runtime-tunable failure injection.
 *
 * It stands for the part of the system nobody controls. Everything before a charge here can be
 * undone by writing an opposite ledger entry; nothing after it can, because the money has left
 * for a card network that has never heard of our saga. That asymmetry is the pivot the whole
 * recovery design turns on.
 */

export type Faults = {
  gateway_mode: "APPROVE" | "DECLINE" | "TIMEOUT";
  duplicate_replies: boolean;
  broker_down: boolean;
  drop_first_commit: boolean;
  forward_recovery: boolean;
};

export async function charge(
  c: PoolClient,
  sessionId: string,
  faults: Faults,
  p: { transferId: string; amountMinor: number }
): Promise<void> {
  // A charge that finds a tombstone was cancelled while this command was in flight. Answering
  // with a decline is what stops a replayed dead letter charging an already-refunded payment.
  const { rows: existing } = await c.query(
    "SELECT * FROM gateway_charges WHERE transfer_id = $1",
    [p.transferId]
  );
  if (existing[0]) {
    const status = existing[0].status;
    if (status === "VOIDED") {
      await emit(c, sessionId, p.transferId, "GatewayDeclined", {
        transferId: p.transferId,
        reason: "CHARGE_VOIDED",
      });
    } else if (status === "APPROVED") {
      await emit(c, sessionId, p.transferId, "GatewayApproved", {
        transferId: p.transferId,
        chargeId: existing[0].id,
        note: "already charged",
      });
    } else {
      await emit(c, sessionId, p.transferId, "GatewayDeclined", {
        transferId: p.transferId,
        reason: "ALREADY_DECLINED",
      });
    }
    return;
  }

  // TIMEOUT: the provider takes the request and never answers. The saga will find this at its
  // deadline, and what it does then is the whole question.
  if (faults.gateway_mode === "TIMEOUT") {
    return;
  }

  const approved = faults.gateway_mode === "APPROVE";
  const chargeId = randomUUID();
  await c.query(
    `INSERT INTO gateway_charges (id, session_id, transfer_id, amount_minor, status)
     VALUES ($1, $2, $3, $4, $5)`,
    [chargeId, sessionId, p.transferId, p.amountMinor, approved ? "APPROVED" : "DECLINED"]
  );

  if (approved) {
    await emit(c, sessionId, p.transferId, "GatewayApproved", {
      transferId: p.transferId,
      chargeId,
    });
  } else {
    await emit(c, sessionId, p.transferId, "GatewayDeclined", {
      transferId: p.transferId,
      reason: "CARD_DECLINED",
    });
  }
}

/**
 * The compensation at the gateway - and the reason the charge is a one-way door.
 *
 * A VOIDED row is both a reversal and a tombstone: written with ON CONFLICT it blocks on a
 * concurrent uncommitted charge and then sees it, so the void cannot lose the race. The unique
 * index on transfer_id IS the mutual exclusion.
 *
 * What it will NOT do is cancel a charge that was already approved. That is not a limitation of
 * this code, it is how card networks work: an authorization can be voided before capture, but once
 * the money has moved, undoing it is a refund - a new movement out into the world, not an erasure
 * of the old one. This is precisely what makes the approval a pivot: everything before it can be
 * undone by writing an opposite ledger entry, and nothing after it can.
 */
export async function voidCharge(
  c: PoolClient,
  sessionId: string,
  p: { transferId: string; reason: string }
): Promise<void> {
  const { rows } = await c.query(
    `INSERT INTO gateway_charges (id, session_id, transfer_id, amount_minor, status, voided_at, void_reason)
     VALUES ($1, $2, $3, 0, 'VOIDED', now(), $4)
     ON CONFLICT (transfer_id) DO UPDATE
       SET status = 'VOIDED', voided_at = now(), void_reason = EXCLUDED.void_reason
       WHERE gateway_charges.status = 'DECLINED'
     RETURNING id`,
    [randomUUID(), sessionId, p.transferId, p.reason]
  );

  const refused = rows.length === 0;

  await emit(c, sessionId, p.transferId, "ChargeVoided", {
    transferId: p.transferId,
    reason: p.reason,
    refused,
    note: refused ? "the charge was already approved and cannot be voided" : undefined,
  });
}

export async function loadFaults(c: PoolClient, sessionId: string): Promise<Faults> {
  const { rows } = await c.query("SELECT * FROM faults WHERE session_id = $1", [sessionId]);
  return (
    rows[0] ?? {
      gateway_mode: "APPROVE",
      duplicate_replies: false,
      broker_down: false,
      drop_first_commit: false,
      forward_recovery: true,
    }
  );
}
