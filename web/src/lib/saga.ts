import type { PoolClient } from "pg";
import { emit, recordStep } from "./events";
import type { Faults } from "./gateway";
import { SAGA_DEADLINE_MS } from "./session";

/**
 * The saga state machine.
 *
 *   STARTED -> RESERVED -> CHARGED -> COMPLETED
 *   STARTED/RESERVED -> COMPENSATING -> COMPENSATED    (a decline, or a timeout before the pivot)
 *   STARTED -> FAILED                                  (the reserve was rejected: nothing to undo)
 *
 * Every non-terminal state means a message is outstanding: a command went out and its reply has
 * not come back. That is exactly what invariant I4 checks - a saga still sitting in one of them
 * after the system has gone quiet is a saga whose reply never came, and somewhere there is a hold
 * with a customer's money in it.
 */

export const TERMINAL = ["COMPLETED", "COMPENSATED", "FAILED"];

type Saga = Record<string, any>;

async function setStatus(
  c: PoolClient,
  saga: Saga,
  status: string,
  extra: { holdId?: string | null; pushDeadline?: boolean } = {}
): Promise<void> {
  const terminal = TERMINAL.includes(status);
  await c.query(
    `UPDATE saga_instances
        SET status = $1,
            hold_id = COALESCE($2, hold_id),
            completed_at = CASE WHEN $3 THEN now() ELSE NULL END,
            deadline_at = CASE WHEN $4 THEN now() + ($5 || ' milliseconds')::interval ELSE deadline_at END
      WHERE id = $6`,
    [status, extra.holdId ?? null, terminal, !!extra.pushDeadline, String(SAGA_DEADLINE_MS), saga.id]
  );
}

async function setTransfer(c: PoolClient, transferId: string, status: string): Promise<void> {
  await c.query("UPDATE transfers SET status = $1 WHERE id = $2", [status, transferId]);
}

/** Handles a reply from a participant. Called inside the consumer's transaction. */
export async function handleReply(
  c: PoolClient,
  sessionId: string,
  eventType: string,
  payload: any,
  messageId: string
): Promise<void> {
  const { rows } = await c.query(
    `SELECT s.*, t.amount_minor
       FROM saga_instances s
       JOIN transfers t ON t.id = s.transfer_id
      WHERE s.transfer_id = $1
        FOR UPDATE OF s`,
    [payload.transferId]
  );
  const saga = rows[0];
  if (!saga) return;

  // A terminal saga is finished. A late reply is recorded and otherwise ignored - it cannot be
  // allowed to restart a payment that has already been settled one way or the other.
  if (TERMINAL.includes(saga.status)) {
    await recordStep(c, sessionId, saga.id, eventType, "SKIPPED", saga.status, messageId, "saga already terminal");
    return;
  }

  switch (eventType) {
    case "FundsReserved": {
      if (saga.status !== "STARTED") break;
      await setStatus(c, saga, "RESERVED", { holdId: payload.holdId });
      await recordStep(c, sessionId, saga.id, "ReserveFunds", "SUCCEEDED", "RESERVED", messageId, `shard ${payload.shard}`);
      const mid = await emit(c, sessionId, payload.transferId, "ChargeGateway", {
        transferId: payload.transferId,
        amountMinor: Number(saga.amount_minor),
      });
      await c.query("UPDATE saga_instances SET charge_sent = true WHERE id = $1", [saga.id]);
      await recordStep(c, sessionId, saga.id, "ChargeGateway", "STARTED", "RESERVED", mid, null);
      break;
    }

    case "ReserveRejected": {
      // Terminal directly, with no trip through COMPENSATING: no money moved, so there is nothing
      // to compensate, and a ReleaseFunds naming a hold that does not exist would be refused.
      const reason = payload.reason ?? "REJECTED";
      if (saga.status === "COMPENSATING") {
        await setStatus(c, saga, "COMPENSATED");
        await setTransfer(c, payload.transferId, "COMPENSATED");
        await recordStep(c, sessionId, saga.id, "ReleaseFunds", "SUCCEEDED", "COMPENSATED", messageId, reason);
      } else {
        await setStatus(c, saga, "FAILED");
        await setTransfer(c, payload.transferId, "FAILED");
        await recordStep(c, sessionId, saga.id, "ReserveFunds", "FAILED", "FAILED", messageId, reason);
      }
      break;
    }

    case "GatewayApproved": {
      if (saga.status !== "RESERVED") break;
      await setStatus(c, saga, "CHARGED");
      await recordStep(c, sessionId, saga.id, "ChargeGateway", "SUCCEEDED", "CHARGED", messageId, "the pivot");
      const mid = await emit(c, sessionId, payload.transferId, "CommitFunds", {
        transferId: payload.transferId,
        holdId: saga.hold_id,
      });
      await recordStep(c, sessionId, saga.id, "CommitFunds", "STARTED", "CHARGED", mid, null);
      break;
    }

    case "GatewayDeclined": {
      if (saga.status !== "RESERVED") break;
      await setStatus(c, saga, "COMPENSATING");
      await recordStep(c, sessionId, saga.id, "ChargeGateway", "FAILED", "COMPENSATING", messageId, payload.reason);
      const mid = await emit(c, sessionId, payload.transferId, "ReleaseFunds", {
        transferId: payload.transferId,
        holdId: saga.hold_id,
        reason: "GATEWAY_DECLINED",
      });
      await recordStep(c, sessionId, saga.id, "ReleaseFunds", "STARTED", "COMPENSATING", mid, null);
      break;
    }

    case "FundsCommitted": {
      // Accepted in COMPENSATING as well as CHARGED. A reply is a statement of fact: if the
      // recipient has the money, the payment is complete, whatever we had decided to do next.
      await setStatus(c, saga, "COMPLETED");
      await setTransfer(c, payload.transferId, "COMPLETED");
      await recordStep(c, sessionId, saga.id, "CommitFunds", "SUCCEEDED", "COMPLETED", messageId, null);
      break;
    }

    case "FundsReleased": {
      await setStatus(c, saga, "COMPENSATED");
      await setTransfer(c, payload.transferId, "COMPENSATED");
      await recordStep(c, sessionId, saga.id, "ReleaseFunds", "SUCCEEDED", "COMPENSATED", messageId, payload.reason);
      // If a charge had already gone out, chase it with a void in case it is still on its way.
      if (saga.charge_sent) {
        const mid = await emit(c, sessionId, payload.transferId, "VoidCharge", {
          transferId: payload.transferId,
          reason: "SAGA_COMPENSATED",
        });
        await recordStep(c, sessionId, saga.id, "VoidCharge", "STARTED", null, mid, null);
      }
      break;
    }

    case "ChargeVoided": {
      await recordStep(c, sessionId, saga.id, "VoidCharge", "SUCCEEDED", saga.status, messageId, null);
      break;
    }
  }
}

/**
 * The timeout sweeper.
 *
 * A timeout is decided from a clock, not from the message order, so it can fire before the step it
 * is cancelling has even arrived. Every compensation it sends is therefore addressed by transfer
 * and leaves a tombstone, so the two commute whichever order they land in.
 */
export async function sweep(c: PoolClient, sessionId: string, faults: Faults): Promise<number> {
  const { rows } = await c.query(
    `SELECT * FROM saga_instances
      WHERE session_id = $1
        AND status NOT IN ('COMPLETED','COMPENSATED','FAILED')
        AND deadline_at < now()
      FOR UPDATE SKIP LOCKED`,
    [sessionId]
  );

  for (const saga of rows) {
    if (saga.status === "CHARGED") {
      if (faults.forward_recovery) {
        // After the pivot, recovery only goes forward. Re-send the commit, stay in CHARGED, and
        // push the deadline out by one step. A forward step has nothing to give up in favour of.
        await setStatus(c, saga, "CHARGED", { pushDeadline: true });
        const mid = await emit(c, sessionId, saga.transfer_id, "CommitFunds", {
          transferId: saga.transfer_id,
          holdId: saga.hold_id,
        });
        await recordStep(c, sessionId, saga.id, "CommitFunds", "STARTED", "CHARGED", mid, "re-sent after timeout");
      } else {
        // The defect, kept switchable on purpose: unwinding after the pivot refunds the customer
        // out of our own books while the card network keeps the charge. I1-I5 all stay green;
        // only S2 notices.
        await setStatus(c, saga, "COMPENSATING", { pushDeadline: true });
        const mid = await emit(c, sessionId, saga.transfer_id, "ReleaseFunds", {
          transferId: saga.transfer_id,
          holdId: saga.hold_id,
          reason: "TIMEOUT",
        });
        await recordStep(c, sessionId, saga.id, "ReleaseFunds", "STARTED", "COMPENSATING", mid, "backward recovery after the pivot");
      }
      continue;
    }

    // Before the pivot, or already compensating: send (or re-send) the compensation.
    await setStatus(c, saga, "COMPENSATING", { pushDeadline: true });
    const mid = await emit(c, sessionId, saga.transfer_id, "ReleaseFunds", {
      transferId: saga.transfer_id,
      holdId: saga.hold_id,
      reason: "TIMEOUT",
    });
    await recordStep(c, sessionId, saga.id, "ReleaseFunds", "STARTED", "COMPENSATING", mid, "saga deadline expired");

    // If a charge request had already gone out, a void follows it in case it is still in flight.
    if (saga.charge_sent) {
      const vid = await emit(c, sessionId, saga.transfer_id, "VoidCharge", {
        transferId: saga.transfer_id,
        reason: "SAGA_TIMEOUT",
      });
      await recordStep(c, sessionId, saga.id, "VoidCharge", "STARTED", null, vid, null);
    }
  }

  return rows.length;
}
