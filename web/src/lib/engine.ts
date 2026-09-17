import { tx, query } from "./db";
import { commit, release, reserve } from "./accounts";
import { charge, loadFaults, voidCharge, type Faults } from "./gateway";
import { handleReply, sweep } from "./saga";

/**
 * The relay and the consumers.
 *
 * In the deployed system a background thread in each service polls its own outbox and publishes to
 * Kafka, and a consumer group in the next service picks the message up. A serverless platform has
 * no background threads, so here the loop is invoked by the page that is watching: every poll from
 * the console runs one round.
 *
 * That is a real difference and worth stating plainly, but it is a difference in WHO TURNS THE
 * CRANK, not in the mechanism. The message is still a committed database row published separately
 * from the transaction that wrote it; delivery is still at-least-once; the consumer still claims
 * an inbox row in the same transaction as the work. Every failure mode the pattern exists to
 * handle is still reachable, which is why the fault switches below can reach them.
 */

const MAX_ROUNDS = 1;

export type TickResult = { published: number; delivered: number; swept: number };

/**
 * One round: publish what is waiting, then deliver what has been published.
 *
 * Messages produced by this round's handlers are left for the next one, so a payment advances one
 * hop per round and a person watching can see it move.
 */
export async function tick(sessionId: string): Promise<TickResult> {
  const faults = await tx((c) => loadFaults(c, sessionId));
  const result: TickResult = { published: 0, delivered: 0, swept: 0 };

  result.swept = await tx((c) => sweep(c, sessionId, faults));

  for (let round = 0; round < MAX_ROUNDS; round++) {
    result.published += await relay(sessionId, faults);
    result.delivered += await deliver(sessionId, faults);
  }

  return result;
}

/**
 * The relay: mark committed outbox rows as published, oldest first.
 *
 * With the broker down this does nothing at all, and that is the whole point - the rows stay in
 * the outbox, the API keeps accepting payments, and nothing is lost. A broker outage is not a
 * customer-visible outage.
 */
async function relay(sessionId: string, faults: Faults): Promise<number> {
  if (faults.broker_down) return 0;

  const rows = await query<{ id: string; event_type: string; aggregate_id: string }>(
    `UPDATE outbox SET published_at = now()
      WHERE id IN (
        SELECT id FROM outbox
         WHERE session_id = $1 AND published_at IS NULL
         ORDER BY created_at
         LIMIT 50
      )
      RETURNING id, event_type, aggregate_id`,
    [sessionId]
  );

  // "The account service was unreachable just then" is injected here, at the relay, because that
  // is where a message can plausibly vanish: written, committed, and never delivered.
  //
  // It is deliberately ONE-SHOT per payment. A fault that swallows every retry forever is not a
  // service that was briefly unreachable, it is a service that no longer exists, and nothing can
  // recover from that - which would make the forward-recovery half of the demonstration
  // impossible to show.
  if (faults.drop_first_commit) {
    const commits = rows.filter((r) => r.event_type === "CommitFunds");
    for (const r of commits) {
      const alreadyDropped = await query<{ n: string }>(
        `SELECT COUNT(*) AS n FROM outbox
          WHERE session_id = $1 AND aggregate_id = $2 AND event_type = 'CommitFunds' AND dropped`,
        [sessionId, r.aggregate_id]
      );
      if (Number(alreadyDropped[0]?.n ?? 0) === 0) {
        await query("UPDATE outbox SET consumed_at = now(), dropped = true WHERE id = $1", [r.id]);
      }
    }
  }

  return rows.length;
}

/** Delivers each published, unconsumed message to its handler, through the inbox. */
async function deliver(sessionId: string, faults: Faults): Promise<number> {
  const pending = await query<{ id: string }>(
    `SELECT id FROM outbox
      WHERE session_id = $1 AND published_at IS NOT NULL AND consumed_at IS NULL
      ORDER BY published_at, created_at
      LIMIT 50`,
    [sessionId]
  );

  let delivered = 0;
  for (const { id } of pending) {
    await dispatch(sessionId, id, faults);
    delivered++;

    // At-least-once delivery, made visible. The same message id is handed to the consumer a
    // second time; the inbox row written by the first delivery collides and the duplicate is
    // discarded before it can do anything.
    if (faults.duplicate_replies) {
      await dispatch(sessionId, id, faults);
    }
  }
  return delivered;
}

/**
 * Consume one message.
 *
 * The inbox claim and the work commit in ONE transaction. That ordering is the whole guarantee:
 * claim first in its own transaction and a crash loses the message for good; do the work first
 * and a redelivery does it twice.
 */
async function dispatch(sessionId: string, messageId: string, faults: Faults): Promise<void> {
  await tx(async (c) => {
    const { rows } = await c.query("SELECT * FROM outbox WHERE id = $1", [messageId]);
    const msg = rows[0];
    if (!msg) return;

    const claim = await c.query(
      `INSERT INTO inbox (message_id, session_id, event_type)
       VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
      [messageId, sessionId, msg.event_type]
    );

    // Already consumed. This is the duplicate being absorbed, and it is a normal event.
    if (claim.rowCount === 0) {
      await c.query("UPDATE outbox SET consumed_at = COALESCE(consumed_at, now()) WHERE id = $1", [
        messageId,
      ]);
      return;
    }

    const p = msg.payload;
    switch (msg.event_type) {
      case "ReserveFunds":
        await reserve(c, sessionId, p);
        break;
      case "CommitFunds":
        await commit(c, sessionId, p);
        break;
      case "ReleaseFunds":
        await release(c, sessionId, { transferId: p.transferId, reason: p.reason ?? "TIMEOUT" });
        break;
      case "ChargeGateway":
        await charge(c, sessionId, faults, p);
        break;
      case "VoidCharge":
        await voidCharge(c, sessionId, { transferId: p.transferId, reason: p.reason ?? "VOID" });
        break;
      default:
        await handleReply(c, sessionId, msg.event_type, p, messageId);
    }

    await c.query("UPDATE outbox SET consumed_at = now() WHERE id = $1", [messageId]);
  });
}
