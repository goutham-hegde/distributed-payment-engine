import { cookies } from "next/headers";
import { randomUUID } from "crypto";
import { migrate, query, tx } from "./db";

export const COOKIE = "dpe_session";
export const CURRENCY = "INR";

/** Opening balances, in paise. Money is a whole number of minor units, never a float. */
export const ALICE_OPENING = 100_000; // Rupees 1,000.00
export const BOB_OPENING = 50_000; //    Rupees   500.00
export const CLEARING_SHARDS = 8;

/**
 * The saga's whole-payment deadline, from acceptance.
 *
 * In the deployed Java system this is 60 s, because it has to outlast a Kafka partition orphaned
 * by a crashed replica (a 45 s session timeout plus rebalance plus margin). Nothing here can
 * orphan a partition, so it is short enough for a visitor to watch a timeout happen.
 */
export const SAGA_DEADLINE_MS = 12_000;

export type Session = { id: string; fresh: boolean };

/** Reads the session cookie, creating and seeding a world the first time someone arrives. */
export async function currentSession(): Promise<Session> {
  await migrate();

  const jar = await cookies();
  const existing = jar.get(COOKIE)?.value;

  if (existing && /^[0-9a-f-]{36}$/i.test(existing)) {
    const rows = await query<{ id: string }>("SELECT id FROM sessions WHERE id = $1", [existing]);
    if (rows.length) {
      await query("UPDATE sessions SET last_seen_at = now() WHERE id = $1", [existing]);
      return { id: existing, fresh: false };
    }
  }

  const id = randomUUID();
  await seed(id);
  return { id, fresh: true };
}

/** Opens the accounts a demo needs, through the ordinary ledger path. */
export async function seed(sessionId: string): Promise<void> {
  await tx(async (c) => {
    await c.query("INSERT INTO sessions (id) VALUES ($1) ON CONFLICT DO NOTHING", [sessionId]);
    await c.query("INSERT INTO faults (session_id) VALUES ($1) ON CONFLICT DO NOTHING", [sessionId]);

    const issuance = randomUUID();
    await c.query(
      `INSERT INTO accounts (id, session_id, owner_id, account_type, currency, balance_minor)
       VALUES ($1, $2, 'issuance', 'SYSTEM', $3, 0)`,
      [issuance, sessionId, CURRENCY]
    );

    // CLEARING is sharded. One clearing row made every reserve and commit queue on a single row
    // lock; a hold records which shard it used, and commit and release read it back from the hold
    // rather than recomputing it - a recomputed shard would differ the day a shard is added, and
    // split one transfer's legs across two accounts.
    for (let i = 0; i < CLEARING_SHARDS; i++) {
      await c.query(
        `INSERT INTO accounts (id, session_id, owner_id, account_type, currency, balance_minor)
         VALUES ($1, $2, $3, 'CLEARING', $4, 0)`,
        [randomUUID(), sessionId, `clearing-${i}`, CURRENCY]
      );
    }

    for (const [owner, opening] of [
      ["alice", ALICE_OPENING],
      ["bob", BOB_OPENING],
    ] as const) {
      const id = randomUUID();
      await c.query(
        `INSERT INTO accounts (id, session_id, owner_id, account_type, currency, balance_minor)
         VALUES ($1, $2, $3, 'CUSTOMER', $4, 0)`,
        [id, sessionId, owner, CURRENCY]
      );
      if (opening > 0) {
        // Funding issues new money: it is debited from the issuance account, whose balance goes
        // negative by design. Without that counterpart this credit would be money from nowhere and
        // invariant I1 would break before the first payment.
        const fundingId = randomUUID();
        await c.query(
          `INSERT INTO ledger_entries (session_id, transfer_id, account_id, amount_minor, entry_type, currency)
           VALUES ($1, $2, $3, $4, 'DEBIT', $5), ($1, $2, $6, $7, 'CREDIT', $5)`,
          [sessionId, fundingId, issuance, -opening, CURRENCY, id, opening]
        );
        await c.query("UPDATE accounts SET balance_minor = balance_minor - $1 WHERE id = $2", [
          opening,
          issuance,
        ]);
        await c.query("UPDATE accounts SET balance_minor = $1 WHERE id = $2", [opening, id]);
      }
    }
  });
}

/** Wipes a visitor's world and opens it again. */
export async function resetSession(sessionId: string): Promise<void> {
  await query("DELETE FROM sessions WHERE id = $1", [sessionId]);
  await seed(sessionId);
}

/**
 * Housekeeping. A public demo accumulates abandoned worlds, and the free database tier this runs
 * on is small. Sessions untouched for a day go, and their rows follow by ON DELETE CASCADE.
 */
export async function sweepOldSessions(): Promise<void> {
  await query("DELETE FROM sessions WHERE last_seen_at < now() - interval '24 hours'");
}

const SWEEP_EVERY_MS = 10 * 60 * 1000;
declare global {
  // eslint-disable-next-line no-var
  var __dpeLastSweep: number | undefined;
}

/**
 * The sweep, on the way past.
 *
 * It used to hang off the reset endpoint alone, which meant abandoned worlds were only collected
 * when somebody happened to press Reset - so the one visitor who tidies up paid for everyone who
 * did not, and a demo nobody resets never collects at all. Hanging it off the tick instead means
 * any traffic at all keeps the database trimmed.
 *
 * Throttled per warm instance rather than run every tick: the page polls this several times a
 * second while a payment is in flight, and a DELETE scanning the session table that often would
 * cost more than the rows it reclaims. Several instances each sweeping on their own clock is
 * harmless - the statement is idempotent and deletes nothing twice.
 *
 * Never awaited by a request, and never allowed to fail one. Housekeeping that can break a
 * payment is not housekeeping.
 */
export function maybeSweepOldSessions(): void {
  const now = Date.now();
  if (global.__dpeLastSweep && now - global.__dpeLastSweep < SWEEP_EVERY_MS) return;
  global.__dpeLastSweep = now;
  sweepOldSessions().catch(() => {
    // Let the next tick try again rather than waiting out the full interval.
    global.__dpeLastSweep = undefined;
  });
}
