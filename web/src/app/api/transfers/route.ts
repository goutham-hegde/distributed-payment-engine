import { NextResponse } from "next/server";
import { randomUUID } from "crypto";
import { currentSession, COOKIE, SAGA_DEADLINE_MS, CURRENCY } from "@/lib/session";
import { tx } from "@/lib/db";
import { emit, recordStep } from "@/lib/events";
import { readState } from "@/lib/state";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const MAX_AMOUNT_MINOR = 10_000_00; // Rupees 10,000.00 - a demo, not a bank.

/**
 * Accept a payment.
 *
 * Everything below happens in ONE transaction: the idempotency claim, the transfer, the saga, and
 * the first command written to the outbox. The ordering is the whole guarantee.
 *
 * Claim first in its own transaction and a crash leaves a key that says "done" pointing at a
 * payment that does not exist, refusing every retry forever. Do the work first and the retry makes
 * a second payment. They commit together, or nothing happened at all.
 *
 * The answer is 202, not 200: the request has been accepted and no money has moved yet.
 */
export async function POST(req: Request) {
  const session = await currentSession();
  const body = await req.json().catch(() => ({}));

  const amountMinor = Number(body.amountMinor);
  if (!Number.isInteger(amountMinor) || amountMinor <= 0 || amountMinor > MAX_AMOUNT_MINOR) {
    return NextResponse.json(
      { error: `amountMinor must be a whole number of paise between 1 and ${MAX_AMOUNT_MINOR}` },
      { status: 400 }
    );
  }

  const fromOwner = String(body.fromOwner ?? "alice");
  const toOwner = String(body.toOwner ?? "bob");
  if (fromOwner === toOwner) {
    return NextResponse.json({ error: "a payment needs two different accounts" }, { status: 400 });
  }

  // Supplied by the caller in the real API. The console sends one per click, and deliberately
  // reuses it when you ask it to.
  const idemKey =
    req.headers.get("Idempotency-Key")?.slice(0, 128) || randomUUID();

  try {
    const outcome = await tx(async (c) => {
      // The gate. ON CONFLICT DO NOTHING blocks on a conflicting UNCOMMITTED row, so a duplicate
      // arriving mid-flight waits for the winner and then replays it - rather than racing it.
      const claim = await c.query(
        `INSERT INTO idempotency_records (session_id, idem_key, subject)
         VALUES ($1, $2, $3)
         ON CONFLICT (session_id, subject, idem_key) DO NOTHING
         RETURNING id`,
        [session.id, idemKey, fromOwner]
      );

      if (claim.rowCount === 0) {
        const { rows } = await c.query(
          `SELECT transfer_id FROM idempotency_records
            WHERE session_id = $1 AND subject = $2 AND idem_key = $3`,
          [session.id, fromOwner, idemKey]
        );
        return { replayed: true as const, transferId: rows[0]?.transfer_id ?? null };
      }

      const { rows: accounts } = await c.query(
        `SELECT id, owner_id FROM accounts
          WHERE session_id = $1 AND account_type = 'CUSTOMER' AND owner_id = ANY($2)`,
        [session.id, [fromOwner, toOwner]]
      );
      const from = accounts.find((a) => a.owner_id === fromOwner);
      const to = accounts.find((a) => a.owner_id === toOwner);
      if (!from || !to) throw new HttpError(404, "no such account");

      const transferId = randomUUID();
      await c.query(
        `INSERT INTO transfers (id, session_id, from_account_id, to_account_id,
                                amount_minor, currency, status, initiated_by)
         VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)`,
        [transferId, session.id, from.id, to.id, amountMinor, CURRENCY, fromOwner]
      );

      const sagaId = randomUUID();
      await c.query(
        `INSERT INTO saga_instances (id, session_id, transfer_id, status, deadline_at)
         VALUES ($1, $2, $3, 'STARTED', now() + ($4 || ' milliseconds')::interval)`,
        [sagaId, session.id, transferId, String(SAGA_DEADLINE_MS)]
      );

      const messageId = await emit(c, session.id, transferId, "ReserveFunds", {
        transferId,
        fromAccountId: from.id,
        toAccountId: to.id,
        amountMinor,
        currency: CURRENCY,
        initiatedBy: fromOwner,
      });

      await recordStep(c, session.id, sagaId, "ReserveFunds", "STARTED", "STARTED", messageId, null);

      await c.query(
        `UPDATE idempotency_records SET transfer_id = $1
          WHERE session_id = $2 AND subject = $3 AND idem_key = $4`,
        [transferId, session.id, fromOwner, idemKey]
      );

      return { replayed: false as const, transferId };
    });

    const state = await readState(session.id);
    const res = NextResponse.json(
      {
        ...state,
        accepted: { transferId: outcome.transferId, replayed: outcome.replayed },
      },
      { status: 202 }
    );
    res.headers.set("Idempotency-Replayed", String(outcome.replayed));
    res.cookies.set(COOKIE, session.id, {
      httpOnly: true,
      sameSite: "lax",
      path: "/",
      maxAge: 60 * 60 * 24,
    });
    return res;
  } catch (e) {
    if (e instanceof HttpError) {
      return NextResponse.json({ error: e.message }, { status: e.status });
    }
    throw e;
  }
}

class HttpError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}
