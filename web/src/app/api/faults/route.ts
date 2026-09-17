import { NextResponse } from "next/server";
import { currentSession } from "@/lib/session";
import { query } from "@/lib/db";
import { readState } from "@/lib/state";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const BOOLEANS = ["duplicate_replies", "broker_down", "drop_first_commit", "forward_recovery"];

export async function POST(req: Request) {
  const session = await currentSession();
  const body = await req.json().catch(() => ({}));

  if (typeof body.gateway_mode === "string") {
    if (!["APPROVE", "DECLINE", "TIMEOUT"].includes(body.gateway_mode)) {
      return NextResponse.json({ error: "unknown gateway mode" }, { status: 400 });
    }
    await query("UPDATE faults SET gateway_mode = $1 WHERE session_id = $2", [
      body.gateway_mode,
      session.id,
    ]);
  }

  for (const key of BOOLEANS) {
    if (typeof body[key] === "boolean") {
      await query(`UPDATE faults SET ${key} = $1 WHERE session_id = $2`, [body[key], session.id]);
    }
  }

  return NextResponse.json(await readState(session.id));
}
