import { NextResponse } from "next/server";
import { currentSession, COOKIE } from "@/lib/session";
import { tick } from "@/lib/engine";
import { readState } from "@/lib/state";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

/**
 * One round of the relay and its consumers.
 *
 * A serverless platform has no background threads, so the page that is watching turns the crank.
 * The mechanism underneath is unchanged: committed rows are published, delivered at-least-once,
 * and claimed through the inbox inside the transaction that does the work.
 */
export async function POST() {
  const session = await currentSession();
  const result = await tick(session.id);
  const state = await readState(session.id);
  const res = NextResponse.json({ ...state, tick: result });
  res.cookies.set(COOKIE, session.id, {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24,
  });
  return res;
}
