import { NextResponse } from "next/server";
import { currentSession, COOKIE } from "@/lib/session";
import { readState } from "@/lib/state";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const session = await currentSession();
  const state = await readState(session.id);
  const res = NextResponse.json(state);
  res.cookies.set(COOKIE, session.id, {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24,
  });
  return res;
}
