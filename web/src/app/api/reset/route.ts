import { NextResponse } from "next/server";
import { currentSession, resetSession, sweepOldSessions } from "@/lib/session";
import { readState } from "@/lib/state";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST() {
  const session = await currentSession();
  await resetSession(session.id);
  // Housekeeping for a public demo: abandoned worlds are swept on the way past.
  sweepOldSessions().catch(() => {});
  return NextResponse.json(await readState(session.id));
}
