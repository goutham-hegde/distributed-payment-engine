import { currentSession } from "@/lib/session";
import { readState } from "@/lib/state";
import { withSession } from "@/lib/respond";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const session = await currentSession();
  return withSession(await readState(session.id), session.id);
}
