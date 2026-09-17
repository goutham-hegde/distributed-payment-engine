import { NextResponse } from "next/server";
import { currentSession } from "@/lib/session";
import { invariants } from "@/lib/invariants";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

/**
 * The same surface the deployed system exposes at /admin/invariants, for anyone who would rather
 * curl it than read a dashboard. It answers for one visitor's world only.
 */
export async function GET() {
  const session = await currentSession();
  const report = await invariants(session.id);
  return NextResponse.json(report, { status: report.allHold ? 200 : 409 });
}
