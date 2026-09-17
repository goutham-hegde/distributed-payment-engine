import { NextResponse } from "next/server";
import { COOKIE } from "./session";

/**
 * Every response carries the session cookie.
 *
 * `currentSession()` will happily open and seed a new world when it does not recognise the
 * caller, so any route that forgets to send the cookie back hands out a fresh ledger on every
 * request - and the visitor watches their payment vanish between two clicks. Routing every
 * response through here means that cannot be forgotten in one place and remembered in another.
 */
export function withSession<T>(body: T, sessionId: string, init?: ResponseInit): NextResponse {
  const res = NextResponse.json(body, init);
  res.cookies.set(COOKIE, sessionId, {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24,
  });
  return res;
}
