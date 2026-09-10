import { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { login as loginRequest } from "./api/client";
import type { TokenSource } from "./api/client";

/**
 * Who is logged in, and the token every request carries.
 *
 * <h3>The console never parses the token</h3>
 *
 * There is no JWT decoding anywhere in this application, deliberately. The roles and the lifetime
 * come from the body of `POST /auth/token`, which the server put there so that a client would not
 * have to open its own credential to use it. Decoding it in the browser would be one line and
 * would create two problems: a second, subtly different reading of a claim the server already
 * interpreted (the `ROLE_` prefix is exactly where that goes wrong), and the impression that the
 * client is making an authorization decision.
 *
 * <h3>What `roles` is for, and what it is not for</h3>
 *
 * Roles here drive AFFORDANCE only - which tabs are offered, and what the empty state says. Every
 * rule that matters is in each service's SecurityConfig and is enforced on the server against the
 * signature. Hiding a button is a courtesy to the operator; it is not a control, and this file
 * should never be the reason something is safe.
 *
 * <h3>Where the token is kept</h3>
 *
 * In React state, plus `sessionStorage` so a page refresh does not log you out. `sessionStorage`
 * rather than `localStorage`: it dies with the tab, which for a 15-minute token on a demo console
 * is the right lifetime. Neither is immune to XSS - any script running on this origin can read
 * both - and the genuinely safer answer is an httpOnly cookie, which needs a server-side session
 * this system does not have and CSRF protection that a bearer-token API does not need. The honest
 * summary: this is a demo console for a portfolio project and the trade is stated rather than
 * hidden.
 */
export interface Session {
  username: string;
  roles: string[];
  token: string;
  /** Absolute wall-clock expiry, computed once from `expiresInSeconds`. */
  expiresAt: number;
}

interface AuthContextValue {
  session: Session | null;
  /** A function, not the string. See the note in `useAuth` for why that matters. */
  token: TokenSource;
  hasRole: (role: string) => boolean;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

const STORAGE_KEY = "dpe.session";

const AuthContext = createContext<AuthContextValue | null>(null);

function restore(): Session | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Session;
    // A restored token that has already expired is worse than no token: every request 401s and
    // the console looks broken rather than logged out.
    return parsed.expiresAt > Date.now() ? parsed : null;
  } catch {
    return null;
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(restore);

  // A ref shadowing the state, so `token()` below always reads the CURRENT value.
  //
  // This is the subtle part. If the token were closed over as a plain value, a long-lived poller
  // started before a re-login would keep sending the old token forever - and would fail with 401s
  // that look like a server problem. Reading through a ref means the token source is always
  // current no matter when the closure was created.
  const sessionRef = useRef<Session | null>(session);
  sessionRef.current = session;

  const token = useCallback<TokenSource>(() => sessionRef.current?.token ?? null, []);

  const login = useCallback(async (username: string, password: string) => {
    const response = await loginRequest(username, password);
    const next: Session = {
      username: response.username,
      roles: response.roles,
      token: response.accessToken,
      expiresAt: Date.now() + response.expiresInSeconds * 1000,
    };
    setSession(next);
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // Private-browsing mode and similar. Losing persistence costs a re-login on refresh and
      // nothing else, so it must not fail the login itself.
    }
  }, []);

  const logout = useCallback(() => {
    setSession(null);
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      /* see above */
    }
  }, []);

  const hasRole = useCallback(
    (role: string) => (sessionRef.current?.roles ?? []).includes(role),
    [],
  );

  const value = useMemo<AuthContextValue>(
    () => ({ session, token, hasRole, login, logout }),
    [session, token, hasRole, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) throw new Error("useAuth must be used inside AuthProvider");
  return context;
}
