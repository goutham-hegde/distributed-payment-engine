import { useState } from "react";
import { useAuth } from "../auth";
import { ErrorBox } from "../components";

/**
 * The login screen.
 *
 * Note what is not offered: no "remember me", and no hint about which of the username or the
 * password was wrong - because the server refuses to tell, deliberately, so that a stranger cannot
 * learn which accounts exist before spending a guess on a password. A UI that guessed for it
 * ("no such user") would hand back the oracle the API declined to be.
 */
export function Login() {
  const { login } = useAuth();
  const [username, setUsername] = useState("alice");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(username, password);
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="login">
      <section className="panel">
        <h2>DPE Console</h2>
        <p className="sub">Sign in to the payment orchestrator.</p>

        <form onSubmit={submit}>
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="username">Username</label>
            <input
              id="username"
              value={username}
              autoComplete="username"
              onChange={(e) => setUsername(e.target.value)}
            />
          </div>

          <div style={{ marginBottom: 16 }}>
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              autoComplete="current-password"
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>

          <button type="submit" disabled={busy || !username || !password} style={{ width: "100%" }}>
            {busy ? "Signing in…" : "Sign in"}
          </button>
        </form>

        <ErrorBox error={error} />

        {/*
          The development directory is printed here because it IS a development directory - plain
          text passwords in application.yml, three fake people, no user store. Anything that looked
          like a real credential would not belong on a screen, and anything hidden here would only
          be hidden from the person who needs it.
        */}
        <p className="hint">
          Development identities (see <code className="mono">application.yml</code>):
          <br />
          <code className="mono">alice</code> / <code className="mono">alice-password</code> — USER,
          owns accounts, can move money
          <br />
          <code className="mono">bob</code> / <code className="mono">bob-password</code> — USER
          <br />
          <code className="mono">operator</code> / <code className="mono">operator-password</code> —
          OPERATOR: sees every operational surface and <em>cannot</em> move money, because ownership
          has no role bypass.
        </p>
      </section>
    </div>
  );
}
