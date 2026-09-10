import type { ReactNode } from "react";
import { ApiError } from "./api/client";

/** A titled section. `sub` is where the "why this panel exists" line goes. */
export function Panel({
  title,
  sub,
  right,
  children,
}: {
  title: string;
  sub?: ReactNode;
  right?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="panel">
      <div className="row" style={{ justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ minWidth: 0 }}>
          <h2>{title}</h2>
          {sub ? <p className="sub">{sub}</p> : null}
        </div>
        {right}
      </div>
      {children}
    </section>
  );
}

/**
 * An error, rendered with the distinction the API went to the trouble of making.
 *
 * A 403 and a 404 mean different things here and the console says so, because a user staring at
 * "request failed" learns nothing about a system whose whole point is that it answers carefully.
 */
export function ErrorBox({ error }: { error: Error | null }) {
  if (!error) return null;

  if (error instanceof ApiError) {
    if (error.status === 401) {
      return <div className="error">401 — the token is missing or expired. Log in again.</div>;
    }
    if (error.status === 403) {
      return (
        <div className="error">
          403 — this token is valid and this subject is not allowed to do that. Logging in again
          will not help; a different role will. (Operator surfaces need OPERATOR; moving money needs
          USER <em>and</em> ownership of the source account.)
        </div>
      );
    }
    if (error.status === 404) {
      return (
        <div className="error">
          404 — no such record, <em>or</em> it is not yours. The API answers both the same way on
          purpose, so that walking ids cannot confirm what exists.
        </div>
      );
    }
    return <div className="error">{error.status} — {error.message}</div>;
  }

  return <div className="error">{error.message}</div>;
}

const OK_STATUSES = new Set(["COMPLETED", "SUCCEEDED", "SUCCESS", "RESERVED", "CHARGED", "COMMITTED"]);
const BAD_STATUSES = new Set(["FAILED", "DECLINED", "REJECTED", "TIMED_OUT"]);
const WARN_STATUSES = new Set(["COMPENSATING", "COMPENSATED", "RELEASED"]);

/**
 * A status pill.
 *
 * COMPENSATED is amber, not red, and that colour is an argument. A compensated transfer is the
 * system working: the money went back to the sender and every invariant still holds. Painting it
 * red would teach whoever watches this screen that correct behaviour is a failure, which is
 * exactly the habit that gets a real alert ignored.
 */
export function Status({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="badge neutral">—</span>;
  const upper = value.toUpperCase();
  const tone = OK_STATUSES.has(upper)
    ? "ok"
    : BAD_STATUSES.has(upper)
      ? "bad"
      : WARN_STATUSES.has(upper)
        ? "warn"
        : "info";
  return <span className={`badge ${tone}`}>{value}</span>;
}

export function Stat({ label, value, note }: { label: string; value: string; note?: ReactNode }) {
  return (
    <div className="stat">
      <div className="k">{label}</div>
      <div className="v">{value}</div>
      {note ? <div className="n">{note}</div> : null}
    </div>
  );
}

/** A copyable id, shortened for the eye and full in the title attribute. */
export function Id({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="dim">—</span>;
  return (
    <code className="mono" title={value}>
      {value.slice(0, 8)}
    </code>
  );
}
