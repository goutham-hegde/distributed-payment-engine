"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { EngineState } from "@/lib/state";

/** How long the page keeps turning the crank after your last action, before it waits for you. */
const MAX_ROUNDS = 90;
const ROUND_MS = 700;

function rupees(paise: number): string {
  const neg = paise < 0;
  const v = Math.abs(paise);
  return `${neg ? "-" : ""}₹${Math.floor(v / 100).toLocaleString("en-IN")}.${String(v % 100).padStart(2, "0")}`;
}

function shortId(id: string): string {
  return id.slice(0, 8);
}

export default function Console() {
  const [state, setState] = useState<EngineState | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rupeeAmount, setRupeeAmount] = useState("300.00");
  const [rounds, setRounds] = useState(0);
  const lastKey = useRef<string | null>(null);
  const [lastReplayed, setLastReplayed] = useState<boolean | null>(null);

  const call = useCallback(
    async (url: string, init?: RequestInit): Promise<EngineState | null> => {
      try {
        const res = await fetch(url, { ...init, cache: "no-store" });
        const body = await res.json();
        if (!res.ok && res.status !== 202 && res.status !== 409) {
          setError(body?.error ?? `Request failed (${res.status})`);
          return null;
        }
        setError(null);
        if (typeof body?.accepted?.replayed === "boolean") {
          setLastReplayed(body.accepted.replayed);
        }
        return body as EngineState;
      } catch {
        setError("Could not reach the engine. Is DATABASE_URL configured?");
        return null;
      }
    },
    []
  );

  // Initial load.
  useEffect(() => {
    let alive = true;
    call("/api/state").then((s) => {
      if (alive && s) setState(s);
    });
    return () => {
      alive = false;
    };
  }, [call]);

  // Turn the crank while anything is still moving. In the deployed system a background thread in
  // each service does this; a serverless platform has none, so the page that is watching does it.
  useEffect(() => {
    if (!state || busy) return;
    if (state.invariants.quiescent) return;
    if (rounds >= MAX_ROUNDS) return;

    const t = setTimeout(async () => {
      const next = await call("/api/tick", { method: "POST" });
      if (next) {
        setState(next);
        setRounds((r) => r + 1);
      }
    }, ROUND_MS);
    return () => clearTimeout(t);
  }, [state, rounds, busy, call]);

  const act = useCallback(
    async (url: string, init?: RequestInit) => {
      setBusy(true);
      setRounds(0);
      const next = await call(url, init);
      if (next) setState(next);
      setBusy(false);
    },
    [call]
  );

  const send = useCallback(
    async (reuseKey: boolean) => {
      const paise = Math.round(parseFloat(rupeeAmount || "0") * 100);
      if (!Number.isFinite(paise) || paise <= 0) {
        setError("Enter an amount greater than zero.");
        return;
      }
      const key = reuseKey && lastKey.current ? lastKey.current : crypto.randomUUID();
      lastKey.current = key;
      await act("/api/transfers", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Idempotency-Key": key },
        body: JSON.stringify({ fromOwner: "alice", toOwner: "bob", amountMinor: paise }),
      });
    },
    [act, rupeeAmount]
  );

  const setFault = useCallback(
    (patch: Record<string, unknown>) =>
      act("/api/faults", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      }),
    [act]
  );

  if (!state) {
    return (
      <div className="machine">
        <div className="m-head">
          <span className="m-title">Live engine</span>
        </div>
        <div className="empty" style={{ padding: "40px 16px" }}>
          {error ?? "Opening your ledger…"}
        </div>
      </div>
    );
  }

  const f = state.faults;
  const latest = state.transfers[0];
  const ledgerSum = state.ledger.reduce((a, e) => a + e.amountMinor, 0);
  const inFlight = !state.invariants.quiescent;
  const stalled = rounds >= MAX_ROUNDS && inFlight;
  const s2 = state.invariants.checks.find((c) => c.id === "S2");
  const brokenS2 = s2?.holds === false;

  const clearing = state.accounts
    .filter((a) => a.type === "CLEARING")
    .reduce((a, x) => a + x.balanceMinor, 0);

  return (
    <div className="machine">
      <div className="m-head">
        <span className="m-title">Live engine &mdash; your own ledger</span>
        <span className={`status-dot ${inFlight ? "busy" : "rest"}`}>
          <i />
          {inFlight ? "payment in flight" : "at rest"}
        </span>
      </div>

      {error && (
        <div className="banner bad">
          <b>{error}</b>
        </div>
      )}

      {brokenS2 && (
        <div className="banner bad">
          <b>S2 is failing, and every one of I1&ndash;I5 is green.</b> {s2?.detail} This is the
          defect the chaos suite found: the books balance perfectly and the payment is still wrong.
        </div>
      )}

      {f.broker_down && (
        <div className="banner warn">
          <b>The broker is down.</b> Commands are committed database rows waiting in the outbox.
          Notice that new payments are still accepted &mdash; a broker outage is not a
          customer-visible outage.
        </div>
      )}

      {stalled && !f.broker_down && (
        <div className="banner info">
          Nothing has moved for a while. The page stops turning the crank after a minute of your
          last action.{" "}
          <button className="ctl" style={{ marginLeft: 6 }} onClick={() => setRounds(0)}>
            Keep going
          </button>
        </div>
      )}

      {lastReplayed === true && (
        <div className="banner info">
          <b>That was a replay.</b> The same Idempotency-Key came back with the original payment
          instead of starting a second one. No new row was written.
        </div>
      )}

      {/* ------------------------------------------------------------ actions */}
      <div className="m-section">
        <h4>Move money</h4>
        <div className="row">
          <label className="check" htmlFor="amt" style={{ color: "var(--m-muted)" }}>
            Alice pays Bob
          </label>
          <input
            id="amt"
            className="amount"
            inputMode="decimal"
            value={rupeeAmount}
            onChange={(e) => setRupeeAmount(e.target.value)}
            aria-label="Amount in rupees"
          />
          <button className="ctl primary" onClick={() => send(false)} disabled={busy}>
            Send payment
          </button>
          <button className="ctl" onClick={() => send(true)} disabled={busy || !lastKey.current}>
            Send that again, same key
          </button>
          <button className="ctl" onClick={() => act("/api/reset", { method: "POST" })} disabled={busy}>
            Reset my ledger
          </button>
        </div>
        <p className="hint">
          Every click writes real rows to a real Postgres. The answer is 202 Accepted, not 200 OK
          &mdash; at that moment no money has moved.
        </p>
      </div>

      {/* ------------------------------------------------------------- faults */}
      <div className="m-section">
        <h4>Break it on purpose</h4>
        <div className="row" style={{ marginBottom: 10 }}>
          <span style={{ font: "500 12px/1 var(--font-mono)", color: "var(--m-dim)" }}>
            CARD PROCESSOR
          </span>
          {(["APPROVE", "DECLINE", "TIMEOUT"] as const).map((m) => (
            <button
              key={m}
              className={`seg${m !== "APPROVE" ? " danger" : ""}`}
              aria-pressed={f.gateway_mode === m}
              onClick={() => setFault({ gateway_mode: m })}
              disabled={busy}
            >
              {m === "APPROVE" ? "Approves" : m === "DECLINE" ? "Declines" : "Never answers"}
            </button>
          ))}
        </div>
        <div className="row" style={{ gap: "10px 18px" }}>
          <label className="check">
            <input
              type="checkbox"
              checked={f.broker_down}
              onChange={(e) => setFault({ broker_down: e.target.checked })}
              disabled={busy}
            />
            Kill the message broker
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={f.duplicate_replies}
              onChange={(e) => setFault({ duplicate_replies: e.target.checked })}
              disabled={busy}
            />
            Deliver every message twice
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={f.drop_first_commit}
              onChange={(e) => setFault({ drop_first_commit: e.target.checked })}
              disabled={busy}
            />
            Lose the commit, once
          </label>
        </div>
        <div className="row" style={{ marginTop: 12 }}>
          <span style={{ font: "500 12px/1 var(--font-mono)", color: "var(--m-dim)" }}>
            RECOVERY AFTER THE CHARGE
          </span>
          <button
            className="seg"
            aria-pressed={f.forward_recovery}
            onClick={() => setFault({ forward_recovery: true })}
            disabled={busy}
          >
            Finish it
          </button>
          <button
            className="seg danger"
            aria-pressed={!f.forward_recovery}
            onClick={() => setFault({ forward_recovery: false })}
            disabled={busy}
          >
            Unwind it (the defect)
          </button>
        </div>
        <p className="hint">
          Tick <em>Lose the commit, once</em>, set recovery to <em>Unwind it</em>, and send a
          payment. The card is charged, the commit never arrives, and the deadline unwinds a
          payment the card network has already taken the money for. I1&ndash;I5 all stay green;
          only S2 notices.
        </p>
      </div>

      {/* ----------------------------------------------------------- services */}
      <div className="m-section">
        <div className="svcs">
          <div className="svc">
            <div className="svc-n">payment-orchestrator</div>
            <div className="svc-s">
              <b style={{ color: sagaColor(latest?.sagaStatus) }}>
                {latest?.sagaStatus ?? "—"}
              </b>
              outbox {state.outbox.unpublished} unpublished
            </div>
          </div>
          <div className="svc">
            <div className="svc-n">account-service</div>
            <div className="svc-s">
              <b>
                {state.holdsActive} hold{state.holdsActive === 1 ? "" : "s"} active
              </b>
              {rupees(clearing)} in flight
            </div>
          </div>
          <div className="svc">
            <div className="svc-n">payment-gateway</div>
            <div className="svc-s">
              <b style={{ color: state.charges.approved ? "var(--m-ok)" : "var(--m-dim)" }}>
                {state.charges.approved} approved
              </b>
              {state.charges.declined} declined &middot; {state.charges.voided} voided
            </div>
          </div>
        </div>
      </div>

      {/* ---------------------------------------------------------- readouts */}
      <div className="readouts">
        <div className="pane">
          <h4>
            Ledger <em>{state.ledger.length} entries, append-only</em>
          </h4>
          <div className="scroll">
            <table className="led">
              <thead>
                <tr>
                  <th>Transfer</th>
                  <th>Account</th>
                  <th>Type</th>
                  <th className="num">Paise</th>
                </tr>
              </thead>
              <tbody>
                {state.ledger.map((e) => (
                  <tr key={e.id}>
                    <td className="muted">{shortId(e.transferId)}</td>
                    <td>{e.accountType === "CUSTOMER" ? e.account : e.account.toUpperCase()}</td>
                    <td className={e.entryType === "DEBIT" ? "dr" : "cr"}>{e.entryType}</td>
                    <td className={`num ${e.entryType === "DEBIT" ? "dr" : "cr"}`}>
                      {e.amountMinor > 0 ? "+" : ""}
                      {e.amountMinor.toLocaleString("en-IN")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="sum">
            <span>Sum of every entry &mdash; invariant I1</span>
            <b style={{ color: ledgerSum === 0 ? "var(--m-ok)" : "var(--m-bad)" }}>
              {ledgerSum === 0 ? "0 ✓" : ledgerSum}
            </b>
          </div>

          <h4 style={{ marginTop: 18 }}>
            Messages <em>rows in the outbox table</em>
          </h4>
          <ul className="log">
            {state.messages.length === 0 && <li className="empty">No messages yet.</li>}
            {state.messages.map((m) => (
              <li key={m.id}>
                <span className="ar">
                  {m.from} &rarr; {m.to}
                </span>
                <span className="nm">{m.eventType}</span>
                <span className={`tag ${m.consumed ? "done" : m.published ? "wait" : "wait"}`}>
                  {m.consumed ? "consumed" : m.published ? "published" : "in outbox"}
                </span>
              </li>
            ))}
          </ul>
        </div>

        <div className="pane">
          <h4>Balances</h4>
          <ul className="bals">
            {state.accounts
              .filter((a) => a.type !== "CLEARING")
              .map((a) => (
                <li key={a.id} className={a.type === "SYSTEM" ? "sys" : ""}>
                  <span className="who">
                    {a.owner}
                    {a.type === "SYSTEM" ? " (system)" : ""}
                  </span>
                  <span className="amt">{rupees(a.balanceMinor)}</span>
                </li>
              ))}
            <li className="sys">
              <span className="who">clearing (8 shards)</span>
              <span className="amt">{rupees(clearing)}</span>
            </li>
          </ul>

          <h4>Invariants</h4>
          <div className="inv-grid">
            {state.invariants.checks.map((c) => (
              <span
                key={c.id}
                className={`inv ${c.holds === null ? "" : c.holds ? "pass" : "fail"}`}
                title={`${c.title} — ${c.detail}`}
              >
                {c.id} {c.holds === null ? "—" : c.holds ? "✓" : "✕"}
              </span>
            ))}
          </div>
          <p className={`inv-detail${brokenS2 ? " bad" : ""}`}>
            {state.invariants.quiescent
              ? "The system is at rest, so every check is meaningful."
              : "I4 and S1 read — while a payment is in flight: a saga in RESERVED is correct now and a violation ten minutes from now."}
          </p>

          <h4 style={{ marginTop: 18 }}>
            Latest payment{" "}
            <em>{latest ? `${shortId(latest.id)} · ${rupees(latest.amountMinor)}` : ""}</em>
          </h4>
          {!latest && <p className="inv-detail">Nothing sent yet.</p>}
          {latest && (
            <ul className="timeline">
              {latest.steps.map((s, i) => (
                <li
                  key={i}
                  className={
                    s.outcome === "SUCCEEDED"
                      ? "ok"
                      : s.outcome === "FAILED"
                        ? "bad"
                        : s.outcome === "STARTED"
                          ? "pend"
                          : ""
                  }
                >
                  <b>{s.name}</b> <span>{s.outcome.toLowerCase()}</span>
                  {s.toStatus && <span> &rarr; {s.toStatus}</span>}
                  {s.detail && <span> &middot; {s.detail}</span>}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

function sagaColor(status?: string | null): string {
  switch (status) {
    case "COMPLETED":
      return "var(--m-ok)";
    case "COMPENSATED":
      return "var(--m-info)";
    case "FAILED":
      return "var(--m-bad)";
    case undefined:
    case null:
      return "var(--m-dim)";
    default:
      return "var(--m-warn)";
  }
}
