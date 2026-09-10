import { useState } from "react";
import { createTransfer, getLedger, getTimeline, listTransfers } from "../api/client";
import type { LedgerPage, Stage, TimelineResponse } from "../api/types";
import { useAuth } from "../auth";
import { ErrorBox, Id, Panel, Status } from "../components";
import { dateTime, millis, money, shortId, time } from "../format";
import { useAsync, useHashRoute } from "../hooks";

export function TrackTransfer() {
  const [selected, select] = useHashRoute();
  return selected ? (
    <Timeline transferId={selected} onBack={() => select(null)} />
  ) : (
    <>
      <NewTransfer onCreated={select} />
      <TransferList onSelect={select} />
    </>
  );
}

// -------------------------------------------------------------------------------------------- new

/**
 * Start a transfer, and demonstrate the idempotency gate while doing it.
 *
 * The key is generated once per ATTEMPT and held until the attempt succeeds - not regenerated on
 * every render, and not regenerated on a retry. That is the entire meaning of the header: a client
 * that mints a fresh key when a request times out has told the server "this is a different
 * payment", and the server will believe it. Reusing the key across the retry is what makes the
 * retry safe, and it is the mistake this widget is shaped to avoid making.
 *
 * The "Send again with the same key" button exists to show the other half live: it replays,
 * returns the first response byte for byte, writes nothing, and the console can only know it was a
 * replay because of the `Idempotency-Replayed` header.
 */
function NewTransfer({ onCreated }: { onCreated: (id: string) => void }) {
  const { token } = useAuth();

  const [fromAccountId, setFrom] = useState("");
  const [toAccountId, setTo] = useState("");
  const [amountMajor, setAmount] = useState("100.00");
  const [key, setKey] = useState(() => crypto.randomUUID());

  const [result, setResult] = useState<{ id: string; replayed: boolean; status: number } | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      // Parsed to minor units HERE and sent as an integer. The float exists for exactly the length
      // of this expression and never reaches the wire.
      const amountMinor = Math.round(Number(amountMajor) * 100);
      const accepted = await createTransfer(token, key, {
        fromAccountId,
        toAccountId,
        amountMinor,
        currency: "INR",
      });
      setResult({
        id: accepted.transfer.transferId,
        replayed: accepted.replayed,
        status: accepted.status,
      });
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Send a payment"
      sub="Accepted with 202 and a saga id. The money has not moved yet — that is what the timeline is for."
    >
      <form onSubmit={submit}>
        <div className="row">
          <div className="field">
            <label htmlFor="from">From account</label>
            <input
              id="from"
              className="mono"
              placeholder="uuid"
              value={fromAccountId}
              onChange={(e) => setFrom(e.target.value.trim())}
            />
          </div>
          <div className="field">
            <label htmlFor="to">To account</label>
            <input
              id="to"
              className="mono"
              placeholder="uuid"
              value={toAccountId}
              onChange={(e) => setTo(e.target.value.trim())}
            />
          </div>
          <div className="field" style={{ flexBasis: 130, flexGrow: 0 }}>
            <label htmlFor="amount">Amount (₹)</label>
            <input id="amount" value={amountMajor} onChange={(e) => setAmount(e.target.value)} />
          </div>
        </div>

        <div className="row" style={{ marginTop: 12 }}>
          <button type="submit" disabled={busy || !fromAccountId || !toAccountId}>
            {busy ? "Sending…" : "Send"}
          </button>
          <button
            type="button"
            className="ghost"
            onClick={() => {
              setKey(crypto.randomUUID());
              setResult(null);
            }}
          >
            New idempotency key
          </button>
          <span className="dim mono" style={{ fontSize: 12 }}>
            Idempotency-Key: {key}
          </span>
        </div>
      </form>

      <ErrorBox error={error} />

      {result ? (
        <div className="note">
          <strong>{result.status}</strong> — transfer{" "}
          <a
            href={`#/transfer/${result.id}`}
            onClick={(e) => {
              e.preventDefault();
              onCreated(result.id);
            }}
          >
            {shortId(result.id)}
          </a>{" "}
          {result.replayed ? (
            <>
              <span className="badge warn">replayed</span> — the gate recognised this key, returned
              the first response unchanged and wrote nothing. Press <em>Send</em> again to see it
              stay replayed; press <em>New idempotency key</em> to make a second payment.
            </>
          ) : (
            <>
              <span className="badge ok">first delivery</span> — the claim, the transfer, the saga
              and the ReserveFunds outbox row all committed in one transaction. Press{" "}
              <em>Send</em> again with the same key to watch the gate refuse to do it twice.
            </>
          )}
        </div>
      ) : null}
    </Panel>
  );
}

// ------------------------------------------------------------------------------------------- list

/**
 * The caller's own transfers, keyset-paged.
 *
 * Cursors are kept as a STACK rather than a page number, because that is what keyset pagination
 * actually gives you: you can always go forward from where you are, and back to somewhere you have
 * been, but there is no "page 7" to jump to. A UI offering numbered pages over a keyset API is
 * promising something the API cannot do - and offering them over an OFFSET API would be promising
 * something the database cannot do correctly while rows are being inserted above the window.
 */
function TransferList({ onSelect }: { onSelect: (id: string) => void }) {
  const { token } = useAuth();
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const cursor = cursors[cursors.length - 1];

  const { data, error, loading, refresh } = useAsync(
    () => listTransfers(token, cursor, 20),
    [cursor],
    5000,
  );

  return (
    <Panel
      title="Your transfers"
      sub="Newest first, paged on (created_at, id). Scoped by initiated_by in the WHERE clause — never filtered after the LIMIT."
      right={
        <button className="ghost" onClick={refresh}>
          Refresh
        </button>
      }
    >
      <ErrorBox error={error} />

      <div className="scroll-x">
        <table>
          <thead>
            <tr>
              <th>Transfer</th>
              <th>From</th>
              <th>To</th>
              <th style={{ textAlign: "right" }}>Amount</th>
              <th>Status</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {data?.items.map((t) => (
              <tr key={t.transferId} className="clickable" onClick={() => onSelect(t.transferId)}>
                <td>
                  <Id value={t.transferId} />
                </td>
                <td>
                  <Id value={t.fromAccountId} />
                </td>
                <td>
                  <Id value={t.toAccountId} />
                </td>
                <td style={{ textAlign: "right" }} className="mono">
                  {money(t.amountMinor, t.currency)}
                </td>
                <td>
                  <Status value={t.status} />
                  {t.failureReason ? <div className="dim">{t.failureReason}</div> : null}
                </td>
                <td className="muted">{dateTime(t.createdAt)}</td>
              </tr>
            ))}
            {data && data.items.length === 0 ? (
              <tr>
                <td colSpan={6} className="muted">
                  No transfers for this subject yet.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <div className="row" style={{ marginTop: 12 }}>
        <button
          className="ghost"
          disabled={cursors.length === 1}
          onClick={() => setCursors((s) => s.slice(0, -1))}
        >
          ← Previous
        </button>
        <button
          className="ghost"
          disabled={!data?.nextCursor}
          onClick={() => setCursors((s) => [...s, data!.nextCursor])}
        >
          Next →
        </button>
        {loading ? <span className="spin">loading…</span> : null}
      </div>
    </Panel>
  );
}

// --------------------------------------------------------------------------------------- timeline

const TERMINAL = new Set(["COMPLETED", "COMPENSATED", "FAILED"]);

function Timeline({ transferId, onBack }: { transferId: string; onBack: () => void }) {
  const { token } = useAuth();

  const { data, error, loading } = useAsync(
    () => getTimeline(token, transferId),
    [transferId],
    // Polled while in flight and left alone once terminal. A terminal saga's timeline cannot
    // change - COMPLETED, COMPENSATED and FAILED are absorbing states - so continuing to poll it
    // would be load with no possible new information.
    2000,
  );

  const settled = data ? TERMINAL.has(data.saga.status) : false;

  return (
    <>
      <div className="row" style={{ marginBottom: 16 }}>
        <button className="ghost" onClick={onBack}>
          ← All transfers
        </button>
        {loading && !data ? <span className="spin">loading…</span> : null}
        {data && !settled ? <span className="spin">in flight — polling every 2s</span> : null}
      </div>

      <ErrorBox error={error} />

      {data ? (
        <>
          <Summary timeline={data} />
          <Stages stages={data.stages} />
          <MoneyView timeline={data} />
        </>
      ) : null}
    </>
  );
}

function Summary({ timeline }: { timeline: TimelineResponse }) {
  const { transfer, saga, traceId } = timeline;

  return (
    <Panel
      title={`Transfer ${shortId(transfer.transferId)}`}
      sub={
        <>
          {money(transfer.amountMinor, transfer.currency)} from{" "}
          <code className="mono">{shortId(transfer.fromAccountId)}</code> to{" "}
          <code className="mono">{shortId(transfer.toAccountId)}</code>
        </>
      }
      right={
        traceId ? (
          // The deep link that M6 part 2 was built to make possible: one payment is one trace,
          // spanning three services and the relay hop between them, because the trace context
          // travelled as a COLUMN on the outbox row rather than on the sending thread.
          <a
            className="badge info"
            href={`http://localhost:16686/trace/${traceId}`}
            target="_blank"
            rel="noreferrer"
            title="Open this payment's distributed trace in Jaeger"
          >
            Open trace in Jaeger ↗
          </a>
        ) : null
      }
    >
      <div className="stats">
        <div className="stat">
          <div className="k">Transfer status</div>
          <div className="v" style={{ fontSize: 16 }}>
            <Status value={transfer.status} />
          </div>
        </div>
        <div className="stat">
          <div className="k">Saga status</div>
          <div className="v" style={{ fontSize: 16 }}>
            <Status value={saga.status} />
          </div>
          <div className="n">{TERMINAL.has(saga.status) ? "terminal" : "in flight"}</div>
        </div>
        <div className="stat">
          <div className="k">Hold</div>
          <div className="v" style={{ fontSize: 16 }}>
            <Id value={saga.holdId} />
          </div>
          <div className="n">money parked in CLEARING</div>
        </div>
        <div className="stat">
          <div className="k">Gateway charge</div>
          <div className="v" style={{ fontSize: 16 }}>
            <Id value={saga.gatewayChargeId} />
          </div>
        </div>
      </div>

      {saga.failureReason ? <div className="note">Failure reason: {saga.failureReason}</div> : null}

      {saga.status === "COMPENSATED" || saga.status === "COMPENSATING" ? (
        <div className="note">
          <strong>Returned to sender.</strong> The reserve succeeded and something downstream said
          no, so the saga ran its compensation: the hold in CLEARING was released back to the
          sender. Nothing was rolled back — a committed transaction cannot be — a second, opposite
          set of ledger entries was written, and the invariants held throughout.
        </div>
      ) : null}

      {saga.timedOutAt ? (
        <div className="note">
          Timed out at {dateTime(saga.timedOutAt)} (deadline {dateTime(saga.deadlineAt)}). The
          sweeper compensated it rather than waiting: a reply that has not arrived by the deadline
          may still arrive, and the inbox is what makes that safe.
        </div>
      ) : null}
    </Panel>
  );
}

/**
 * The stage timeline, with the message behind each stage.
 *
 * `saga_steps` holds two rows per step - one when the command goes out, one when the reply comes
 * back - which is why per-stage latency is a fact the server can report rather than something this
 * component has to estimate from timestamps it happens to have.
 *
 * `relayLagMs` is shown separately from the stage latency on purpose. It is the interval between
 * the outbox row committing and the relay actually publishing it, and it is the number that would
 * silently vanish if the trace context had been copied straight onto the record instead of the
 * relay opening a child span. A broker stall would then read as a slow producer.
 */
function Stages({ stages }: { stages: Stage[] }) {
  return (
    <Panel
      title="Stages"
      sub="Each step as the orchestrator recorded it, with the outbox row that carried the command and the inbox row that absorbed the reply."
    >
      {stages.length === 0 ? <p className="muted">No steps recorded yet.</p> : null}

      {stages.map((stage, index) => {
        const tone =
          stage.outcome === "SUCCEEDED" || stage.outcome === "SUCCESS"
            ? "ok"
            : stage.outcome === "FAILED" || stage.outcome === "DECLINED"
              ? "bad"
              : stage.endedAt === null
                ? "pending"
                : "warn";

        return (
          <div className="stage" key={`${stage.name}-${stage.startedAt}-${index}`}>
            <div className="rail">
              <div className={`dot ${tone}`} />
              {index < stages.length - 1 ? <div className="line" /> : null}
            </div>

            <div className="stage-body">
              <div className="stage-head">
                <span className="stage-name">{stage.name}</span>
                <Status value={stage.outcome} />
                {stage.toStatus ? <span className="dim">→ {stage.toStatus}</span> : null}
                <span className="dim mono">
                  {time(stage.startedAt)}
                  {stage.endedAt ? ` → ${time(stage.endedAt)}` : " → …"}
                </span>
                {stage.latencyMs !== null ? (
                  <span className="badge neutral">{millis(stage.latencyMs)}</span>
                ) : null}
              </div>

              {stage.detail ? <div className="muted">{stage.detail}</div> : null}

              {stage.command || stage.reply ? (
                <div className="msg">
                  {stage.command ? (
                    <>
                      <div className="hop">
                        outbox → <strong>{stage.command.eventType}</strong> →{" "}
                        {stage.command.topic}
                      </div>
                      <div>
                        message {shortId(stage.command.messageId)} · queued{" "}
                        {time(stage.command.queuedAt)} · published{" "}
                        {time(stage.command.publishedAt)} · relay lag{" "}
                        <strong>{millis(stage.command.relayLagMs)}</strong>
                      </div>
                    </>
                  ) : null}

                  {stage.reply ? (
                    <div className="hop">
                      inbox ← <strong>{stage.reply.eventType}</strong> ← {stage.reply.topic} ·
                      received {time(stage.reply.receivedAt)}
                      {stage.command && stage.reply.messageId === stage.command.messageId ? (
                        <span className="dim"> · same message id, end to end</span>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          </div>
        );
      })}
    </Panel>
  );
}

/**
 * Where the money is, read from the ledger rather than described.
 *
 * Both accounts are fetched with their recent entries so the CLEARING legs are visible: a reserve
 * debits the sender and credits CLEARING, and commit and release BOTH debit CLEARING. Money in
 * flight is never nowhere - that is the property that lets I1 and I3 both hold, and it is much
 * easier to believe when the rows are on screen.
 *
 * The recipient's account is fetched too, and it is worth knowing that this can legitimately fail
 * with 403: the caller owns the source account, not necessarily the destination. The panel says so
 * rather than hiding the row, because the refusal is itself the demonstration.
 */
function MoneyView({ timeline }: { timeline: TimelineResponse }) {
  const { token } = useAuth();
  const { transfer } = timeline;

  const from = useAsync(() => getLedger(token, transfer.fromAccountId, null, 6), [transfer.fromAccountId], 4000);
  const to = useAsync(() => getLedger(token, transfer.toAccountId, null, 6), [transfer.toAccountId], 4000);

  return (
    <Panel
      title="The money"
      sub="The ledger rows themselves. A reserve debits the sender and credits CLEARING; commit and release both debit CLEARING — which is why completing and compensating the same transfer are structurally exclusive."
    >
      <div className="grid two">
        <LedgerCard title="Sender" page={from.data} error={from.error} highlight={transfer.transferId} />
        <LedgerCard title="Recipient" page={to.data} error={to.error} highlight={transfer.transferId} />
      </div>
    </Panel>
  );
}

function LedgerCard({
  title,
  page,
  error,
  highlight,
}: {
  title: string;
  page: LedgerPage | null;
  error: Error | null;
  highlight: string;
}) {
  return (
    <div>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <strong>{title}</strong>
        {page ? (
          <span className="mono">{money(page.balanceMinor, page.currency)}</span>
        ) : null}
      </div>

      <ErrorBox error={error} />

      {page ? (
        <div className="scroll-x">
          <table>
            <thead>
              <tr>
                <th>Entry</th>
                <th>Type</th>
                <th style={{ textAlign: "right" }}>Amount</th>
                <th>At</th>
              </tr>
            </thead>
            <tbody>
              {page.entries.map((entry) => (
                <tr
                  key={entry.id}
                  style={
                    entry.transferId === highlight
                      ? { background: "color-mix(in srgb, var(--accent) 10%, transparent)" }
                      : undefined
                  }
                >
                  <td className="mono">{entry.id}</td>
                  <td>
                    <span className={`badge ${entry.entryType === "CREDIT" ? "ok" : "neutral"}`}>
                      {entry.entryType}
                    </span>
                  </td>
                  <td style={{ textAlign: "right" }} className="mono">
                    {money(entry.amountMinor, entry.currency)}
                  </td>
                  <td className="muted">{time(entry.createdAt)}</td>
                </tr>
              ))}
              {page.entries.length === 0 ? (
                <tr>
                  <td colSpan={4} className="muted">
                    No entries.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
