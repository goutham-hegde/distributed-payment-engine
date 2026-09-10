import { useEffect, useState } from "react";
import { getSimulation, updateSimulation } from "../api/client";
import type { SimulationUpdate, SimulationView } from "../api/types";
import { useAuth } from "../auth";
import { ErrorBox, Panel } from "../components";
import { useAsync } from "../hooks";

/**
 * The knobs on the simulated PSP.
 *
 * These are the gateway's own runtime-tunable failure injectors, not something this screen
 * invented - `POST /admin/simulation` has existed since the gateway did, so that the compensation
 * path could be forced on demand rather than waited for. The console is a set of buttons on top of
 * it.
 *
 * What makes this the most useful screen for demonstrating the system: turn the decline rate to
 * 1.0, send a payment on the Transfers tab, and watch the saga go
 * STARTED → RESERVED → COMPENSATING → COMPENSATED with the money returning to the sender - while
 * every invariant on the System tab stays green throughout, because compensation is not a rollback.
 */
export function Chaos() {
  const { token } = useAuth();
  const current = useAsync(() => getSimulation(token), [], 5000);

  return (
    <>
      <Panel
        title="Gateway simulation"
        sub="Runtime knobs on the simulated PSP. Absent fields are left alone, so raising the decline rate does not restate the latency."
      >
        <ErrorBox error={current.error} />
        {current.data ? <Knobs current={current.data} onApplied={current.refresh} token={token} /> : null}
      </Panel>

      <Panel title="Scenarios" sub="One click each. Every one is reversible with Reset.">
        {current.data ? <Presets onApplied={current.refresh} token={token} /> : null}

        <p className="note">
          <strong>Restore the fault only after the sagas are terminal.</strong> A scenario that
          resets the decline rate while a saga is still in flight has changed the conditions
          mid-experiment, and the result describes neither the injected fault nor the healthy
          system. Watch the in-flight count on the System tab reach zero first — the same rule the
          M7 chaos scripts will have to follow.
        </p>
      </Panel>
    </>
  );
}

function Knobs({
  current,
  onApplied,
  token,
}: {
  current: SimulationView;
  onApplied: () => void;
  token: () => string | null;
}) {
  const [draft, setDraft] = useState<SimulationView>(current);
  const [error, setError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);

  // The poll keeps `current` fresh, and the draft follows it while the user is not mid-edit.
  // Without this the form would silently show a stale value after somebody else (a chaos script,
  // say) moved a knob - and the operator would apply an old number believing it was the current
  // one.
  useEffect(() => {
    if (!busy) setDraft(current);
  }, [current, busy]);

  const apply = async () => {
    setBusy(true);
    setError(null);
    try {
      await updateSimulation(token, draft);
      onApplied();
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <div className="row">
        <Knob
          label="Decline rate"
          hint="fraction of charges refused → compensation"
          value={draft.failureRate}
          onChange={(failureRate) => setDraft({ ...draft, failureRate })}
        />
        <Knob
          label="Timeout rate"
          hint="fraction that never answer → sweeper compensates"
          value={draft.timeoutRate}
          onChange={(timeoutRate) => setDraft({ ...draft, timeoutRate })}
        />
        <Knob
          label="Duplicate callbacks"
          hint="fraction replied to twice → the inbox absorbs it"
          value={draft.duplicateCallbackRate}
          onChange={(duplicateCallbackRate) => setDraft({ ...draft, duplicateCallbackRate })}
        />
        <div className="field" style={{ flexBasis: 140, flexGrow: 0 }}>
          <label htmlFor="latency">Latency (ms)</label>
          <input
            id="latency"
            type="number"
            min={0}
            value={draft.latencyMs}
            onChange={(e) => setDraft({ ...draft, latencyMs: Number(e.target.value) })}
          />
          <div className="dim" style={{ fontSize: 11.5, marginTop: 4 }}>
            added to every charge
          </div>
        </div>
      </div>

      <div className="row" style={{ marginTop: 12 }}>
        <button onClick={apply} disabled={busy}>
          {busy ? "Applying…" : "Apply"}
        </button>
        <span className="dim" style={{ fontSize: 12 }}>
          live: decline {pct(current.failureRate)} · timeout {pct(current.timeoutRate)} · duplicate{" "}
          {pct(current.duplicateCallbackRate)} · latency {current.latencyMs}ms
        </span>
      </div>

      <ErrorBox error={error} />
    </>
  );
}

function Knob({
  label,
  hint,
  value,
  onChange,
}: {
  label: string;
  hint: string;
  value: number;
  onChange: (value: number) => void;
}) {
  return (
    <div className="field">
      <label>
        {label} — <strong>{pct(value)}</strong>
      </label>
      <input
        type="range"
        min={0}
        max={1}
        step={0.05}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
      />
      <div className="dim" style={{ fontSize: 11.5 }}>
        {hint}
      </div>
    </div>
  );
}

interface Preset {
  name: string;
  detail: string;
  update: SimulationUpdate;
}

/**
 * Named scenarios rather than raw sliders, because the point of each one is a claim about the
 * system, and the claim is what should be on the button.
 */
const PRESETS: Preset[] = [
  {
    name: "Healthy",
    detail: "Every knob off. The baseline every scenario is measured against.",
    update: { failureRate: 0, timeoutRate: 0, duplicateCallbackRate: 0, latencyMs: 0 },
  },
  {
    name: "Every charge declined",
    detail:
      "failureRate 1.0. Forces the compensation path: RESERVED → COMPENSATING → COMPENSATED, the hold released back to the sender, and I1–I5 unchanged throughout.",
    update: { failureRate: 1.0, timeoutRate: 0 },
  },
  {
    name: "Gateway goes silent",
    detail:
      "timeoutRate 1.0. No reply ever arrives, so the timeout sweeper compensates on the deadline — and the late reply, if it ever lands, is absorbed by the inbox rather than double-spending.",
    update: { failureRate: 0, timeoutRate: 1.0 },
  },
  {
    name: "Duplicate callbacks",
    detail:
      "duplicateCallbackRate 1.0. Every reply is delivered twice. Nothing changes, which is the whole point: the inbox's primary key on message id makes the second delivery a no-op.",
    update: { duplicateCallbackRate: 1.0, failureRate: 0, timeoutRate: 0 },
  },
  {
    name: "Slow gateway (2s)",
    detail:
      "latencyMs 2000. Sagas sit in RESERVED for two seconds each — visible as a rising in-flight gauge that comes back down, which is what a busy system looks like and must not be alerted on.",
    update: { latencyMs: 2000, failureRate: 0, timeoutRate: 0 },
  },
];

function Presets({ onApplied, token }: { onApplied: () => void; token: () => string | null }) {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);

  const run = async (preset: Preset) => {
    setBusy(preset.name);
    setError(null);
    try {
      await updateSimulation(token, preset.update);
      onApplied();
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    } finally {
      setBusy(null);
    }
  };

  return (
    <>
      <ErrorBox error={error} />
      <div className="grid two">
        {PRESETS.map((preset) => (
          <div key={preset.name} className="stat">
            <div className="row" style={{ justifyContent: "space-between" }}>
              <strong>{preset.name}</strong>
              <button className="ghost" onClick={() => run(preset)} disabled={busy !== null}>
                {busy === preset.name ? "…" : "Apply"}
              </button>
            </div>
            <div className="dim" style={{ fontSize: 12, marginTop: 6 }}>
              {preset.detail}
            </div>
          </div>
        ))}
      </div>
    </>
  );
}

function pct(value: number): string {
  return `${Math.round(value * 100)}%`;
}
