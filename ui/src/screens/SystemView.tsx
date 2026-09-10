import { getInvariants, promFiringAlerts, promScalar } from "../api/client";
import type { SettledResult } from "../api/client";
import type { InvariantCheck, InvariantsResponse, PromSample } from "../api/types";
import { useAuth } from "../auth";
import { ErrorBox, Panel, Stat } from "../components";
import { money, stat } from "../format";
import { useAsync } from "../hooks";

/**
 * The operational screen: five invariants, the messaging backlogs, and whatever is firing.
 *
 * The five lights come from TWO endpoints, and that is the design rather than an inconvenience.
 * I1/I2/I3/I5 are statements about accounts_db and I4 is a statement about payments_db; a single
 * endpoint that answered all five would need a process holding credentials to both databases -
 * a shared-database architecture reintroduced through the monitoring door, and the
 * widest-access component in the estate, added for a dashboard. So each service answers for its
 * own database and this component does the join, holding no credentials but the operator's token.
 */
export function SystemView() {
  const { token } = useAuth();

  const invariants = useAsync(() => getInvariants(token), [], 5000);
  const messaging = useAsync(() => loadMessaging(), [], 5000);
  const alerts = useAsync(() => promFiringAlerts(), [], 10000);

  return (
    <>
      <FiringAlerts samples={alerts.data} />

      <Panel
        title="Invariants"
        sub="Two databases, two endpoints, joined here. Every chaos scenario and the k6 run end by asserting the same five."
      >
        <div className="grid two">
          <ServiceChecks result={invariants.data?.accounts} />
          <ServiceChecks result={invariants.data?.orchestrator} />
        </div>

        <Conservation result={invariants.data?.accounts} />
        <InFlight result={invariants.data?.orchestrator} />
      </Panel>

      <Panel
        title="Messaging"
        sub="Outbox age is the liveness signal, not backlog: a backlog of 5,000 that is draining is a busy system; a backlog of one that is not is a stopped relay."
      >
        <ErrorBox error={messaging.error} />
        <div className="stats">
          <Stat
            label="Outbox backlog"
            value={stat(messaging.data?.backlog ?? null)}
            note="rows written, not yet relayed"
          />
          <Stat
            label="Oldest un-relayed"
            value={
              messaging.data?.age === null || messaging.data?.age === undefined
                ? "—"
                : `${stat(messaging.data.age)}s`
            }
            note="alerts above 30s for 1m"
          />
          <Stat
            label="Dead letters"
            value={stat(messaging.data?.deadLetters ?? null)}
            note="unreplayed, all three services"
          />
          <Stat
            label="Sagas in flight"
            value={stat(messaging.data?.inFlight ?? null)}
            note="non-terminal, all states"
          />
        </div>

        <p className="note">
          These four come from Prometheus, not from a database query issued by this page. That is
          the same rule the backend holds itself to: a gauge must not run a query on the scrape
          path, so the services publish <code className="mono">AtomicLong</code> fields refreshed by
          a scheduler and a scrape is a memory read. A console that queried the write path every
          five seconds would be the observability becoming the outage.
        </p>
      </Panel>
    </>
  );
}

/**
 * The four messaging numbers, all from Prometheus.
 *
 * `admin/dead-letters/depth` would answer the third one too, and from the table rather than from a
 * gauge refreshed on a tick. It is deliberately not used here: it would have to be called once per
 * service and then summed in the browser, which is three requests to answer what one PromQL `sum`
 * already answers across all three. The per-service endpoint is the right tool when an operator
 * needs to know WHICH service is holding dead letters - which is what the dead letter screen is
 * for, not this tile.
 */
async function loadMessaging() {
  const [backlog, age, deadLetters, inFlight] = await Promise.all([
    promScalar("sum(dpe_outbox_backlog)"),
    promScalar("max(dpe_outbox_age_seconds)"),
    promScalar("sum(dpe_dlq_depth)"),
    promScalar("sum(dpe_saga_inflight)"),
  ]);

  return { backlog, age, deadLetters, inFlight };
}

/**
 * Whatever is currently firing, straight from the ALERTS series.
 *
 * There is no Alertmanager in this stack, so nothing is routed anywhere - but the rules still
 * evaluate and Prometheus still publishes ALERTS, which means the rules in
 * infra/prometheus/alerts.yml are testable and visible without a notification pipeline that has
 * nowhere to notify.
 */
function FiringAlerts({ samples }: { samples: PromSample[] | null }) {
  if (!samples || samples.length === 0) return null;

  return (
    <Panel title="Firing" sub="From the ALERTS series published by infra/prometheus/alerts.yml.">
      <div className="scroll-x">
        <table>
          <thead>
            <tr>
              <th>Alert</th>
              <th>Severity</th>
              <th>Scope</th>
            </tr>
          </thead>
          <tbody>
            {samples.map((sample, index) => (
              <tr key={index}>
                <td>
                  <strong>{sample.metric.alertname}</strong>
                </td>
                <td>
                  <span
                    className={`badge ${sample.metric.severity === "critical" ? "bad" : "warn"}`}
                  >
                    {sample.metric.severity ?? "—"}
                  </span>
                </td>
                <td className="muted mono">
                  {sample.metric.application ?? sample.metric.instance ?? sample.metric.state ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function ServiceChecks({ result }: { result?: SettledResult<InvariantsResponse> }) {
  if (!result) return <div className="spin">loading…</div>;

  if (!result.ok) {
    return (
      <div>
        <ErrorBox error={result.error} />
        <p className="muted">
          One database did not answer. The other half is still shown — an all-or-nothing fetch would
          blank the panel that says which half is broken.
        </p>
      </div>
    );
  }

  const { service, database, checks } = result.value;

  return (
    <div>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <strong>{service}</strong>
        <code className="mono dim">{database}</code>
      </div>
      {checks.map((check) => (
        <Check key={check.id} check={check} />
      ))}
    </div>
  );
}

/**
 * One invariant light.
 *
 * A check carrying `requiresQuiescence` is never painted red on a non-zero count, because it is not
 * a violation until the system is at rest and a browser cannot know when that is. I4 counts sagas
 * in flight, and sagas in flight are what a working system under load looks like.
 */
function Check({ check }: { check: InvariantCheck }) {
  const tone = check.holds ? "ok" : check.requiresQuiescence ? "warn" : "bad";
  const label = check.holds ? "holds" : check.requiresQuiescence ? "in flight" : "VIOLATED";

  return (
    <div className="row" style={{ justifyContent: "space-between", padding: "6px 0" }}>
      <div style={{ minWidth: 0 }}>
        <code className="mono">{check.id}</code>{" "}
        <span className="muted">{check.title}</span>
        <div className="dim" style={{ fontSize: 12 }}>
          {check.detail}
          {check.requiresQuiescence && !check.holds
            ? " — only a violation after quiescence"
            : ""}
        </div>
      </div>
      <span className={`badge ${tone}`}>{label}</span>
    </div>
  );
}

/**
 * I3, reported as a total and never as a verdict.
 *
 * Conservation is a statement about two instants and this page sees one. It cannot know when a
 * test run started, or whether an account was legitimately funded since. Five green lights with a
 * sixth that quietly asserts something it cannot check would be a lie in the one place this system
 * claims to prove something, so the number is shown and the judgement is left to whoever knows
 * what the number was before.
 */
function Conservation({ result }: { result?: SettledResult<InvariantsResponse> }) {
  if (!result?.ok || !result.value.conservation) return null;
  const { customerBalanceMinor, activeHoldsMinor, totalMinor } = result.value.conservation;

  return (
    <>
      <div className="stats" style={{ marginTop: 12 }}>
        <Stat label="Customer balances" value={money(customerBalanceMinor)} />
        <Stat label="Active holds" value={money(activeHoldsMinor)} note="parked in CLEARING" />
        <Stat label="I3 total" value={money(totalMinor)} note="constant across a run" />
      </div>
      <p className="note">
        I3 is the one check with no light. Conservation compares two instants and this page has one:
        it would take a reading from before the run to turn this number into a verdict, and a green
        light that could not actually have been checked is worse than no light at all.
      </p>
    </>
  );
}

/**
 * Non-terminal sagas by state.
 *
 * The total says something is stuck; the shape says which hop. STARTED means account-service is not
 * replying, RESERVED means the gateway is not answering, COMPENSATING means the release is not
 * landing.
 */
function InFlight({ result }: { result?: SettledResult<InvariantsResponse> }) {
  if (!result?.ok) return null;
  const inFlight = result.value.inFlight ?? {};
  const states = Object.entries(inFlight);
  if (states.length === 0) return null;

  return (
    <>
      <div className="row" style={{ marginTop: 12 }}>
        {states.map(([state, count]) => (
          <span key={state} className="badge info">
            {state}: {count}
          </span>
        ))}
      </div>
      <p className="note">
        Which state they are parked in names the hop that is not answering: STARTED means
        account-service has not replied, RESERVED means the gateway has not, COMPENSATING means the
        release is not landing.
      </p>
    </>
  );
}
