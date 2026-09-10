import { useState } from "react";
import { useAuth } from "./auth";
import { Chaos } from "./screens/Chaos";
import { Login } from "./screens/Login";
import { SystemView } from "./screens/SystemView";
import { TrackTransfer } from "./screens/TrackTransfer";

type Tab = "transfers" | "system" | "chaos";

interface TabSpec {
  id: Tab;
  label: string;
  /** The role the SERVER requires. Repeated here only to decide what to offer. */
  role: string;
  why: string;
}

/**
 * The three screens, each labelled with the role its endpoints require.
 *
 * A tab the current token cannot use is disabled rather than hidden, with the reason in its
 * tooltip. Hiding it would make the console look like it had three features for one identity and
 * two for another; disabling it says something true about the system - that OPERATOR sees every
 * operational surface and cannot move money, and that USER is the opposite - which is one of the
 * more interesting things M5 decided.
 *
 * To be explicit, because a reader should not have to guess: this is presentation. Every one of
 * these endpoints enforces its own rule against the token's signature, and clicking through to a
 * disabled screen by editing the hash would produce 403s, not access.
 */
const TABS: TabSpec[] = [
  {
    id: "transfers",
    label: "Transfers",
    role: "USER",
    why: "Moving money needs USER and ownership of the source account. An operator has neither.",
  },
  {
    id: "system",
    label: "System",
    role: "OPERATOR",
    why: "/admin/invariants on both services is OPERATOR-only.",
  },
  {
    id: "chaos",
    label: "Chaos",
    role: "OPERATOR",
    why: "/admin/simulation on the gateway is OPERATOR-only.",
  },
];

export function App() {
  const { session, hasRole, logout } = useAuth();

  // The first tab this identity can actually use, so an operator does not land on a screen that
  // 403s before they have clicked anything.
  const [tab, setTab] = useState<Tab>(() => "transfers");

  if (!session) return <Login />;

  const allowed = (spec: TabSpec) => hasRole(spec.role);
  const active = TABS.find((t) => t.id === tab);
  const usable = active && allowed(active) ? active : TABS.find(allowed);

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          DPE Console<span>distributed payment engine</span>
        </div>

        <nav className="tabs" role="tablist">
          {TABS.map((spec) => (
            <button
              key={spec.id}
              className="tab"
              role="tab"
              aria-selected={usable?.id === spec.id}
              disabled={!allowed(spec)}
              title={allowed(spec) ? undefined : `Needs ${spec.role}. ${spec.why}`}
              onClick={() => setTab(spec.id)}
            >
              {spec.label}
            </button>
          ))}
        </nav>

        <div className="who">
          <span>
            {session.username}{" "}
            {session.roles.map((role) => (
              <span key={role} className="badge neutral" style={{ marginLeft: 4 }}>
                {role}
              </span>
            ))}
          </span>
          <button className="ghost" onClick={logout}>
            Sign out
          </button>
        </div>
      </header>

      <main>
        {!usable ? (
          <div className="panel">
            <h2>Nothing to show</h2>
            <p className="sub">
              This token carries {session.roles.join(", ") || "no roles"}, and none of the three
              screens accepts it. That is the <code className="mono">denyAll()</code> default doing
              its job: an endpoint nobody wrote a rule for is reachable by nobody.
            </p>
          </div>
        ) : usable.id === "transfers" ? (
          <TrackTransfer />
        ) : usable.id === "system" ? (
          <SystemView />
        ) : (
          <Chaos />
        )}
      </main>
    </div>
  );
}
