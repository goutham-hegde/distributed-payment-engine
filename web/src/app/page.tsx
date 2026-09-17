import Console from "@/components/Console";

export default function Page() {
  return (
    <>
      <header className="wrap masthead">
        <div className="rule-under">
          <div className="kicker">
            <span>Distributed Systems &middot; Portfolio Project</span>
            <span>Java 21 &middot; Spring Boot 4.1 &middot; Kafka &middot; PostgreSQL</span>
          </div>
        </div>

        <h1>
          Distributed
          <br />
          Payment Engine
        </h1>

        <p className="claim">
          Money is never created and never destroyed. <em>Only moved.</em>
        </p>

        <div className="measure" style={{ marginTop: 26 }}>
          <p className="lede">
            A money-transfer system split across three independently-failing services, built to
            hold that one guarantee under failure &mdash; and then broken on purpose, eight
            different ways, to find out where it didn&rsquo;t.
          </p>
          <p>
            The engine below is not a recording. It is a working implementation of the same design
            &mdash; the same saga, the same double-entry ledger, the same outbox and inbox &mdash;
            running against a real PostgreSQL database. Every payment you send writes real rows, and
            the invariants are SQL queries over them, not a verdict the page decided in advance.
          </p>
        </div>

        <div className="links">
          <a className="btn" href="https://github.com/goutham-hegde/distributed-payment-engine">
            View the source
          </a>
          <a className="btn ghost" href="#engine">
            Move some money
          </a>
        </div>
      </header>

      <section className="wrap band" id="engine">
        <div className="measure">
          <span className="eyebrow">Try to break it</span>
          <h2>Send a payment. Then stop it working.</h2>
          <p>
            You have your own ledger here, isolated from everyone else&rsquo;s, opened with
            &#8377;1,000 for Alice and &#8377;500 for Bob. Send a payment and watch it settle; then
            throw one of the fault switches and send another. The system is allowed to fail. It is
            not allowed to lose money.
          </p>
        </div>

        <Console />

        <p className="tbl-note measure">
          A debit is stored as a negative number and a credit as a positive one, so &ldquo;no money
          was created&rdquo; is a plain <code>SUM()</code> over one column rather than a rule
          somebody has to remember. The uniqueness constraint on{" "}
          <code>(transfer, account, entry_type)</code> is what makes completing and compensating the
          same payment mutually exclusive &mdash; in the schema, not by the orchestrator&rsquo;s
          promise.
        </p>
      </section>

      <section className="wrap band">
        <div className="measure">
          <span className="eyebrow">The problem</span>
          <h2>In one database this is trivial. Across three services it is not.</h2>
          <p>
            Alice sends Bob &#8377;300. Inside a single database that is one transaction and there
            is nothing to discuss. Split the work across three services that can each fail on their
            own, and the account service can crash after debiting Alice and before crediting Bob; a
            message can be dropped, or delivered twice; two payments can read the same balance at
            once; the card processor can time out with nobody knowing whether it charged.
          </p>
          <p>
            This project implements the standard industrial answer to each &mdash; saga
            orchestration, a transactional outbox and inbox, idempotent consumers, a double-entry
            ledger &mdash; and then attacks it to find out which answers were incomplete. Five of
            them were.
          </p>
        </div>
      </section>

      <section className="wrap band">
        <div className="measure">
          <span className="eyebrow">How correctness is proven</span>
          <h2>Not by HTTP 200s</h2>
          <p>
            A service can answer every request successfully and still have lost your money. Five
            invariants are asserted after every fault injection and every load run:
          </p>
        </div>

        <div className="tbl-scroll">
          <table className="data">
            <thead>
              <tr>
                <th>&nbsp;</th>
                <th>Invariant</th>
                <th>In plain terms</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="n">I1</td>
                <td className="n">SUM(ledger_entries) = 0</td>
                <td>No money was created or destroyed anywhere in the system.</td>
              </tr>
              <tr>
                <td className="n">I2</td>
                <td className="n">balance = SUM(its entries)</td>
                <td>Every stored balance still agrees with the entries behind it.</td>
              </tr>
              <tr>
                <td className="n">I3</td>
                <td className="n">balances + holds constant</td>
                <td>The total is the same at the end of a run as at the start.</td>
              </tr>
              <tr>
                <td className="n">I4</td>
                <td className="n">no saga left non-terminal</td>
                <td>Nothing is stuck half-done once the system goes quiet.</td>
              </tr>
              <tr>
                <td className="n">I5</td>
                <td className="n">no customer balance &lt; 0</td>
                <td>Nobody was overdrawn, however concurrent the traffic was.</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div className="measure" style={{ marginTop: 28 }}>
          <h3>The five invariants turned out not to be enough</h3>
          <p>
            The chaos suite&rsquo;s first run found five saga defects. Only one tripped any of
            I1&ndash;I5. The rest passed all five while money sat stranded mid-flight, or while the
            card network held a charge for a payment that had been refunded. Money can be perfectly
            conserved inside your own books and still be wrong about the outside world. Four more
            checks now run alongside them &mdash; S1 to S4 in the panel above &mdash; each joining
            two services&rsquo; databases.
          </p>
        </div>
      </section>

      <section className="wrap band">
        <div className="measure">
          <span className="eyebrow">What breaking it found</span>
          <h2>Five defects, in code whose tests were all green</h2>
        </div>

        <ol className="finds measure">
          <li>
            <h3>A refund that paid the customer out of our own books</h3>
            <p>
              A timeout <em>after</em> the card was charged recovered the same way as a timeout
              before it: by unwinding. The customer got their money back and the card network kept
              the charge. Every one of I1&ndash;I5 stayed green throughout.
              <span className="fix">
                The charge is now the saga&rsquo;s pivot. Before it, recovery runs backward; after
                it, only forward. You can reproduce this one yourself in the panel above.
              </span>
            </p>
          </li>
          <li>
            <h3>A participant that answered with silence</h3>
            <p>
              Asked to act on a hold that had already been settled, the account service logged the
              fact and replied with nothing, so the saga could only ever learn from its own
              deadline.
              <span className="fix">
                It now replies with what actually happened to the hold. A reply is a statement of
                fact, not an acknowledgement &mdash; and silence was never the third option.
              </span>
            </p>
          </li>
          <li>
            <h3>A compensation that raced the step it was compensating</h3>
            <p>
              A timeout is decided from a clock, not from the message order, so it can fire before
              the step it is cancelling has even arrived. The cancellation sent nothing, the late
              step landed anyway, and the money stayed stranded mid-flight forever.
              <span className="fix">
                Compensations are addressed by transfer and leave a tombstone behind, so they
                commute with the step they compensate &mdash; whichever arrives first.
              </span>
            </p>
          </li>
          <li>
            <h3>A replayed dead letter that charged a cancelled payment</h3>
            <p>
              A message rescued from the dead-letter queue and replayed went on to charge a payment
              that had already been refunded.
              <span className="fix">
                Fixed the same way, at the gateway: a cancelled payment leaves a tombstone that a
                later charge collides with instead of overwriting.
              </span>
            </p>
          </li>
          <li>
            <h3>A restarted service that timed out payments it could already hear</h3>
            <p>
              After a crash the orchestrator served traffic and ran its timeout sweeper for roughly
              23 seconds before its message consumer rejoined the group &mdash; and failed payments
              whose replies were sitting unread, waiting for it.
              <span className="fix">
                The sweeper now waits until the reply listener actually holds its partitions. A
                service must not declare things dead while it is deaf.
              </span>
            </p>
          </li>
        </ol>
      </section>

      <section className="wrap band">
        <div className="measure">
          <span className="eyebrow">Results from the deployed system</span>
          <h2>Eight faults, a thousand users, and a rolling deploy</h2>
          <p>
            These are from the Java system running on Docker Compose and Kubernetes, not from the
            panel above. Every figure comes from a recorded run, with the command that produced it,
            in the engineering log in the repository.
          </p>
        </div>

        <div className="tbl-scroll">
          <table className="data">
            <thead>
              <tr>
                <th>1,000 concurrent users</th>
                <th>At rated capacity</th>
                <th>At roughly twice capacity</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Payments settled</td>
                <td className="n held">7,307 of 7,307</td>
                <td className="n held">17,974 of 17,974 accepted</td>
              </tr>
              <tr>
                <td>Create latency, p99</td>
                <td className="n">58 ms</td>
                <td className="n">228 ms</td>
              </tr>
              <tr>
                <td>Settle latency, p99</td>
                <td className="n">3.55 s</td>
                <td className="n">&le; 5.4 s server-side</td>
              </tr>
              <tr>
                <td>Turned away up front</td>
                <td className="n">0</td>
                <td className="n">48,345</td>
              </tr>
              <tr>
                <td>Every invariant</td>
                <td className="n held">pass</td>
                <td className="n held">pass</td>
              </tr>
            </tbody>
          </table>
        </div>

        <p className="tbl-note measure">
          The overloaded run is the interesting one. The system stays correct and stays fast{" "}
          <em>for what it accepts</em>, and refuses the excess at the front door instead of
          accepting it and timing it out thirty seconds later. Getting there meant finding a
          metastable collapse first: past about 15 payments a second, customers&rsquo; status polls
          took every database connection the payment pipeline needed, payments timed out, their
          compensations added more work, and the system stayed down after the load stopped &mdash;
          with every paisa still conserved. Capacity went from ~15/s to ~38/s.
        </p>

        <div className="tbl-scroll">
          <table className="data">
            <thead>
              <tr>
                <th>Kubernetes, under load</th>
                <th>Failed requests</th>
                <th>Payments settled</th>
                <th>Invariants</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Rolling restart of all three services</td>
                <td className="n held">0 of 6,874</td>
                <td className="n">1,298 of 1,298</td>
                <td className="n held">pass</td>
              </tr>
              <tr>
                <td>Hard kill of an orchestrator</td>
                <td className="n">2 of 7,343</td>
                <td className="n">1,139 of 1,139</td>
                <td className="n held">pass</td>
              </tr>
              <tr>
                <td>An orchestrator frozen mid-flight, 30 s deadline</td>
                <td className="n">9 of 6,279</td>
                <td className="n">1,053 of 1,066</td>
                <td className="n held">pass</td>
              </tr>
              <tr>
                <td>The same freeze, 60 s deadline</td>
                <td className="n held">0 of 6,138</td>
                <td className="n">1,050 of 1,050</td>
                <td className="n held">pass</td>
              </tr>
            </tbody>
          </table>
        </div>

        <p className="tbl-note measure">
          The third row is the one worth reading. Thirteen payments whose replies had already been
          sent were sitting unread on a frozen replica&rsquo;s partitions, and the surviving replica
          timed them out. The money stayed correct; the customers were told the wrong thing. A
          deadline has to outlast an orphaned partition, so it is now 60 seconds &mdash; and the
          same freeze fails nothing.
        </p>
      </section>

      <section className="wrap band">
        <div className="measure">
          <span className="eyebrow">About the engine on this page</span>
          <h2>What is the same, and what is not</h2>
        </div>

        <div className="two">
          <div>
            <h3>The same</h3>
            <p style={{ color: "var(--ink-2)", fontSize: 15 }}>
              The saga and its compensations. The double-entry ledger, with the signed amounts and
              every constraint that enforces it. Money in flight parked in a sharded clearing
              account. The idempotency claim committing in the same transaction as the payment it
              guards. Messages written to an outbox in the transaction that caused them, delivered
              at-least-once, and claimed through an inbox inside the transaction that does the work.
              Deadlock made impossible by locking in one global order rather than merely detected.
            </p>
          </div>
          <div>
            <h3>Not the same</h3>
            <p style={{ color: "var(--ink-2)", fontSize: 15 }}>
              Three services became one process, and Kafka became a table, because a serverless
              platform has nowhere to put a long-running broker or a background relay thread. The
              page you are reading turns the crank that a relay thread turns in the deployed system.
              That changes <em>who</em> invokes the loop, not what the loop guarantees &mdash; which
              is why the fault switches can still reach every failure the design exists to handle.
            </p>
          </div>
        </div>

        <div className="caveat measure">
          <h3>One open issue in the deployed system, recorded honestly</h3>
          <p>
            After a broker restart, a message consumer occasionally stops fetching while its group
            still reports it healthy. It reproduced in 4 of 10 restarts on one broker implementation
            and 0 of 13 on another, with identical client code. The root cause is not established,
            and it would be wrong to describe it as known. Since the fixes above it strands no
            money; an alert catches it and a restart clears it.
          </p>
        </div>
      </section>

      <footer className="wrap">
        <div className="measure">
          <p style={{ marginBottom: 10 }}>
            Java 21 &middot; Spring Boot 4.1 &middot; PostgreSQL &middot; Flyway &middot; Kafka
            &middot; Redis &middot; Prometheus &amp; Grafana &middot; OpenTelemetry &amp; Jaeger
            &middot; Testcontainers &middot; k6 &middot; Kubernetes &amp; Helm. This page:
            Next.js and PostgreSQL.
          </p>
          <p style={{ marginBottom: 0 }}>
            <a href="https://github.com/goutham-hegde/distributed-payment-engine">
              goutham-hegde/distributed-payment-engine
            </a>
          </p>
        </div>
      </footer>
    </>
  );
}
