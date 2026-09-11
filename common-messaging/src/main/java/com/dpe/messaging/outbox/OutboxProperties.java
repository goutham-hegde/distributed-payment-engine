package com.dpe.messaging.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Relay tuning. Defaults applied here rather than only in {@code application.yml}, so a test that
 * boots a bare context still gets sane values.
 *
 * @param batchSize    rows claimed per poll. This is the throughput/transaction-duration knob:
 *                     the claim transaction stays open across every Kafka send in the batch, and
 *                     a long-running transaction in Postgres holds back the vacuum horizon and
 *                     bloats the table. Bigger batches amortise the poll better and hold the
 *                     transaction open longer. 100 is small enough that a slow broker cannot
 *                     wedge the database.
 * @param pollInterval how often to look for work. This is the system's latency floor - a message
 *                     waits, on average, half of this before anyone hears about it. Shortening it
 *                     buys latency and costs a query per interval per instance against an index
 *                     that is empty when idle, which is cheap; do not shorten it to zero, because
 *                     a hot loop against Postgres is a fine way to run out of connections.
 * @param sendTimeout  how long to wait for the broker to acknowledge one message. Bounded on
 *                     purpose: an unbounded {@code get()} on a wedged broker holds the claim
 *                     transaction open indefinitely, which turns a broker outage into a database
 *                     incident.
 * @param batchBudget  M7. Wall-clock time after which a drain stops starting new sends and commits
 *                     what it has; the unsent rows stay unpublished for the next poll. Needed
 *                     because sendTimeout bounds ONE send and a batch is up to batchSize of them -
 *                     100 x 5 s against a dead broker is eight minutes with the claim transaction
 *                     open. To Postgres that whole stretch is one "idle in transaction" (no SQL
 *                     runs between the claim and the commit), so without this bound no
 *                     {@code idle_in_transaction_session_timeout} could be set short enough to
 *                     reap an orphaned transaction without also killing a healthy relay. The
 *                     ceiling on the relay's idle stretch is batchBudget + sendTimeout +
 *                     the producer's max.block.ms, and the session timeout in application.yml
 *                     is set above that sum.
 */
@ConfigurationProperties(prefix = "dpe.outbox")
public record OutboxProperties(int batchSize, Duration pollInterval, Duration sendTimeout,
                               Duration batchBudget) {

    public OutboxProperties {
        if (batchSize <= 0) {
            batchSize = 100;
        }
        if (pollInterval == null) {
            pollInterval = Duration.ofMillis(500);
        }
        if (sendTimeout == null) {
            sendTimeout = Duration.ofSeconds(5);
        }
        if (batchBudget == null) {
            batchBudget = Duration.ofSeconds(10);
        }
    }
}
