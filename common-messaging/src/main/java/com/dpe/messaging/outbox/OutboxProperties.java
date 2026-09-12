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
 * @param maxBatchesPerPoll M8. How many batches one scheduler tick may drain back to back, while
 *                     each comes back full and fully acknowledged. Pipelined, a batch of 100 takes
 *                     milliseconds, and then the relay would sleep pollInterval with a backlog
 *                     waiting - so it drains again. Bounded because the relay has no thread of its
 *                     own: in the orchestrator it shares Boot's single scheduler thread with the
 *                     saga sweeper and the metrics refresh, and an unbounded loop under sustained
 *                     load would stop sagas timing out and freeze every gauge at its last value.
 *                     (A dedicated thread is not free either: it is a connection, taken from the
 *                     reserve the request bulkhead keeps.) Any batch that is not both full and
 *                     fully acked ends the tick, so a failing broker costs one batch per tick, as
 *                     before.
 */
@ConfigurationProperties(prefix = "dpe.outbox")
public record OutboxProperties(int batchSize, Duration pollInterval, Duration sendTimeout,
                               Duration batchBudget, int maxBatchesPerPoll) {

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
        if (maxBatchesPerPoll <= 0) {
            maxBatchesPerPoll = 5;
        }
    }
}
