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
 */
@ConfigurationProperties(prefix = "dpe.outbox")
public record OutboxProperties(int batchSize, Duration pollInterval, Duration sendTimeout) {

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
    }
}
