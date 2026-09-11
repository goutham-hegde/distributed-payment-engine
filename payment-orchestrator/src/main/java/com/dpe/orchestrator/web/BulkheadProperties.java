package com.dpe.orchestrator.web;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * M8: how many request threads may be doing database work at once.
 *
 * @param requestPermits concurrent API requests. MUST be below the Hikari pool size - the
 *                       difference is what request traffic can never take from the relay, the
 *                       reply listener and the sweeper. Enforced at startup by
 *                       {@link BulkheadConfig}.
 * @param maxWait        how long a request waits for a permit before it is answered 503
 * @param retryAfter     what that 503 tells the client to wait
 */
@ConfigurationProperties(prefix = "dpe.bulkhead")
public record BulkheadProperties(Integer requestPermits, Duration maxWait, Duration retryAfter) {

    public BulkheadProperties {
        if (requestPermits == null || requestPermits <= 0) {
            requestPermits = 6;
        }
        if (maxWait == null || maxWait.isNegative()) {
            maxWait = Duration.ofSeconds(1);
        }
        if (retryAfter == null || retryAfter.isNegative() || retryAfter.isZero()) {
            retryAfter = Duration.ofSeconds(1);
        }
    }
}
