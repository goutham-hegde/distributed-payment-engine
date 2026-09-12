package com.dpe.orchestrator.admission;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * M8: how much new work the API may accept while earlier work is still in flight.
 *
 * <p>Every value here is a statement about MEASURED capacity, not a tuning knob to be nudged until
 * a graph looks better. See application.yml for the derivation of the default, and re-derive it
 * with a knee run whenever the pipeline's throughput changes. Not by arithmetic alone: after the
 * knee moved from ~15/s to ~35/s, "capacity x 10 s" said 300, and the run at 300 doubled the tail
 * for 1.7% more completions - capacity falls as the queue deepens, and Little's law gives the mean.
 *
 * @param enabled       master switch. Boxed, because a primitive binds to false when the key is
 *                      absent and admission control would ship silently off (the M4 trap)
 * @param maxInFlight   sagas that may be non-terminal at once, across every instance
 * @param countInterval how stale the in-flight count may get before the next admission recounts
 * @param retryAfter    what a refused client is told to wait before retrying the same key
 */
@ConfigurationProperties(prefix = "dpe.admission")
public record AdmissionProperties(Boolean enabled, Integer maxInFlight, Duration countInterval,
                                  Duration retryAfter) {

    public AdmissionProperties {
        if (enabled == null) {
            enabled = true;
        }
        if (maxInFlight == null || maxInFlight <= 0) {
            // Kept equal to application.yml's measured value; see there for the derivation.
            maxInFlight = 150;
        }
        if (countInterval == null || countInterval.isNegative()) {
            countInterval = Duration.ofSeconds(1);
        }
        if (retryAfter == null || retryAfter.isNegative() || retryAfter.isZero()) {
            retryAfter = Duration.ofSeconds(2);
        }
    }
}
