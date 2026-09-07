package com.dpe.gateway.sim;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Runtime-tunable fault injection - what makes this service a chaos target rather than a stub.
 *
 * <p>Mutable and thread-safe on purpose. The M7 chaos suite flips these with an HTTP call in the
 * middle of a running load test, so a restart to change a failure rate would defeat the point:
 * the interesting question is what the saga does when the gateway starts declining <i>while</i>
 * traffic is in flight, not what it does after a clean restart.
 *
 * <p>Atomics rather than {@code volatile} primitives because {@code latencyMs} and the rates are
 * read on every Kafka consumer thread and written from a request thread. Nothing here needs to
 * be consistent with anything else here, so independent atomics are sufficient and cheaper than
 * a lock.
 */
@Component
@ConfigurationProperties(prefix = "gateway.simulation")
public class GatewaySimulationProperties {

    /** Fraction of charges that come back DECLINED. This is the compensation trigger. */
    private final AtomicLong failureRate = new AtomicLong(Double.doubleToLongBits(0.0));

    /** Simulated PSP round-trip time. Turn it above the saga's step-timeout to force I4 pressure. */
    private final AtomicInteger latencyMs = new AtomicInteger(50);

    /**
     * Fraction of charges that hang instead of answering.
     *
     * <p>Distinct from {@code failureRate} because the two exercise completely different paths: a
     * decline drives compensation through the saga's normal reply handling, while a timeout
     * drives it through the sweeper with no reply at all. A system that handles the first and not
     * the second looks fault-tolerant right up until a participant actually dies.
     */
    private final AtomicLong timeoutRate = new AtomicLong(Double.doubleToLongBits(0.0));

    /**
     * Fraction of charges whose reply is published twice.
     *
     * <p>Real PSPs do this - a webhook retried because our acknowledgement was slow. It is the
     * direct test of the orchestrator's inbox: the second copy must change nothing.
     */
    private final AtomicLong duplicateCallbackRate = new AtomicLong(Double.doubleToLongBits(0.0));

    public double getFailureRate() {
        return Double.longBitsToDouble(failureRate.get());
    }

    public void setFailureRate(double value) {
        failureRate.set(Double.doubleToLongBits(clampRate(value, "failure-rate")));
    }

    public int getLatencyMs() {
        return latencyMs.get();
    }

    public void setLatencyMs(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("latency-ms must not be negative: " + value);
        }
        latencyMs.set(value);
    }

    public double getTimeoutRate() {
        return Double.longBitsToDouble(timeoutRate.get());
    }

    public void setTimeoutRate(double value) {
        timeoutRate.set(Double.doubleToLongBits(clampRate(value, "timeout-rate")));
    }

    public double getDuplicateCallbackRate() {
        return Double.longBitsToDouble(duplicateCallbackRate.get());
    }

    public void setDuplicateCallbackRate(double value) {
        duplicateCallbackRate.set(Double.doubleToLongBits(clampRate(value, "duplicate-callback-rate")));
    }

    private static double clampRate(double value, String name) {
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + " must be between 0 and 1: " + value);
        }
        return value;
    }
}
