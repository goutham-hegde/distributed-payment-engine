package com.dpe.messaging.tracing;

/**
 * A W3C trace context frozen into two strings, so it can be stored in a database row.
 *
 * <p>A record rather than two loose parameters because the two values are meaningless apart:
 * a {@code tracestate} without its {@code traceparent} names a sampling decision for a trace
 * nobody can identify. Keeping them together makes it impossible to persist half of one.
 *
 * <p>{@link #NONE} is the honest representation of "there was no active span". That is a normal
 * condition, not a failure - a message written by the timeout sweeper, by a test, or by a service
 * booted with no tracing bridge on the classpath has no context to capture - and it must stay
 * cheap and silent, because the alternative is an outbox that logs a warning per message forever.
 *
 * @param traceParent the W3C {@code traceparent} header value, or {@code null}
 * @param traceState  the W3C {@code tracestate} header value, or {@code null}
 */
public record CapturedTrace(String traceParent, String traceState) {

    public static final CapturedTrace NONE = new CapturedTrace(null, null);

    /** {@code true} when there is a parent worth storing. */
    public boolean isPresent() {
        return traceParent != null && !traceParent.isBlank();
    }
}
