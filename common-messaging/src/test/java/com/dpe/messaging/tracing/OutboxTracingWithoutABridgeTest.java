package com.dpe.messaging.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.function.Supplier;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

/**
 * The claim that tracing is not load-bearing, asserted rather than stated.
 *
 * <p>This module declares only the tracing <i>API</i>. A service that brings no bridge - and any
 * future headless component that depends on {@code common-messaging} without wanting a telemetry
 * stack - gets no {@code Tracer} and no {@code Propagator} bean, and every path through
 * {@link OutboxTracing} still has to work. Not "work" as in log a warning and continue: capture
 * returns nothing, the column stores NULL, the relay publishes exactly as it did at M5.
 *
 * <p>It is the same rule Redis is held to in this system, and it is worth a test for the same
 * reason: an instrumentation component that can throw has quietly joined the payment path, and
 * that is not a thing anybody discovers on a good day.
 *
 * <p>A plain JUnit test with no Spring context, like {@code RetryClassifierTest} - there is nothing
 * to autoconfigure here, and the whole point is the behaviour when nothing has been configured.
 */
class OutboxTracingWithoutABridgeTest {

    private final OutboxTracing tracing = new OutboxTracing(absent(), absent());

    @Test
    @DisplayName("with no tracer on the classpath, capture reports no context rather than failing")
    void captureIsSilentlyEmpty() {
        CapturedTrace captured = tracing.capture();

        assertThat(captured).isEqualTo(CapturedTrace.NONE);
        assertThat(captured.isPresent()).isFalse();
        assertThat(captured.traceParent()).isNull();
    }

    @Test
    @DisplayName("a publish span can still be opened, injected and closed - it simply records nothing")
    void publishPathIsFullyNoOp() {
        Headers headers = new RecordHeaders();

        assertThatCode(() -> {
            try (OutboxTracing.PublishSpan publish =
                         tracing.beginPublish(null, null, "dpe.account.events.v1", "FundsTransferred")) {
                publish.injectInto(headers);
                publish.error(new IllegalStateException("recorded nowhere, and that is fine"));
            }
        }).doesNotThrowAnyException();

        assertThat(headers.toArray())
                .as("no bridge means no context to write; the record goes out exactly as it did "
                        + "before this milestone existed")
                .isEmpty();
    }

    @Test
    @DisplayName("a stored traceparent is not enough on its own - without a propagator it is inert")
    void aStoredContextWithoutAPropagatorChangesNothing() {
        Headers headers = new RecordHeaders();

        try (OutboxTracing.PublishSpan publish = tracing.beginPublish(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", null,
                "dpe.account.events.v1", "FundsTransferred")) {
            publish.injectInto(headers);
        }

        assertThat(headers.toArray())
                .as("the column may hold a context from a service that HAS a bridge; a consumer "
                        + "of this library that has not must ignore it, not half-honour it")
                .isEmpty();
    }

    /**
     * An {@link ObjectProvider} for a bean that is not in the context. Only
     * {@code getIfAvailable()} is reachable from {@link OutboxTracing}; everything else stays
     * unimplemented so that a future call to one shows up as a failure here rather than as a
     * quietly wrong default.
     */
    private static <T> ObjectProvider<T> absent() {
        return new ObjectProvider<>() {
            @Override
            public T getIfAvailable() {
                return null;
            }

            @Override
            public T getObject() {
                throw new UnsupportedOperationException();
            }

            @Override
            public T getObject(Object... args) {
                throw new UnsupportedOperationException();
            }

            @Override
            public T getIfUnique() {
                return null;
            }

            @Override
            public T getIfAvailable(Supplier<T> defaultSupplier) {
                return defaultSupplier.get();
            }
        };
    }
}
