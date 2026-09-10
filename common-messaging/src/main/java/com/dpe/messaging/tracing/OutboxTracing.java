package com.dpe.messaging.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * M6 part 2. Carries a trace context across the gap the transactional outbox opens.
 *
 * <h2>The gap</h2>
 *
 * <p>Automatic trace propagation rests on one assumption: the outbound call happens on the thread
 * that is currently inside the span. HTTP honours it. A plain {@code kafkaTemplate.send()} honours
 * it - Spring Kafka injects a {@code traceparent} header at send time and the consumer extracts
 * it, for free.
 *
 * <p>The outbox breaks that assumption deliberately. Nothing is sent on the request thread; a ROW
 * is written inside the business transaction, and {@code OutboxRelay} turns it into a record later,
 * on a scheduled thread. By then the producing span is closed. Left to itself the relay would
 * either send with no context at all, or - worse - with its own, making every message in a drained
 * batch a child of one "drain" span and shattering a single payment into several unrelated traces
 * that no query can join back together.
 *
 * <h2>The fix, in two halves</h2>
 *
 * <p>{@link #capture()} runs on the producing thread, inside the business transaction, and freezes
 * the context into {@link CapturedTrace}. {@code OutboxWriter} stores it on the row, so it commits
 * or rolls back with the message it describes - the outbox's own guarantee, applied to the trace.
 *
 * <p>{@link #beginPublish} runs on the relay thread and does the reverse: extracts the stored
 * context, starts a PRODUCER span parented to it, and hands back a handle that injects <i>that</i>
 * span into the Kafka headers. The consumer's listener span then extracts those headers normally,
 * and the chain is unbroken: HTTP span, business span, publish span, consumer span.
 *
 * <h2>Why the publish span is a child rather than a straight header copy</h2>
 *
 * <p>Copying {@code trace_parent} verbatim onto the record is simpler, and is wrong in a way that
 * only shows up when it matters. It makes the consumer a direct child of the producing request,
 * which erases the relay hop entirely - and relay lag (row committed at T, published at T+400ms) is
 * one of the two numbers this milestone exists to make visible, the other being how long the row
 * then waited in the broker. A span for the publish puts that interval on the timeline where an
 * operator can see it, and correctly attributes a broker stall to the relay rather than to the
 * service that happened to write the row.
 *
 * <h2>What it does when tracing is switched off</h2>
 *
 * <p>Both dependencies are optional. With no bridge on the classpath Boot registers no
 * {@link Tracer} and no {@link Propagator}, and this class falls back to the no-op implementations
 * the tracing API ships. {@link #capture()} then returns {@link CapturedTrace#NONE}, the column
 * stores NULL, {@code beginPublish} produces a span that records nothing, and the outbox behaves
 * exactly as it did at M5. That is not defensive coding for its own sake: it is the rule Redis is
 * already held to in this system - <b>observability must not be load-bearing</b>. If deleting the
 * tracing backend could make a payment fail, the instrumentation would have become part of the
 * payment path, which is precisely the mistake.
 */
@Component
public class OutboxTracing {

    /**
     * The header names are W3C's, and they are also the column names in the migration. Written out
     * rather than taken from a propagator constant because there is not one to take: the
     * {@link Propagator} interface is format-agnostic on purpose and never names its own keys.
     * {@code management.tracing.propagation.type: w3c} is pinned in each service's config so that
     * the stored column and the emitted header cannot drift apart.
     */
    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";

    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this.tracer = tracer.getIfAvailable(() -> Tracer.NOOP);
        this.propagator = propagator.getIfAvailable(() -> Propagator.NOOP);
    }

    /**
     * Freezes the calling thread's current trace context.
     *
     * <p>Must be called on the producing thread, which in practice means from inside the business
     * transaction. Called anywhere else it returns {@link CapturedTrace#NONE} - silently, because
     * "no active span" is a normal state and not an error. A warning here would fire once per
     * message written by the timeout sweeper, forever.
     *
     * <p>Note that it goes through the propagator rather than reading {@code span.context()} and
     * formatting the header by hand. The propagator is the only thing that knows the wire format,
     * and the moment this method formats one itself, the stored value stops being whatever the
     * configured propagator would have sent - a bug that survives every unit test and fails only
     * against a real collector.
     */
    public CapturedTrace capture() {
        if (tracer.currentTraceContext() == null || tracer.currentTraceContext().context() == null) {
            return CapturedTrace.NONE;
        }
        Map<String, String> carrier = new HashMap<>(2);
        propagator.inject(tracer.currentTraceContext().context(), carrier, Map::put);
        String parent = carrier.get(TRACEPARENT);
        return parent == null
                ? CapturedTrace.NONE
                : new CapturedTrace(parent, carrier.get(TRACESTATE));
    }

    /**
     * Opens a PRODUCER span for one publish attempt, parented to the stored context.
     *
     * <p>The returned handle is {@link AutoCloseable} and MUST be closed, in a try-with-resources,
     * or the span leaks: the scope it opens is a thread local, and a relay thread that fails to
     * close one will parent the next message - and every message after it - under a span that
     * ended long ago.
     *
     * @param traceParent the row's {@code trace_parent}; {@code null} starts a fresh root span,
     *                    which is the right outcome for a message whose producer had no trace
     * @param traceState  the row's {@code trace_state}, may be {@code null}
     * @param topic       destination. Used as the span name so the timeline reads as the message
     *                    flow - and safe as a name because a topic is a bounded, declared
     *                    vocabulary. The message id is not, and belongs in an attribute
     * @param eventType   recorded as a tag, bounded for the same reason
     */
    public PublishSpan beginPublish(String traceParent, String traceState, String topic,
                                    String eventType) {
        Map<String, String> carrier = new HashMap<>(2);
        if (traceParent != null) {
            carrier.put(TRACEPARENT, traceParent);
        }
        if (traceState != null) {
            carrier.put(TRACESTATE, traceState);
        }

        Span span = propagator.extract(carrier, Map::get)
                .name(topic + " publish")
                .kind(Span.Kind.PRODUCER)
                .tag("messaging.system", "kafka")
                .tag("messaging.destination.name", topic)
                .tag("dpe.event.type", eventType)
                .start();

        return new PublishSpan(span, tracer.withSpan(span));
    }

    /**
     * One publish attempt's span, plus the thread-local scope that makes it current.
     *
     * <p>Two objects rather than one because they answer different questions: the scope is about
     * THIS thread, the span is about the operation. They are closed together here, in that order -
     * scope first, then span - because ending a span while it is still the thread's current one
     * leaves the thread pointing at something that has already finished.
     */
    public final class PublishSpan implements AutoCloseable {

        private final Span span;
        private final Tracer.SpanInScope scope;

        private PublishSpan(Span span, Tracer.SpanInScope scope) {
            this.span = span;
            this.scope = scope;
        }

        /**
         * Writes this span's context onto the outgoing record, which is what lets the consumer
         * continue the trace.
         *
         * <p>Injects the PUBLISH span's context, deliberately, and not the stored parent's. The
         * consumer is caused by this send attempt: if a first attempt fails and a later poll
         * republishes, the consumer that eventually receives the message should hang off the
         * attempt that actually delivered it, not off the one that did not.
         *
         * <p>Call it before {@code send()}, never after. The producer serializes the record on the
         * calling thread, so a header added afterwards is added to an object the broker has already
         * been told about - a mutation that compiles, runs, and silently does nothing.
         */
        public void injectInto(Headers headers) {
            propagator.inject(span.context(), headers,
                    (h, key, value) -> h.add(key, value.getBytes(StandardCharsets.UTF_8)));
        }

        /**
         * Marks the attempt failed. Kept separate from {@link #close()} rather than inferred,
         * because the relay swallows its exceptions on purpose - one undeliverable message must not
         * roll back a batch of delivered ones - and a span left to guess from a normal return would
         * then look perfectly successful for a message that never left the process.
         */
        public void error(Throwable t) {
            span.error(t);
        }

        @Override
        public void close() {
            scope.close();
            span.end();
        }
    }
}
