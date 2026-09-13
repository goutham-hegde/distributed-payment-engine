package com.dpe.orchestrator.tracing;

import org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.otlp.OtlpHttpSpanExporterBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * M9: the span exporter makes ONE attempt per batch. No retries.
 *
 * <p>Found because stopping this service took ~12 s whenever Jaeger was down, against 1.9 s with it
 * up - past Docker's 10 s grace, so {@code docker compose stop}, which stops Jaeger first because
 * nothing depends on it, SIGKILLed all three services. A thread dump during the slow stop put the
 * shutdown hook in {@code OpenTelemetrySdk.close()}, waiting on the final flush, and the export
 * thread asleep inside OpenTelemetry's {@code RetryInterceptor}: the default policy retries a failed
 * export up to five times with backoff from 1 s. Bounding the call timeout ({@code application.yml},
 * {@code management.opentelemetry.tracing.export.otlp.timeout}) only halved it, because the backoff
 * sleeps sit between attempts and a shutdown can meet two exports in a row - the one already in
 * flight and the final flush.
 *
 * <p>Retrying buys a span batch that survives a collector blip. That is worth nothing here next to
 * the rule tracing is held to: it must not be load-bearing, and deciding whether this process can
 * exit cleanly is load it was never meant to carry. A batch dropped while Jaeger is down is the
 * designed outcome; the payment it describes is unaffected either way.
 *
 * <p>Only applies when the OTLP exporter is being built. With export switched off (the test suite)
 * Boot never calls it.
 */
@Configuration(proxyBeanMethods = false)
public class TraceExportConfig {

    @Bean
    OtlpHttpSpanExporterBuilderCustomizer noExportRetries() {
        // null is "no retry policy": HttpExporterBuilder stores it as given and builds no
        // RetryInterceptor. RetryPolicy has no "none" constant to pass instead.
        return builder -> builder.setRetryPolicy(null);
    }
}
