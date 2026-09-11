package com.dpe.orchestrator.web;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * M8: a bulkhead between API traffic and the saga pipeline, which share one connection pool.
 *
 * <h2>The failure it exists for</h2>
 *
 * <p>In the first load test's collapse, a thousand customers waiting on slow payments polled
 * {@code GET /transfers/{id}} 1,857 times a second - 97% of all traffic - and those requests took all
 * ten of this service's database connections, with 187 more threads queued behind them. The outbox
 * relay, the reply listener and the timeout sweeper draw from the SAME ten. So the orchestrator
 * could neither send the commands nor read the replies that would have finished the payments the
 * customers were polling about: the slower it got, the more they polled, and the more they polled,
 * the less it could do. Pool exhaustion was not the trigger - it arrived two minutes after the knee
 * - but it is what turned "slow" into "nothing completes".
 *
 * <h2>What it does</h2>
 *
 * <p>Bounds concurrent API requests to fewer than the pool has connections. Every request here uses
 * at most ONE connection at a time (open-in-view is off; the POST's ownership check and its
 * transaction run one after the other, never nested), so N permits can hold at most N connections,
 * and {@code pool size - N} are always left for background work, whatever the API is doing. A
 * request that cannot get a permit within {@code max-wait} is answered 503 and never touches the
 * pool.
 *
 * <p>That arithmetic is the whole guarantee, and it has one assumption. A future endpoint that
 * opens a second connection inside a request (a {@code REQUIRES_NEW}, a second DataSource call
 * outside the transaction) makes each permit worth two connections and quietly eats the reserve.
 *
 * <h2>Why a semaphore and not a second pool</h2>
 *
 * <p>The textbook bulkhead gives each workload its own pool. Here that means a routing DataSource
 * in front of two Hikari pools - and in Boot 4, defining any DataSource bean switches off the
 * auto-configuration that wires connection details, including the {@code @ServiceConnection}
 * every Testcontainers test relies on. This is the semaphore form of the same pattern (what
 * Resilience4j calls a SemaphoreBulkhead): same isolation guarantee, one pool, no rewiring.
 * What it does not give is per-workload pool metrics - the pool's own gauges still show one pool.
 *
 * <h2>Not the same thing as admission control</h2>
 *
 * <p>{@code AdmissionControl} bounds how much WORK is accepted; this bounds how many requests run
 * at once. The first protects the pipeline's queue from new payments, this protects the pipeline's
 * connections from all requests, reads included. Each alone leaves the other failure open.
 */
public class RequestBulkhead extends OncePerRequestFilter {

    private final Semaphore permits;
    private final int size;
    private final BulkheadProperties properties;
    private final Counter rejected;

    public RequestBulkhead(BulkheadProperties properties, MeterRegistry registry) {
        this.properties = properties;
        this.size = properties.requestPermits();
        // Fair: a request that has waited longest is served first, so under contention a poll that
        // arrived later cannot keep overtaking a payment that has been waiting.
        this.permits = new Semaphore(size, true);
        this.rejected = Counter.builder("dpe.bulkhead.rejected")
                .description("API requests refused because every request permit was in use")
                .register(registry);
        // A memory read on the scrape path, never a query (the M6 gauge rule). The semaphore is a
        // field of this filter bean, which is what keeps Micrometer's weak reference alive.
        Gauge.builder("dpe.bulkhead.in_use", permits, s -> size - s.availablePermits())
                .description("API requests currently holding a permit")
                .register(registry);
        Gauge.builder("dpe.bulkhead.permits", permits, s -> size)
                .description("API requests that may run at once")
                .register(registry);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(properties.maxWait().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            acquired = false;
        }
        if (!acquired) {
            rejected.increment();
            refuse(response);
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            permits.release();
        }
    }

    private void refuse(HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.SERVICE_UNAVAILABLE.value());
        response.setHeader(HttpHeaders.RETRY_AFTER,
                Long.toString(ApiExceptionHandler.retryAfterSeconds(properties.retryAfter())));
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        // Same shape as ApiExceptionHandler's bodies. Written by hand because no controller ran -
        // and nothing in it comes from the request, so there is nothing to escape.
        response.getWriter().write("{\"timestamp\":\"" + Instant.now() + "\",\"status\":503,"
                + "\"code\":\"BUSY\",\"message\":\"The request was not processed: the service is "
                + "busy. Retry after the Retry-After interval; a payment retried with the same "
                + "Idempotency-Key cannot be taken twice.\"}");
    }

    /** For tests: permits not currently held. */
    int availablePermits() {
        return permits.availablePermits();
    }
}
