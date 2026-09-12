package com.dpe.orchestrator.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.FilterChain;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * M8: the bulkhead's arithmetic, without a container - the guarantee is a property of a semaphore
 * and needs nothing else to be demonstrated.
 */
class RequestBulkheadTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("with every permit held, the next request is answered 503 and never reaches the chain")
    void fullBulkheadRefuses() throws Exception {
        RequestBulkhead bulkhead = bulkhead(1, Duration.ofMillis(50));
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> run(bulkhead, (req, res) -> {
            inside.countDown();
            await(release);
        }));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();

        AtomicInteger reached = new AtomicInteger();
        MockHttpServletResponse refused = run(bulkhead, (req, res) -> reached.incrementAndGet());

        assertThat(refused.getStatus()).isEqualTo(503);
        assertThat(refused.getHeader(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
        assertThat(refused.getContentAsString()).contains("\"code\":\"BUSY\"");
        assertThat(reached.get())
                .as("a refused request must not run - running is what would take the connection")
                .isZero();
        assertThat(registry.get("dpe.bulkhead.rejected").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("dpe.bulkhead.in_use").gauge().value()).isEqualTo(1.0);

        release.countDown();
        holder.get(5, TimeUnit.SECONDS);

        MockHttpServletResponse served = run(bulkhead, (req, res) -> reached.incrementAndGet());
        assertThat(served.getStatus()).isEqualTo(200);
        assertThat(reached.get()).as("the permit came back when the first request finished").isEqualTo(1);
    }

    @Test
    @DisplayName("a request that throws still returns its permit")
    void permitReturnedOnException() {
        RequestBulkhead bulkhead = bulkhead(2, Duration.ofMillis(50));

        assertThatThrownBy(() -> bulkhead.doFilter(new MockHttpServletRequest(),
                new MockHttpServletResponse(), (req, res) -> {
                    throw new IllegalStateException("the controller blew up");
                })).isInstanceOf(IllegalStateException.class);

        // A leaked permit is a bulkhead that shrinks by one per exception until it refuses
        // everything - an outage that builds up quietly over days.
        assertThat(bulkhead.availablePermits()).isEqualTo(2);
    }

    @Test
    @DisplayName("a bulkhead as large as the pool is refused at startup - it would reserve nothing")
    void bulkheadMustLeaveAReserve() {
        assertThatThrownBy(() -> BulkheadConfig.requireReserve(10, 10, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exceeds the connection pool size");
        assertThatThrownBy(() -> BulkheadConfig.requireReserve(12, 10, 1))
                .isInstanceOf(IllegalStateException.class);

        BulkheadConfig.requireReserve(6, 10, 1);
    }

    @Test
    @DisplayName("the reserve must cover every reply listener thread, not merely be non-empty")
    void reserveCoversEveryListenerThread() {
        // The M8 change this guards: three reply threads against the old pool of 10. 6 < 10 still
        // held, and the reserve would have been two connections short of what the pipeline needs.
        assertThatThrownBy(() -> BulkheadConfig.requireReserve(6, 10, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("3 reply listener threads");

        BulkheadConfig.requireReserve(6, 12, 3);
    }

    private RequestBulkhead bulkhead(int permits, Duration maxWait) {
        return new RequestBulkhead(new BulkheadProperties(permits, maxWait, Duration.ofSeconds(1)),
                registry);
    }

    private static MockHttpServletResponse run(RequestBulkhead bulkhead, FilterChain chain) {
        MockHttpServletResponse response = new MockHttpServletResponse();
        try {
            bulkhead.doFilter(new MockHttpServletRequest("GET", "/api/v1/transfers/x"), response, chain);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return response;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
