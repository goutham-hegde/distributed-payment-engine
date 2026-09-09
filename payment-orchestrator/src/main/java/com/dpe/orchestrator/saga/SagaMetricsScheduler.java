package com.dpe.orchestrator.saga;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Refreshes the {@code dpe.saga.inflight} gauges on a timer, so a Prometheus scrape is a memory
 * read rather than a query. Same convention as {@code MessagingMetricsScheduler} and switched off
 * by the same property.
 */
@Component
@ConditionalOnProperty(prefix = "dpe.metrics", name = "scheduled", havingValue = "true",
        matchIfMissing = true)
public class SagaMetricsScheduler {

    private final SagaMetrics metrics;

    public SagaMetricsScheduler(SagaMetrics metrics) {
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${dpe.metrics.refresh-interval:5s}")
    public void refresh() {
        metrics.refresh();
    }
}
