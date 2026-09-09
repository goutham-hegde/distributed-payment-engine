package com.dpe.messaging.metrics;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link MessagingMetrics#refresh()}, so the scrape never touches the database.
 *
 * <p>Same shape as {@code OutboxRelayScheduler}, and switched off by the same convention
 * ({@code dpe.metrics.scheduled=false}) so a test can keep background work off its tables. Unlike
 * the relay this one only reads, so leaving it on would not corrupt a test - but a timer firing
 * queries against a table a test is truncating produces log noise and the occasional confusing
 * stack trace, and "the metrics thread did it" is a bad thing to have to work out twice.
 *
 * <p>The default interval is 5s against a 10s scrape: fast enough that a scrape never sees a value
 * older than it expects, slow enough that the database load is trivial and fixed regardless of how
 * many things are scraping.
 */
@Component
@ConditionalOnProperty(prefix = "dpe.metrics", name = "scheduled", havingValue = "true",
        matchIfMissing = true)
public class MessagingMetricsScheduler {

    private final MessagingMetrics metrics;

    public MessagingMetricsScheduler(MessagingMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * No try/catch here, unlike the relay's scheduler: {@link MessagingMetrics#refresh()} already
     * swallows and logs, because a failed metrics read must never surface as an error in the
     * service being measured.
     */
    @Scheduled(fixedDelayString = "${dpe.metrics.refresh-interval:5s}")
    public void refresh() {
        metrics.refresh();
    }
}
