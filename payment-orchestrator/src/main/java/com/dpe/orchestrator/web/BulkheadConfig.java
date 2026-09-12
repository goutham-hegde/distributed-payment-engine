package com.dpe.orchestrator.web;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * M8: registers {@link RequestBulkhead} - and refuses to start with one that reserves nothing.
 */
@Configuration
@EnableConfigurationProperties(BulkheadProperties.class)
public class BulkheadConfig {

    private static final Logger log = LoggerFactory.getLogger(BulkheadConfig.class);

    /**
     * The filter is built here and NOT exposed as its own bean: Boot registers every Filter bean on
     * every URL by default, and a second, unscoped registration would put the token endpoint and
     * the JWKS document - neither of which touches the database - behind the same permits.
     *
     * <p>Scoped to the paths that touch the database. A controller added under a new prefix is NOT
     * covered until it is listed here. That fails open, which is acceptable for a bulkhead - it
     * protects capacity, not correctness - but it is the line to check when adding one.
     *
     * <p>Ordered just after Spring Security, so an unauthenticated request is answered 401 without
     * ever holding a permit: a flood of bad tokens must not be able to spend the budget the pipeline
     * is being protected with.
     */
    @Bean
    FilterRegistrationBean<RequestBulkhead> requestBulkhead(
            BulkheadProperties properties, MeterRegistry registry, DataSource dataSource,
            @Value("${dpe.saga.reply-concurrency}") int replyConcurrency) {
        requireReserve(properties.requestPermits(), poolSize(dataSource), replyConcurrency);

        FilterRegistrationBean<RequestBulkhead> registration =
                new FilterRegistrationBean<>(new RequestBulkhead(properties, registry));
        registration.addUrlPatterns("/api/*", "/admin/*");
        registration.setOrder(SecurityFilterProperties.DEFAULT_FILTER_ORDER + 1);
        registration.setName("requestBulkhead");
        return registration;
    }

    /**
     * Connections background work can hold at the same moment, apart from the reply listener's:
     * Boot's single scheduler thread (relay, sweeper, idempotency sweep and metrics refresh take
     * turns on it), the dead-letter listener, and the actuator's DB health check on the management
     * port, which the filter never sees.
     */
    static final int FIXED_BACKGROUND_CONNECTIONS = 3;

    /**
     * The reserve has to cover everything that can hold a connection at the same time as a full
     * set of permits - it is arithmetic, and the boot is where to do it.
     *
     * <p>Until M8 this checked only {@code permits < pool}, which is the right check for "reserves
     * something" and the wrong one for "reserves enough". Giving the reply listener three threads
     * needs two more connections, and {@code 6 < 10} would have gone on passing while the reserve
     * silently shrank below what the pipeline needs - the collapse this filter exists to prevent,
     * reintroduced by a change to a different class. A thread that touches the database is a
     * connection.
     *
     * <p>A boot failure, not a warning: someone raising the permits to "fix" 503s under load, or
     * adding listener threads, must raise the pool with them, on purpose.
     */
    static void requireReserve(int permits, int poolSize, int replyConcurrency) {
        if (poolSize <= 0) {
            return;
        }
        int background = FIXED_BACKGROUND_CONNECTIONS + replyConcurrency;
        if (permits + background > poolSize) {
            throw new IllegalStateException("dpe.bulkhead.request-permits (" + permits + ") plus "
                    + background + " background connections (" + replyConcurrency + " reply "
                    + "listener threads, the scheduler thread, the dead-letter listener and the "
                    + "health check) exceeds the connection pool size (" + poolSize + "). The "
                    + "difference is what API traffic can never take from the saga pipeline; "
                    + "without it, a surge of reads starves the pipeline. Raise "
                    + "spring.datasource.hikari.maximum-pool-size, or lower the permits.");
        }
        log.info("request bulkhead: {} permits against a pool of {} - {} connections reserved for "
                + "background work, which needs {}", permits, poolSize, poolSize - permits,
                background);
    }

    private static int poolSize(DataSource dataSource) {
        try {
            if (dataSource.isWrapperFor(HikariDataSource.class)) {
                return dataSource.unwrap(HikariDataSource.class).getMaximumPoolSize();
            }
        } catch (SQLException e) {
            log.warn("could not read the pool size to check the bulkhead against it", e);
        }
        log.warn("DataSource is not Hikari; the bulkhead's reserve cannot be checked");
        return 0;
    }
}
