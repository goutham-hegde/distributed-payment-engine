package com.dpe.orchestrator.web;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    FilterRegistrationBean<RequestBulkhead> requestBulkhead(BulkheadProperties properties,
                                                            MeterRegistry registry,
                                                            DataSource dataSource) {
        requireReserve(properties.requestPermits(), poolSize(dataSource));

        FilterRegistrationBean<RequestBulkhead> registration =
                new FilterRegistrationBean<>(new RequestBulkhead(properties, registry));
        registration.addUrlPatterns("/api/*", "/admin/*");
        registration.setOrder(SecurityFilterProperties.DEFAULT_FILTER_ORDER + 1);
        registration.setName("requestBulkhead");
        return registration;
    }

    /**
     * A bulkhead as large as the pool reserves nothing and protects nothing - and would look, in
     * config and on the dashboard, exactly like one that did. So it is a boot failure, not a
     * warning: someone raising the permits to "fix" 503s under load must raise the pool with them,
     * on purpose.
     */
    static void requireReserve(int permits, int poolSize) {
        if (poolSize <= 0) {
            return;
        }
        if (permits >= poolSize) {
            throw new IllegalStateException("dpe.bulkhead.request-permits (" + permits + ") must be "
                    + "below the connection pool size (" + poolSize + "): the difference is what API "
                    + "traffic can never take from the outbox relay, the reply listener and the "
                    + "sweeper. With none left over, a surge of reads starves the saga pipeline.");
        }
        log.info("request bulkhead: {} permits against a pool of {} - {} connections reserved for "
                + "background work", permits, poolSize, poolSize - permits);
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
