package com.dpe.account;

import com.dpe.messaging.MessagingConfig;
import com.dpe.security.JwtConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * <p>The three annotations below {@code @SpringBootApplication} exist because the outbox and
 * inbox live in {@code common-messaging}, outside this application's package. Boot scans only the
 * application class's own package and its subpackages, so without them the relay's repository is
 * never registered and the context fails at startup. Both package lists must name this service
 * AND the library, because {@code @EntityScan} and {@code @EnableJpaRepositories} replace the
 * default scan rather than adding to it - naming only the library would silently unregister this
 * service's own repositories. See {@link com.dpe.messaging.MessagingConfig} for the full
 * reasoning.
 */
@SpringBootApplication
@Import({MessagingConfig.class, JwtConfig.class})
@EntityScan({"com.dpe.account", "com.dpe.messaging"})
@EnableJpaRepositories({"com.dpe.account", "com.dpe.messaging"})
public class AccountServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountServiceApplication.class, args);
    }
}
