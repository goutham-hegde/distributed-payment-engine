package com.dpe.gateway.saga;

import com.dpe.events.ChargeGateway;
import com.dpe.gateway.service.ChargeService;
import com.dpe.messaging.inbox.InboxRepository;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional boundary for the commands this service consumes.
 *
 * <p>Same three-writes-one-commit rule as everywhere else: the inbox row, the charge row and the
 * outbox reply share a transaction. Here the stakes are highest, because the business write is
 * the one thing in the system that cannot be compensated by writing an opposite row.
 */
@Component
public class GatewayCommandHandler {

    private final InboxRepository inbox;
    private final ChargeService charges;

    public GatewayCommandHandler(InboxRepository inbox, ChargeService charges) {
        this.inbox = inbox;
        this.charges = charges;
    }

    /**
     * @return {@code true} if this delivery did the work, {@code false} if it was a duplicate
     */
    @Transactional
    public boolean handle(UUID messageId, String topic, ChargeGateway command) {
        if (inbox.insertIfAbsent(messageId, topic, ChargeGateway.TYPE) == 0) {
            return false;
        }
        charges.charge(command);
        return true;
    }

    /** Records a command this version does not understand, without acting on it. */
    @Transactional
    public boolean skip(UUID messageId, String topic, String eventType) {
        return inbox.insertIfAbsent(messageId, topic, eventType) != 0;
    }
}
