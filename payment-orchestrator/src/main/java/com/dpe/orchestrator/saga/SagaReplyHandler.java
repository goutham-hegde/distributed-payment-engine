package com.dpe.orchestrator.saga;

import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.ReserveRejected;
import com.dpe.messaging.inbox.InboxGate;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional boundary and idempotency gate for every saga reply.
 *
 * <p>One gate, one transaction, six reply types. The inbox row, the saga state change and the
 * next command all commit together - so there is no reachable state in which the saga advanced
 * but its next command was lost, or in which a reply was marked consumed without being acted on.
 *
 * <p>The gate lives here rather than in each {@link SagaOrchestrator} method for the same reason
 * it does in account-service: one call site cannot be forgotten, six can.
 */
@Component
public class SagaReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyHandler.class);

    private final InboxGate inbox;
    private final SagaOrchestrator orchestrator;

    public SagaReplyHandler(InboxGate inbox, SagaOrchestrator orchestrator) {
        this.inbox = inbox;
        this.orchestrator = orchestrator;
    }

    /**
     * @return {@code true} if this delivery advanced the saga, {@code false} if it was a duplicate
     */
    @Transactional
    public boolean handle(UUID messageId, String topic, String eventType, Object reply) {
        if (!inbox.claim(messageId, topic, eventType)) {
            return false;
        }

        switch (reply) {
            case FundsReserved r   -> orchestrator.onFundsReserved(r, messageId);
            case ReserveRejected r -> orchestrator.onReserveRejected(r, messageId);
            case GatewayApproved r -> orchestrator.onGatewayApproved(r, messageId);
            case GatewayDeclined r -> orchestrator.onGatewayDeclined(r, messageId);
            case FundsCommitted r  -> orchestrator.onFundsCommitted(r, messageId);
            case FundsReleased r   -> orchestrator.onFundsReleased(r, messageId);
            default -> throw new IllegalStateException(
                    "no handler for reply type " + reply.getClass().getName());
        }

        log.debug("applied {} (message {})", eventType, messageId);
        return true;
    }

    /** Records a reply this version does not understand, without acting on it. */
    @Transactional
    public boolean skip(UUID messageId, String topic, String eventType) {
        return inbox.claim(messageId, topic, eventType);
    }
}
