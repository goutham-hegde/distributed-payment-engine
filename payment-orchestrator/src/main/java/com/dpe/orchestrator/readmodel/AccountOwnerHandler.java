package com.dpe.orchestrator.readmodel;

import com.dpe.events.AccountOpened;
import com.dpe.events.EventEnvelope;
import com.dpe.messaging.inbox.InboxRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies {@link AccountOpened} to the ownership projection, behind the inbox gate.
 *
 * <p>Structurally identical to {@code AccountEventHandler} - claim the message id, skip if it has
 * been seen, otherwise do the write in the same transaction - and it is worth noticing that this
 * is now the third consumer of {@code dpe.account.events.v1} inside one service and still exactly
 * ONE consumer group. That is not a coincidence, it is the rule from M3: the inbox's primary key
 * is the message id alone, so two groups in one service would each receive every message and the
 * second would skip everything the first had already recorded. Adding a projection meant adding a
 * branch to {@code SagaReplyConsumer}, never a listener.
 *
 * <p>The reason this handler needs its own inbox claim rather than sharing the saga handler's:
 * each message must pass through exactly one gate. Routing AccountOpened into
 * {@code SagaReplyHandler} would put two different pieces of work behind one claim, and the
 * second would be skipped as a duplicate of the first.
 */
@Component
public class AccountOwnerHandler {

    private static final Logger log = LoggerFactory.getLogger(AccountOwnerHandler.class);

    private final InboxRepository inbox;
    private final AccountOwnerRepository owners;

    public AccountOwnerHandler(InboxRepository inbox, AccountOwnerRepository owners) {
        this.inbox = inbox;
        this.owners = owners;
    }

    /**
     * @return {@code true} if this delivery applied the event, {@code false} if it was a duplicate
     */
    @Transactional
    public boolean handle(UUID messageId, String topic, EventEnvelope<AccountOpened> envelope) {
        if (inbox.insertIfAbsent(messageId, topic, envelope.eventType()) == 0) {
            return false;
        }

        AccountOpened event = envelope.payload();
        int inserted = owners.recordOwner(event.accountId(), event.ownerId(),
                event.accountType(), event.currency());

        if (inserted == 0) {
            // A first delivery of a message about an account we already know the owner of. Not a
            // duplicate message - the inbox just proved that - so it is a SECOND AccountOpened
            // for one account id. The row already here wins (ownership is immutable), but this
            // should never happen and a silent no-op would hide a producer bug that has
            // authorization consequences.
            log.error("second AccountOpened for account {} (message {}, owner claimed: {}); "
                            + "keeping the owner already projected",
                    event.accountId(), messageId, event.ownerId());
        }
        return true;
    }
}
