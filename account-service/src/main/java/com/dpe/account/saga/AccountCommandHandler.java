package com.dpe.account.saga;

import com.dpe.events.CommitFunds;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import com.dpe.messaging.inbox.InboxRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional boundary for every saga command this service consumes.
 *
 * <p>Same shape as the orchestrator's {@code AccountEventHandler}, and for the same reasons: the
 * inbox row and the business write share one commit, the transaction lives here rather than in
 * the consumer so that the offset is acknowledged only after it has committed, and the dedup
 * decision belongs to Postgres rather than to an {@code if}.
 *
 * <p>What is different is that this handler <b>dispatches</b>. Three command types arrive on one
 * topic and each maps to a different method on {@link ReservationService}. The dispatch is
 * deliberately dumb - decode, gate, call - because every interesting decision belongs one layer
 * down where the money is.
 *
 * <h2>Why the gate runs before the dispatch, not inside each method</h2>
 *
 * <p>Because then it is impossible to forget. A gate replicated into three business methods is
 * three chances to omit it, and the omission is invisible until a duplicate delivery double-spends
 * in production. Here there is exactly one call to {@link InboxRepository#insertIfAbsent} and
 * every command passes through it.
 */
@Component
public class AccountCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(AccountCommandHandler.class);

    private final InboxRepository inbox;
    private final ReservationService reservations;

    public AccountCommandHandler(InboxRepository inbox, ReservationService reservations) {
        this.inbox = inbox;
        this.reservations = reservations;
    }

    /**
     * Claims the message and, if it is new, executes it.
     *
     * @return {@code true} if this delivery did the work, {@code false} if it was a duplicate
     */
    @Transactional
    public boolean handle(UUID messageId, String topic, String eventType, Object command) {
        if (inbox.insertIfAbsent(messageId, topic, eventType) == 0) {
            return false;
        }

        // A switch over the payload type rather than over the eventType string: the consumer has
        // already used the string to choose which class to deserialize into, so re-deriving the
        // route from it here would let the two disagree.
        switch (command) {
            case ReserveFunds c -> reservations.reserve(c);
            case CommitFunds c  -> reservations.commit(c);
            case ReleaseFunds c -> reservations.release(c);
            default -> {
                // Unreachable from AccountCommandConsumer, which filters by type before calling.
                // Kept as a loud failure rather than a silent one so that a future command type
                // added to the consumer but not to this switch cannot be quietly discarded -
                // and note the inbox row above rolls back with the throw, so the message is
                // redelivered rather than lost.
                throw new IllegalStateException(
                        "no handler for command type " + command.getClass().getName());
            }
        }

        log.debug("applied {} (message {})", eventType, messageId);
        return true;
    }

    /**
     * Records a message as consumed without doing any work.
     *
     * <p>For a command this service understands but must not act on. It is a separate method
     * rather than a flag on {@link #handle} so that "we deliberately skipped this" is a distinct,
     * greppable event in the inbox rather than something inferred from an absence.
     */
    @Transactional
    public boolean skip(UUID messageId, String topic, String eventType) {
        return inbox.insertIfAbsent(messageId, topic, eventType) != 0;
    }
}
