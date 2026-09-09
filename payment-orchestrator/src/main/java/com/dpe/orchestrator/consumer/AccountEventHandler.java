package com.dpe.orchestrator.consumer;

import com.dpe.events.EventEnvelope;
import com.dpe.events.FundsTransferred;
import com.dpe.messaging.inbox.InboxGate;
import com.dpe.orchestrator.readmodel.TransferProjectionRepository;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The idempotency gate: the reason this system can tolerate duplicate delivery.
 *
 * <p>Three lines of code, and everything around them - the topic, the consumer, the envelope, the
 * schema - is scaffolding around those three lines.
 *
 * <h2>What {@link #handle} does</h2>
 *
 * <ol>
 *   <li><b>Claims the message</b> with {@link InboxGate#claim}, which returns 1 for
 *       a first delivery and 0 for a repeat.</li>
 *
 *   <li><b>Returns {@code false} on a repeat</b>, immediately and without throwing. A duplicate is
 *       a normal, expected event in an at-least-once system, not an error condition; the caller
 *       acknowledges it and moves on.</li>
 *
 *   <li><b>Otherwise does the business write in this same transaction</b> and returns
 *       {@code true}.</li>
 * </ol>
 *
 * <h2>The two properties that make this correct</h2>
 *
 * <p><b>The dedup marker and the business write share one transaction.</b> This is the whole
 * design, and both failure directions are worth being able to state. Write the inbox row in its
 * own transaction first, and a crash before the business write leaves the message marked consumed
 * with the work never done - lost forever, because redelivery is now treated as a duplicate.
 * Write the business row first and mark afterwards, and a crash in between applies the work
 * twice. Only one commit covering both has neither hole. That is why this method is
 * {@code @Transactional} and why {@link AccountEventConsumer} is not.
 *
 * <p><b>The uniqueness decision belongs to Postgres.</b> {@code insertIfAbsent} is an
 * {@code INSERT ... ON CONFLICT DO NOTHING} against a primary key: the conflict is evaluated
 * atomically, under the index's lock, as part of the write. The tempting equivalent -
 * {@code if (inbox.existsById(id)) return false;} followed by a save - has a window between the
 * check and the insert in which a second consumer thread can pass the same check, and both apply
 * the message. Constraints cannot race; {@code if} statements can. Read
 * {@link InboxGate#claim}'s javadoc for why a caught
 * {@code DataIntegrityViolationException} is not a workable third option here either.
 *
 * <h2>What this does NOT protect against, on its own</h2>
 *
 * <p>Two deliveries of the same message arriving on two threads <i>concurrently</i> are handled,
 * but not by anything in this file. Both call {@code insertIfAbsent}; the second blocks on the
 * first's uncommitted row and then, once the first commits, inserts nothing and returns 0.
 * Correct - but only because the row lock made it so. The concurrency safety here is borrowed
 * entirely from the database.
 */
@Component
public class AccountEventHandler {

    private final InboxGate inbox;
    private final TransferProjectionRepository projections;

    public AccountEventHandler(InboxGate inbox, TransferProjectionRepository projections) {
        this.inbox = inbox;
        this.projections = projections;
    }

    /**
     * @return {@code true} if this delivery did the work, {@code false} if it was a duplicate. The
     *         boolean is what a test asserts on to prove the gate fired, and becomes a counter at
     *         M6.
     */
    @Transactional
    public boolean handle(UUID messageId, String topic, EventEnvelope<FundsTransferred> envelope) {
        if (!inbox.claim(messageId, topic, envelope.eventType())) {
            return false;
        }

        FundsTransferred event = envelope.payload();
        projections.recordCompleted(
                event.transferId(),
                event.fromAccountId(),
                event.toAccountId(),
                event.amountMinor(),
                event.currency());
        return true;
    }
}
