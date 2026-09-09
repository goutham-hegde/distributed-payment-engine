package com.dpe.messaging.deadletter;
import java.time.OffsetDateTime;
import com.dpe.messaging.outbox.OutboxProperties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.dpe.events.EventEnvelope;
/**
 * Sends a dead letter back to the topic it failed on.
 *
 * <p>A dead letter queue with no replay is a graveyard with good lighting. Recording the failure
 * is only half the pattern; the half that matters operationally is being able to fix the cause -
 * deploy the consumer that understands the new field, bring the database back - and then push the
 * message through as if nothing had happened.
 *
 * <h2>Why replay is safe here, and what makes it safe</h2>
 *
 * <p>Republishing a message that may have been partially processed sounds alarming, and would be,
 * were it not for two properties this system already has.
 *
 * <p><b>The inbox row rolls back with the failure.</b> A technical failure throws, which rolls
 * back the handler transaction, which includes the {@code inbox} INSERT. So a message that
 * dead-lettered left no trace of having been consumed, and the replay is its first successful
 * delivery.
 *
 * <p><b>And if it did commit, the inbox catches it.</b> Suppose the handler committed and then
 * the acknowledgement failed - the message reaches the dead letter topic having genuinely been
 * processed. The replay is then a duplicate delivery, which is the case the inbox exists for: it
 * is skipped, and no money moves twice. Replay is therefore safe in both directions, and neither
 * direction needs the operator to know which one they are in.
 *
 * <p>This is the payoff for the discipline in M2 and M3, and it is a good thing to be able to
 * say out loud: the reason an operator can press "replay" without thinking about it is that
 * every consumer downstream is idempotent.
 *
 * <h2>The four things a replay has to get right</h2>
 *
 * <ol>
 *   <li><b>It goes back to {@code originalTopic}</b>, never to the dead letter topic it was read
 *       from - which would be a loop with no end.</li>
 *   <li><b>It carries the original key and the original {@code messageId} and {@code eventType}
 *       headers.</b> The key decides the partition, so a replay under a different key can be
 *       reordered against messages it was previously ordered with. The message id is what the
 *       consumer dedupes on: mint a fresh one and a safe replay becomes a guaranteed double-spend.
 *       A letter missing either is refused rather than repaired - see {@link #isReplayable}.</li>
 *   <li><b>Publish first, mark second</b>, with the wait bounded by
 *       {@link OutboxProperties#sendTimeout}. Same argument as {@code OutboxRelay}: marking first
 *       loses the message if the send then fails, and this row is the last copy in existence.</li>
 *   <li><b>One failure does not roll back the successes.</b> {@link #replayPending()} claims a
 *       batch in one transaction, so an escaping exception would undo every {@code markReplayed}
 *       already earned and republish all of them on the next call. Same trap and same shape as
 *       {@code OutboxRelay#drainBatch} step 5, with the same exception for an interrupt: that one
 *       stops the loop, because it is the JVM shutting down rather than one bad message.</li>
 * </ol>
 *
 * <p>{@link DeadLetterRepository#claimPending} returns a locked batch of managed entities, so
 * {@link DeadLetter#markReplayed} is flushed by dirty checking at commit.
 *
 * <p>{@code DeadLetterReplayTest} is the specification.
 */
@Service
public class DeadLetterReplayService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayService.class);

    private final DeadLetterRepository deadLetters;
    private final KafkaTemplate<String, String> kafka;
    private final DeadLetterProperties properties;
    private final OutboxProperties outboxProperties;

    public DeadLetterReplayService(DeadLetterRepository deadLetters,
                                   KafkaTemplate<String, String> kafka,
                                   DeadLetterProperties properties,
                                   OutboxProperties outboxProperties) {
        this.deadLetters = deadLetters;
        this.kafka = kafka;
        this.properties = properties;
        this.outboxProperties = outboxProperties;
    }

    /**
     * Replays one dead letter by id.
     *
     * @return {@code true} if it was published, {@code false} if there is no such letter, it had
     *         already been replayed, or it is not replayable at all
     */
    @Transactional
    public boolean replay(UUID deadLetterId) {
        return deadLetters.findById(deadLetterId)
                .filter(letter -> letter.getReplayedAt() == null)
                .filter(DeadLetterReplayService::isReplayable)
                .map(this::publishAndMarkReplayed)
                .orElse(false);
    }

    /**
     * Replays every pending dead letter, up to {@link DeadLetterProperties#replayBatchSize}.
     *
     * @return how many the broker acknowledged
     */
    @Transactional
    public int replayPending() {
        List<DeadLetter> batch = deadLetters.claimPending(properties.replayBatchSize());
        int acknowledged = 0;
        for (DeadLetter deadLetter : batch) {
            if (!isReplayable(deadLetter)) {
                continue;
            }
            try {
                if (publishAndMarkReplayed(deadLetter)) {
                    acknowledged++;
                }
            } catch (RuntimeException e) {
                // Swallowed, not rethrown, and for the same reason OutboxRelay#drainBatch does it
                // at step 5: an exception escaping here rolls the transaction back, discarding
                // every markReplayed the batch had already earned and republishing all of them on
                // the next call.
                log.warn("failed to replay dead letter {}: {}", deadLetter.getId(), e.toString(), e);

                // Except for an interrupt, which is not a per-message failure - it is the JVM
                // shutting down. publishAndMarkReplayed has already restored the flag; stop
                // claiming work and leave the rest of the batch pending, which is the safe state.
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("interrupted mid-batch; {} letter(s) left pending for the next call",
                            batch.size() - acknowledged);
                    break;
                }
            }
        }
        return acknowledged;
    }

    /**
     * Whether this letter carries enough to be put back on its topic.
     *
     * <p>{@code message_id} and {@code event_type} are nullable columns, and not by accident: a
     * record with a missing or unparseable message id is exactly the kind that dead-letters in the
     * first place, since {@code AccountCommandConsumer} drops undedupable commands before any
     * handler sees them. So the table holds rows that must be readable and must never be replayed.
     *
     * <p>Replaying one anyway is the worst available option. Without a message id the consumer has
     * nothing to dedupe on, so the message would be dropped again on arrival - or, worse, acted on
     * with no protection against a second delivery. Minting a replacement id would be worse still:
     * it makes the message look new to every inbox in the system, which is a deliberate
     * double-spend dressed as a fix.
     *
     * <p>So it is refused, loudly and permanently. The row stays pending and stays visible; the fix
     * is to correct the producer and publish the intent afresh, which is a decision for a human and
     * not for a replay button.
     */
    private static boolean isReplayable(DeadLetter deadLetter) {
        if (deadLetter.getMessageId() == null || deadLetter.getEventType() == null) {
            log.warn("dead letter {} from {}-{}@{} cannot be replayed: {} is missing, so a "
                            + "consumer would have nothing to dedupe on",
                    deadLetter.getId(), deadLetter.getOriginalTopic(),
                    deadLetter.getOriginalPartition(), deadLetter.getOriginalOffset(),
                    deadLetter.getMessageId() == null ? "message_id" : "event_type");
            return false;
        }
        return true;
    }

    private boolean publishAndMarkReplayed(DeadLetter deadLetter) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                deadLetter.getOriginalTopic(),
                deadLetter.getMessageKey(),
                deadLetter.getPayload());
        record.headers()
                .add(EventEnvelope.MESSAGE_ID_HEADER,
                        deadLetter.getMessageId().toString().getBytes(StandardCharsets.UTF_8))
                .add(EventEnvelope.EVENT_TYPE_HEADER,
                        deadLetter.getEventType().getBytes(StandardCharsets.UTF_8));

        try {
            kafka.send(record).get(outboxProperties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted replaying dead letter " + deadLetter.getId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException(
                    "Failed to replay dead letter " + deadLetter.getId(), e);
        }

        deadLetter.markReplayed(OffsetDateTime.now());
        return true;
    }
}
