package com.dpe.messaging.deadletter;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The operator surface for the dead letter queue: how deep is it, what is in it, push it through.
 *
 * <p>Identical in all three services, which is why it lives in the library rather than being
 * copied three times. It is conditional on the application being a servlet web application so
 * that a future headless component can depend on {@code common-messaging} without acquiring an
 * HTTP endpoint it never asked for - the same reasoning as the {@code optional} scope on the web
 * starter in this module's POM. The condition is evaluated from annotation metadata, so the class
 * is never loaded where the web classes are absent.
 *
 * <p><b>Unauthenticated until M5.</b> Every endpoint here is an operator action - one of them
 * republishes payment commands - and none of them currently checks who is calling. That is
 * acceptable only because nothing is exposed outside the Compose network yet. M5 puts
 * {@code /admin/**} behind an authenticated role, and the M6.5 console calls it with a token.
 */
@RestController
@RequestMapping("/admin/dead-letters")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class DeadLetterController {

    private final DeadLetterRepository deadLetters;
    private final DeadLetterReplayService replays;

    public DeadLetterController(DeadLetterRepository deadLetters,
                                DeadLetterReplayService replays) {
        this.deadLetters = deadLetters;
        this.replays = replays;
    }

    /** Queue depth. One number, so a dashboard can poll it cheaply. */
    @GetMapping("/depth")
    public DepthResponse depth() {
        return new DepthResponse(deadLetters.countByReplayedAtIsNull());
    }

    /**
     * The pending letters, oldest first, capped at 100.
     *
     * <p>The payload is deliberately included: triage means looking at the message that failed,
     * and a listing that omits it sends the operator to psql anyway.
     */
    @GetMapping
    public List<DeadLetterView> pending() {
        return deadLetters.findTop100ByReplayedAtIsNullOrderByCreatedAtAsc()
                .stream()
                .map(DeadLetterView::of)
                .toList();
    }

    /** Every failure recorded for one aggregate - normally a transfer id. */
    @GetMapping("/by-key/{messageKey}")
    public List<DeadLetterView> byKey(@PathVariable String messageKey) {
        return deadLetters.findByMessageKeyOrderByCreatedAtAsc(messageKey)
                .stream()
                .map(DeadLetterView::of)
                .toList();
    }

    /**
     * Republishes one letter to the topic it failed on.
     *
     * <p>404 when there is no such letter, 409 when it has already been replayed. The second is
     * not pedantry: an operator refreshing a page must not be able to publish a payment command
     * twice by accident, and "already replayed" is a different fact from "does not exist".
     */
    @PostMapping("/{id}/replay")
    public ResponseEntity<Void> replay(@PathVariable UUID id) {
        if (deadLetters.findById(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return replays.replay(id)
                ? ResponseEntity.accepted().build()
                : ResponseEntity.status(409).build();
    }

    /** Republishes everything pending, up to the configured batch size. */
    @PostMapping("/replay")
    public ReplayResponse replayAll() {
        return new ReplayResponse(replays.replayPending());
    }

    public record DepthResponse(long pending) {
    }

    public record ReplayResponse(int replayed) {
    }

    public record DeadLetterView(UUID id,
                                 UUID messageId,
                                 String originalTopic,
                                 int originalPartition,
                                 long originalOffset,
                                 String messageKey,
                                 String eventType,
                                 String payload,
                                 String exceptionType,
                                 String exceptionMessage,
                                 int attempts,
                                 OffsetDateTime createdAt,
                                 OffsetDateTime replayedAt,
                                 int replayCount) {

        static DeadLetterView of(DeadLetter letter) {
            return new DeadLetterView(letter.getId(), letter.getMessageId(),
                    letter.getOriginalTopic(), letter.getOriginalPartition(),
                    letter.getOriginalOffset(), letter.getMessageKey(), letter.getEventType(),
                    letter.getPayload(), letter.getExceptionType(), letter.getExceptionMessage(),
                    letter.getAttempts(), letter.getCreatedAt(), letter.getReplayedAt(),
                    letter.getReplayCount());
        }
    }
}
