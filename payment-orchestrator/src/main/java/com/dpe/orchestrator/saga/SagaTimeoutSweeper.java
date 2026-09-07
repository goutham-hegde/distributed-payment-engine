package com.dpe.orchestrator.saga;

import java.time.OffsetDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Finds sagas that have stopped moving and drives them to a terminal state.
 *
 * <p>This is the piece most tutorials leave out. Without it the system is not fault-tolerant - it
 * simply has not failed yet.
 *
 * <h2>Why it has to exist</h2>
 *
 * <p>If account-service dies after reserving, nobody will ever send the reply. Nothing else in
 * this design will notice: Kafka has no "this reply is late" signal, and the orchestrator is not
 * blocked on a thread that could time out - it is entirely event-driven, so the absence of an
 * event is the absence of everything. The saga sits in RESERVED forever and the sender's money
 * sits in a hold forever. That is invariant I4 violated, and it is a customer-visible incident.
 *
 * <p>The sweeper converts "we are waiting" into "we waited too long", which is the only way an
 * event-driven system can observe something that did not happen.
 *
 * <h2>How it works</h2>
 *
 * <p>{@link SagaInstanceRepository#claimExpired} returns in-flight sagas whose deadline has
 * passed, locked {@code FOR UPDATE SKIP LOCKED} - so a second orchestrator instance sweeping at
 * the same moment takes a disjoint batch instead of blocking on this one or double-compensating.
 * Each is handed to {@link SagaOrchestrator#onTimeout(SagaInstance)}, which decides what "stuck"
 * means for that particular state.
 *
 * <p>The returned count is not decoration: it lets a test drive the sweeper deterministically
 * instead of sleeping and hoping, and it becomes a counter at M6.
 *
 * <p><b>Why the transaction wraps the whole batch</b> rather than one saga at a time: the claim's
 * locks are the only mutual exclusion between sweeper instances, and releasing them early would
 * reopen the window for another instance to claim the same saga. Same reasoning as
 * {@code OutboxRelay}, and the same cost - a long-running transaction - bounded the same way, by
 * a small batch.
 */
@Component
public class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    private final SagaInstanceRepository sagas;
    private final SagaOrchestrator orchestrator;
    private final SagaProperties properties;

    public SagaTimeoutSweeper(SagaInstanceRepository sagas, SagaOrchestrator orchestrator,
                              SagaProperties properties) {
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    /**
     * Sweeps one batch of expired sagas.
     *
     * @return how many sagas this sweep acted on
     */
    @Transactional
    public int sweep() {
        List<SagaInstance> expired = sagas.claimExpired(OffsetDateTime.now(),
                properties.maxSweepAttempts(), properties.sweepBatchSize());
        if (expired.isEmpty()) {
            return 0;
        }

        int swept = 0;
        for (SagaInstance saga : expired) {
            try {
                orchestrator.onTimeout(saga);
                swept++;
            } catch (RuntimeException e) {
                // Per saga, so one saga that cannot be compensated does not stop every other
                // stranded saga from being rescued. Same principle as the outbox relay blocking
                // only the aggregate that failed: the blast radius should equal the thing that
                // actually broke.
                log.error("could not sweep saga {} (transfer {}); leaving it for the next pass",
                        saga.getId(), saga.getTransferId(), e);
            }
        }
        return swept;
    }
}
