package com.dpe.orchestrator.saga;

import jakarta.persistence.LockModeType;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    /**
     * Loads the saga for a transfer and locks it until the surrounding transaction ends.
     *
     * <p>Every reply handler starts here, and the lock is not optional. Handling a reply is a
     * read-modify-write of the status, and two replies for the same saga can genuinely arrive
     * together - a duplicate PSP callback, or an approval racing the timeout sweeper. Without the
     * lock both would read RESERVED, both would decide to advance, and the saga would emit two
     * commands for one step.
     *
     * <p>Kafka's per-partition ordering does <b>not</b> save you here. It guarantees that one
     * consumer thread sees a partition's messages in order; it says nothing about the sweeper's
     * scheduled thread, which is not a consumer at all and can be inside this same saga at the
     * same instant.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SagaInstance s where s.transferId = :transferId")
    Optional<SagaInstance> findByTransferIdForUpdate(@Param("transferId") UUID transferId);

    Optional<SagaInstance> findByTransferId(UUID transferId);

    /**
     * The timeout sweeper's query: in-flight sagas whose deadline has passed.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED}, for exactly the reason the outbox relay uses it. Two
     * orchestrator instances sweeping at the same moment must not both compensate the same saga,
     * and blocking on each other would serialise the sweep behind whichever instance is slowest.
     * Skipping means each instance takes a disjoint batch and gets on with it.
     *
     * <p>Native, because JPQL cannot express {@code SKIP LOCKED} -
     * {@code @Lock(PESSIMISTIC_WRITE)} gives {@code FOR UPDATE} and then waits, which is the
     * behaviour to avoid.
     *
     * <p>The status list is repeated here rather than derived from {@link SagaStatus#isTerminal()}
     * because SQL cannot call Java. It must match the partial index
     * {@code idx_saga_instances_in_flight} exactly, or the planner ignores the index and the
     * sweep degrades to a sequential scan over every saga ever run.
     */
    @Query(value = """
            SELECT * FROM saga_instances
            WHERE status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')
              AND deadline_at < :now
              AND sweep_attempts < :maxAttempts
            ORDER BY deadline_at
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<SagaInstance> claimExpired(@Param("now") OffsetDateTime now,
                                    @Param("maxAttempts") int maxAttempts,
                                    @Param("batchSize") int batchSize);

    /**
     * M6: in-flight sagas grouped by state, for {@code dpe.saga.inflight}.
     *
     * <p>The live form of invariant I4, and more useful than {@link #countNonTerminal()} because
     * the SHAPE names the broken hop. STARTED piling up means account-service is not replying;
     * RESERVED piling up means the gateway is not answering; COMPENSATING piling up means the
     * release command is not landing. A single total says only "something is wrong".
     *
     * <p>Native, and the status list is written out rather than derived from
     * {@link SagaStatus#isTerminal()}, for exactly the reason given on {@link #claimExpired}: SQL
     * cannot call Java, and this predicate must match {@code idx_saga_instances_in_flight}
     * character for character or the planner ignores the partial index and this degrades to a
     * sequential scan over every saga ever run - on a timer, forever. That makes this the FIFTH
     * place the terminal set is written down; they change together or I4 stops meaning anything.
     *
     * @return rows of {@code [status, count]}
     */
    @Query(value = """
            SELECT status, COUNT(*)
            FROM saga_instances
            WHERE status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')
            GROUP BY status
            """, nativeQuery = true)
    List<Object[]> countInFlightByStatus();

    /** Invariant I4, as a query. Zero at rest; anything else means money may be stranded. */
    @Query("select count(s) from SagaInstance s where s.status not in "
            + "(com.dpe.orchestrator.saga.SagaStatus.COMPLETED, "
            + " com.dpe.orchestrator.saga.SagaStatus.COMPENSATED, "
            + " com.dpe.orchestrator.saga.SagaStatus.FAILED)")
    long countNonTerminal();
}
