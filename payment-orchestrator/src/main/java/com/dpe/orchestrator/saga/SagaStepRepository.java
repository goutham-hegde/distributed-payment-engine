package com.dpe.orchestrator.saga;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {

    List<SagaStep> findBySagaIdOrderByCreatedAtAsc(UUID sagaId);

    long countBySagaIdAndStepName(UUID sagaId, String stepName);

    long countBySagaIdAndStepNameAndOutcome(UUID sagaId, String stepName, StepOutcome outcome);

    /**
     * Every step of one saga, in order, with its message joined in from whichever of the two
     * tables holds it.
     *
     * <p>Both joins are <b>unassociated entity joins</b> - {@code join Entity alias on ...} with
     * no mapped relationship between them. That is deliberate and it is not laziness about
     * mapping: {@code saga_steps.message_id} is a foreign key to two different tables depending on
     * the row, and to neither of them at the database level. There is no {@code REFERENCES}
     * constraint and there must not be, because half those message ids are the ids of rows in
     * <i>another service's</i> outbox. A {@code @ManyToOne} would be claiming a relationship the
     * schema cannot enforce and that is false half the time.
     *
     * <p>Bounded by construction: a saga has at most a handful of steps, and the query is by
     * {@code saga_id}, which is the leading column of {@code idx_saga_steps_saga}. This is the
     * per-request read, not a scan.
     *
     * <p>Ordered by {@code created_at} then {@code id}. The tiebreak matters more here than in the
     * paged list: the two rows of a step - the command going out and its reply coming back - can
     * land in the same microsecond when a reply is already waiting, and a timeline that shows a
     * step finishing before it started is a timeline nobody trusts again.
     */
    @Query("""
            select new com.dpe.orchestrator.saga.SagaStepTrail(
                s.stepName, s.outcome, s.toStatus, s.messageId, s.detail, s.createdAt,
                o.topic, o.eventType, o.createdAt, o.publishedAt, o.traceParent,
                i.topic, i.eventType, i.receivedAt)
            from SagaStep s
            left join OutboxMessage o on o.id = s.messageId
            left join InboxMessage i on i.messageId = s.messageId
            where s.sagaId = :sagaId
            order by s.createdAt asc, s.id asc
            """)
    List<SagaStepTrail> findTrail(@Param("sagaId") UUID sagaId);
}
