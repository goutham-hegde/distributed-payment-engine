package com.dpe.orchestrator.saga;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {

    List<SagaStep> findBySagaIdOrderByCreatedAtAsc(UUID sagaId);

    long countBySagaIdAndStepName(UUID sagaId, String stepName);
}
