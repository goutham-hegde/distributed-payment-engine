package com.dpe.orchestrator.saga;

/** What {@link SagaOrchestrator#reconcile} did with a request. */
public enum ReconcileOutcome {

    /** The saga is FAILED or COMPENSATED; a transfer-addressed release is in the outbox. */
    REQUESTED,

    /** No saga for that transfer id. */
    NO_SUCH_TRANSFER,

    /** The saga is still live. The sweeper owns it; a person racing the sweeper helps nobody. */
    STILL_IN_FLIGHT,

    /**
     * The transfer COMPLETED: the recipient has the money and the PSP has the charge, and both are
     * correct. There is no compensation to finish - reversing it would be moving money.
     */
    COMPLETED
}
