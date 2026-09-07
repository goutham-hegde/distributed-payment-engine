package com.dpe.orchestrator.saga;

/** How one saga step ended. Must stay in step with {@code saga_steps_outcome_known}. */
public enum StepOutcome {

    /** The command has been written to the outbox. No reply yet. */
    STARTED,

    /** The reply arrived and the saga moved forward. */
    SUCCEEDED,

    /** The participant refused - a rejected reserve, a declined charge. A business outcome. */
    FAILED,

    /** No reply arrived before the deadline; the sweeper acted. */
    TIMED_OUT,

    /** A reply that arrived but changed nothing - a duplicate, or one that lost a race. */
    SKIPPED
}
