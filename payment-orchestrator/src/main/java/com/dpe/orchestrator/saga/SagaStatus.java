package com.dpe.orchestrator.saga;

import java.util.Set;

/**
 * The saga state machine's vocabulary.
 *
 * <p>Every non-terminal state means <b>a message is outstanding</b> - a command was sent and its
 * reply has not arrived. That is precisely what invariant I4 checks: a saga sitting in a
 * non-terminal state after the system has gone quiet is a saga whose reply never came, and
 * somewhere there is a hold with a customer's money in it.
 *
 * <p>The terminal set here must stay in step with two other places or I4 silently stops meaning
 * anything: the {@code saga_status_known} CHECK constraint and the partial index
 * {@code idx_saga_instances_in_flight} in {@code V2__saga.sql}, and the query in
 * {@code scripts/verify-invariants.sh}.
 */
public enum SagaStatus {

    /** ReserveFunds emitted; waiting for account-service. */
    STARTED,

    /** FundsReserved received and the hold id is known; ChargeGateway emitted. */
    RESERVED,

    /** GatewayApproved received; CommitFunds emitted. */
    CHARGED,

    /** FundsCommitted received. The recipient has the money. */
    COMPLETED,

    /** ReleaseFunds emitted after a decline or a timeout; waiting for account-service. */
    COMPENSATING,

    /** FundsReleased received. The sender has their money back. */
    COMPENSATED,

    /**
     * The reserve was rejected, so no money ever moved.
     *
     * <p>Terminal directly, with no trip through COMPENSATING - there is nothing to compensate.
     * Routing a rejection into the compensation path would emit a ReleaseFunds naming a hold that
     * does not exist, and the schema rejects the attempt: {@code saga_compensation_needs_a_hold}
     * forbids a COMPENSATING row with a null {@code hold_id}.
     */
    FAILED;

    private static final Set<SagaStatus> TERMINAL = Set.of(COMPLETED, COMPENSATED, FAILED);

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
