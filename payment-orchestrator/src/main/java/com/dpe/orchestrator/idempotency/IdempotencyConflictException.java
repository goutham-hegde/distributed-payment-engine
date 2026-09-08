package com.dpe.orchestrator.idempotency;

/**
 * The client reused an idempotency key for a different request.
 *
 * <p>This is always a client bug, and the only safe answer is to refuse. The alternative -
 * replaying the stored response - hands back the receipt for the FIRST transfer as though it were
 * the second, so a client that accidentally reused a key is told its 5000 rupee payment succeeded
 * when what actually exists is a 300 rupee one. Silent, and discovered at reconciliation.
 *
 * <p>Answered 409 rather than 422: the request is not unprocessable in itself, it conflicts with
 * something the server already has.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String message) {
        super(message);
    }
}
