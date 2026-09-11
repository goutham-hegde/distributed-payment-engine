package com.dpe.orchestrator.admission;

import java.time.Duration;

/**
 * M8: a new transfer was refused because the pipeline already holds as much work as it can finish
 * inside the saga's deadline.
 *
 * <p>Thrown from inside the creating transaction, so everything that transaction wrote rolls back
 * with it - including the idempotency claim. The key is therefore NOT consumed: the client's retry
 * with the same key is a first request, and is judged on the capacity available when it arrives.
 */
public class AdmissionRefusedException extends RuntimeException {

    private final Duration retryAfter;

    public AdmissionRefusedException(long inFlight, int limit, Duration retryAfter) {
        super("refused a new transfer: " + inFlight + " sagas in flight against a limit of " + limit);
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
