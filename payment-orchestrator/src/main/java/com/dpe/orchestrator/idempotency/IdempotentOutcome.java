package com.dpe.orchestrator.idempotency;

/**
 * What the gate decided, ready to be turned into an HTTP response.
 *
 * <p><b>{@code bodyJson} is the stored response verbatim, not a re-rendering of current state,</b>
 * and that distinction is the contract. If a replay re-read the transfer and serialized it fresh,
 * a retry sent thirty seconds later would answer {@code COMPLETED} where the original answered
 * {@code PENDING} - two different answers to what the client believes is one request, which is
 * exactly the ambiguity idempotency exists to remove. A client comparing the two would be right
 * to conclude it had made two payments.
 *
 * <p>So the body is carried as text all the way to the wire. Deserializing it into a
 * {@code TransferResponse} and letting Jackson write it out again would be typed and tidy and
 * would quietly break the guarantee the moment the DTO gains, loses or reorders a field.
 *
 * @param status   the HTTP status the original request answered
 * @param bodyJson that response, byte for byte
 * @param replayed false the first time an intent is executed, true for every retry of it. Exposed
 *                 as the {@code Idempotency-Replayed} header, which is what makes the mechanism
 *                 demonstrable from a terminal instead of only from the database.
 */
public record IdempotentOutcome(int status, String bodyJson, boolean replayed) {
}
