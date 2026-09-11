package com.dpe.orchestrator.web;

import com.dpe.orchestrator.admission.AdmissionRefusedException;
import com.dpe.orchestrator.authz.AccountAccessDeniedException;
import com.dpe.orchestrator.idempotency.IdempotencyConflictException;
import com.dpe.orchestrator.transfer.InvalidTransferException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns domain exceptions into HTTP status codes.
 *
 * <p>422 rather than 400: the request was syntactically valid JSON with valid types, and was
 * refused on its meaning. Note the constant is {@code UNPROCESSABLE_CONTENT} - RFC 9110 renamed
 * 422 from "Unprocessable Entity", and Spring Framework 7 deprecated the old name. Same status
 * code, new constant.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(InvalidTransferException.class)
    ResponseEntity<Map<String, Object>> onInvalidTransfer(InvalidTransferException e) {
        return body(HttpStatus.UNPROCESSABLE_CONTENT, "INVALID_TRANSFER", e.getMessage());
    }

    /**
     * The key was used before with a different request body.
     *
     * <p>409 rather than 422 because there is nothing wrong with the request in itself - it
     * conflicts with a record the server already holds. And a refusal rather than a replay,
     * because replaying would answer this request with another one receipt. See
     * {@link IdempotencyConflictException}.
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    ResponseEntity<Map<String, Object>> onIdempotencyConflict(IdempotencyConflictException e) {
        return body(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED", e.getMessage());
    }

    /**
     * M5: the caller does not own the account they tried to spend from.
     *
     * <p>403, and a body that says nothing about why. "No such account" and "not yours" are the
     * same answer here on purpose - see {@link AccountAccessDeniedException}. The detail goes to
     * the log, where an operator can see it and an attacker cannot.
     */
    @ExceptionHandler(AccountAccessDeniedException.class)
    ResponseEntity<Map<String, Object>> onAccountAccessDenied(AccountAccessDeniedException e) {
        log.warn("access denied: {}", e.getMessage());
        return body(HttpStatus.FORBIDDEN, "ACCOUNT_FORBIDDEN",
                "You may not use that account");
    }

    /**
     * A required header was not sent.
     *
     * <p>Handled explicitly so the answer says WHICH header and in the same error shape as
     * everything else. Left to the framework it is a 400 with a body that does not match this
     * API error model, which a client parsing errors generically cannot read.
     */
    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<Map<String, Object>> onMissingHeader(MissingRequestHeaderException e) {
        return body(HttpStatus.BAD_REQUEST, "MISSING_HEADER",
                "Required header is missing: " + e.getHeaderName());
    }

    /**
     * M6: a {@code cursor} query parameter this service did not issue.
     *
     * <p>400 and a message that does not say which of the four ways it was malformed. The detail
     * would only ever be read by somebody constructing cursors by hand, which is the thing the
     * opaque encoding exists to discourage.
     */
    @ExceptionHandler(InvalidCursorException.class)
    ResponseEntity<Map<String, Object>> onInvalidCursor(InvalidCursorException e) {
        return body(HttpStatus.BAD_REQUEST, "INVALID_CURSOR",
                "The cursor is not one this API issued");
    }

    /**
     * M8: the pipeline is full, and this payment was NOT accepted.
     *
     * <p>503 rather than 429. 429 says the CLIENT sent too much - a per-caller rate limit - and
     * this client may have sent one request all day. The refusal is about the server's state, so
     * it is the server's status code. {@code Retry-After} tells the client when to try again, and
     * the message tells it how: with the SAME Idempotency-Key, which a refusal did not consume. A
     * client that retries with a fresh key after a lost 202 is the one way to pay twice, so the
     * body says so rather than trusting every client to know.
     *
     * <p>Only ever reached for new work - see {@code AdmissionControl}. A retry of an accepted
     * payment is replayed at any load, so this answer is always true when it is given.
     */
    @ExceptionHandler(AdmissionRefusedException.class)
    ResponseEntity<Map<String, Object>> onAdmissionRefused(AdmissionRefusedException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, Long.toString(retryAfterSeconds(e.retryAfter())))
                .body(Map.of(
                        "timestamp", Instant.now().toString(),
                        "status", HttpStatus.SERVICE_UNAVAILABLE.value(),
                        "code", "AT_CAPACITY",
                        "message", "The payment was not accepted: the system is at capacity. Retry "
                                + "after the Retry-After interval with the same Idempotency-Key."));
    }

    /** Retry-After is whole seconds (RFC 9110); round up so "wait 1500 ms" is never "wait 1 s". */
    static long retryAfterSeconds(Duration retryAfter) {
        return Math.max(1, (retryAfter.toMillis() + 999) / 1000);
    }

    private static ResponseEntity<Map<String, Object>> body(HttpStatus status, String code,
                                                            String message) {
        return ResponseEntity.status(status).body(Map.of(
                "timestamp", Instant.now().toString(),
                "status", status.value(),
                "code", code,
                "message", message));
    }
}
