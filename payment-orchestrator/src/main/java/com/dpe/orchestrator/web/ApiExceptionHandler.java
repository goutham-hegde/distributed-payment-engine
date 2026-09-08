package com.dpe.orchestrator.web;

import com.dpe.orchestrator.idempotency.IdempotencyConflictException;
import com.dpe.orchestrator.transfer.InvalidTransferException;
import java.time.Instant;
import java.util.Map;
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

    private static ResponseEntity<Map<String, Object>> body(HttpStatus status, String code,
                                                            String message) {
        return ResponseEntity.status(status).body(Map.of(
                "timestamp", Instant.now().toString(),
                "status", status.value(),
                "code", code,
                "message", message));
    }
}
