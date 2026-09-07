package com.dpe.orchestrator.web;

import com.dpe.orchestrator.transfer.InvalidTransferException;
import java.time.Instant;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

    private static ResponseEntity<Map<String, Object>> body(HttpStatus status, String code,
                                                            String message) {
        return ResponseEntity.status(status).body(Map.of(
                "timestamp", Instant.now().toString(),
                "status", status.value(),
                "code", code,
                "message", message));
    }
}
