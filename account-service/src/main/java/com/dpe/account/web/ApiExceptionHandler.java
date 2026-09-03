package com.dpe.account.web;

import com.dpe.account.service.AccountNotFoundException;
import com.dpe.account.service.InsufficientFundsException;
import com.dpe.account.service.InvalidTransferException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps domain failures onto RFC 9457 problem responses.
 *
 * <p>The status codes are chosen to say something true about retryability, because from M3 a
 * saga reads them to decide between retrying and compensating:
 *
 * <ul>
 *   <li><b>404</b> - the account does not exist. Retrying will not help.</li>
 *   <li><b>422</b> - insufficient funds. A valid request with a business answer of "no". This is
 *       the trigger for compensation, not for a retry.</li>
 *   <li><b>400</b> - the command is malformed. Never retry.</li>
 *   <li><b>409</b> - a uniqueness constraint rejected a duplicate posting. The work was already
 *       done by an earlier attempt; M4 turns this into an idempotent success instead.</li>
 * </ul>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    public ProblemDetail onAccountNotFound(AccountNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Account not found");
        problem.setProperty("accountId", ex.getAccountId().toString());
        return problem;
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ProblemDetail onInsufficientFunds(InsufficientFundsException ex) {
        // UNPROCESSABLE_CONTENT, not UNPROCESSABLE_ENTITY: RFC 9110 renamed 422, and Spring
        // Framework 7 (Boot 4) deprecates the old constant. Same status code, new name.
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, ex.getMessage());
        problem.setTitle("Insufficient funds");
        problem.setProperty("accountId", ex.getAccountId().toString());
        problem.setProperty("balanceMinor", ex.getBalanceMinor());
        problem.setProperty("requestedMinor", ex.getRequestedMinor());
        return problem;
    }

    @ExceptionHandler(InvalidTransferException.class)
    public ProblemDetail onInvalidTransfer(InvalidTransferException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Invalid transfer");
        return problem;
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ProblemDetail onConstraintViolation(DataIntegrityViolationException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.CONFLICT, "the request violates a ledger constraint");
        problem.setTitle("Ledger constraint violated");
        return problem;
    }
}
