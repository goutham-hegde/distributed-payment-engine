package com.dpe.orchestrator.admin;

import com.dpe.orchestrator.saga.ReconcileOutcome;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /admin/transfers/{transferId}/reconcile} - finish the compensation of a transfer that
 * has already FAILED, when a participant still holds money for it.
 *
 * <p>Operator-only, by the {@code /admin/**} rule. It is the one {@code /admin} endpoint besides the
 * dead letter replay that causes a ledger movement, and it is bounded the same way the replay is:
 * it can only re-send a command the system itself already decided to send. The argument for why
 * that is not "an operator moving money" is on {@link SagaOrchestrator#reconcile}.
 *
 * <p>202, not 200: the request is a row in the outbox. What it did is learned from the transfer's
 * timeline, as a {@code Reconcile} stage, once account-service and the gateway have answered.
 *
 * <p>Deliberately no bulk form. Which transfers need reconciling is a question that joins
 * {@code accounts_db}, {@code gateway_db} and {@code payments_db}, and no service may hold
 * credentials to all three - so the list is built by an operator tool
 * ({@code scripts/reconcile.sh}) and each transfer is named here individually.
 */
@RestController
@RequestMapping("/admin/transfers")
public class ReconciliationController {

    private final SagaOrchestrator orchestrator;

    public ReconciliationController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping("/{transferId}/reconcile")
    public ResponseEntity<ReconcileResponse> reconcile(@PathVariable UUID transferId) {
        ReconcileOutcome outcome = orchestrator.reconcile(transferId);
        HttpStatus status = switch (outcome) {
            case REQUESTED -> HttpStatus.ACCEPTED;
            case NO_SUCH_TRANSFER -> HttpStatus.NOT_FOUND;
            case STILL_IN_FLIGHT, COMPLETED -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(new ReconcileResponse(transferId, outcome));
    }

    public record ReconcileResponse(UUID transferId, ReconcileOutcome outcome) {
    }
}
