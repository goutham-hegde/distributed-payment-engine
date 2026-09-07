package com.dpe.orchestrator.transfer;

import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.saga.SagaInstanceRepository;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import com.dpe.orchestrator.web.dto.TransferResponse;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The API-facing entry point. Accepts a transfer request and starts its saga.
 *
 * <p>Thin on purpose. Everything it does that matters happens inside
 * {@link SagaOrchestrator#start}, in this method's transaction: the transfer row, the saga row
 * and the ReserveFunds command all commit together.
 *
 * <p><b>What this method does NOT do is wait.</b> It returns as soon as the saga has started, with
 * the transfer in PENDING - it does not block until the money has moved. That is the fundamental
 * shape of a saga and it is a real API consequence, not an implementation detail: the caller gets
 * a transfer id and polls, or subscribes. A synchronous "the transfer succeeded" response would
 * require holding an HTTP connection open across three services and two brokers, which is the
 * distributed-lock-shaped thinking this design exists to avoid.
 */
@Service
public class TransferService {

    private final TransferRepository transfers;
    private final SagaInstanceRepository sagas;
    private final SagaOrchestrator orchestrator;

    public TransferService(TransferRepository transfers, SagaInstanceRepository sagas,
                           SagaOrchestrator orchestrator) {
        this.transfers = transfers;
        this.sagas = sagas;
        this.orchestrator = orchestrator;
    }

    /**
     * Validates, creates the transfer, and starts its saga.
     *
     * <p>Validation here is only what can be checked without leaving this database: a positive
     * amount and two distinct accounts. Whether the accounts exist, whether their currencies
     * agree and whether the sender can afford it are all account-service's to answer, and asking
     * it here would be a synchronous cross-service read that could be stale by the time the
     * reserve arrives. The saga's ReserveRejected path is the answer to all three.
     *
     * <p>Note there is no idempotency key yet - a client that retries this call starts a second
     * transfer. That is M4's job, and it is deliberately absent rather than half-done: an
     * idempotency gate that only covers the happy path is worse than none, because it is trusted.
     */
    @Transactional
    public TransferResponse createTransfer(CreateTransferRequest request) {
        if (request.fromAccountId().equals(request.toAccountId())) {
            throw new InvalidTransferException("Cannot transfer to the same account");
        }
        if (request.amountMinor() <= 0) {
            throw new InvalidTransferException("Amount must be positive");
        }

        Transfer transfer = new Transfer(UUID.randomUUID(), request.fromAccountId(),
                request.toAccountId(), request.amountMinor(),
                request.currency().toUpperCase());

        SagaInstance saga = orchestrator.start(transfer);
        return TransferResponse.of(transfer, saga);
    }

    @Transactional(readOnly = true)
    public Optional<TransferResponse> findTransfer(UUID transferId) {
        return transfers.findById(transferId)
                .map(t -> TransferResponse.of(t, sagas.findByTransferId(transferId).orElse(null)));
    }
}
