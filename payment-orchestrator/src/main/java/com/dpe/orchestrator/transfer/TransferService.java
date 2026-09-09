package com.dpe.orchestrator.transfer;

import com.dpe.orchestrator.authz.AccountOwnershipGuard;
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
    private final AccountOwnershipGuard ownership;

    public TransferService(TransferRepository transfers, SagaInstanceRepository sagas,
                           SagaOrchestrator orchestrator, AccountOwnershipGuard ownership) {
        this.transfers = transfers;
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.ownership = ownership;
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
     *
     * <h2>M5: the subject</h2>
     *
     * <p>{@code initiatedBy} is recorded on the transfer and travels on the ReserveFunds command,
     * in the same commit as both. It is not re-checked here - {@code AccountOwnershipGuard} has
     * already refused unauthorized callers at the edge, and account-service checks again under
     * the lock. This layer's job is to make sure the identity is durable and travels with the
     * work, because a saga's steps run minutes after the request that started them and there is
     * no SecurityContext on a Kafka consumer thread.
     *
     * @param initiatedBy the authenticated subject, never null once M5's filter chain is in place
     */
    @Transactional
    public TransferResponse createTransfer(CreateTransferRequest request, String initiatedBy) {
        if (request.fromAccountId().equals(request.toAccountId())) {
            throw new InvalidTransferException("Cannot transfer to the same account");
        }
        if (request.amountMinor() <= 0) {
            throw new InvalidTransferException("Amount must be positive");
        }

        Transfer transfer = new Transfer(UUID.randomUUID(), request.fromAccountId(),
                request.toAccountId(), request.amountMinor(),
                request.currency().toUpperCase(), initiatedBy);

        SagaInstance saga = orchestrator.start(transfer);
        return TransferResponse.of(transfer, saga);
    }

    /**
     * The polling read, scoped to the caller.
     *
     * <p>A transfer that exists but belongs to somebody else comes back <b>empty</b>, exactly like
     * one that does not exist, and the controller therefore answers 404 for both. Two different
     * answers here would make this endpoint an oracle: an attacker walking transfer ids would
     * learn which are real from the difference between 403 and 404, without ever reading one.
     *
     * <p>That is the opposite call from the write path, where a refused account gets a 403 - and
     * the reason is which value is the secret. Here it is the id itself. There the caller supplied
     * the account id and is being told their claim to it is refused.
     */
    @Transactional(readOnly = true)
    public Optional<TransferResponse> findTransfer(UUID transferId, String subject) {
        return transfers.findById(transferId)
                .filter(t -> ownership.canView(subject, t.getInitiatedBy()))
                .map(t -> TransferResponse.of(t, sagas.findByTransferId(transferId).orElse(null)));
    }
}
