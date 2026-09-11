package com.dpe.orchestrator.transfer;

import com.dpe.orchestrator.admission.AdmissionControl;
import com.dpe.orchestrator.authz.AccountOwnershipGuard;
import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.saga.SagaInstanceRepository;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import com.dpe.orchestrator.saga.SagaStepRepository;
import com.dpe.orchestrator.saga.SagaStepTrail;
import com.dpe.orchestrator.saga.SagaTimelineAssembler;
import com.dpe.orchestrator.web.TransferCursor;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import com.dpe.orchestrator.web.dto.TimelineResponse;
import com.dpe.orchestrator.web.dto.TransferPage;
import com.dpe.orchestrator.web.dto.TransferResponse;
import com.dpe.orchestrator.web.dto.TransferSummary;
import java.util.List;
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

    /**
     * The largest page {@code GET /api/v1/transfers} will serve, whatever the caller asks for.
     *
     * <p>A cap, not a default. Page size is an argument the client controls, and an uncapped one
     * is a denial-of-service parameter with a friendly name: {@code ?limit=1000000} is a request
     * for a million rows to be loaded, mapped and serialized, on a connection the caller can
     * abandon the moment it is sent. Clamped rather than rejected, so a client asking for too much
     * gets data and a next cursor rather than an error it has to learn about.
     */
    public static final int MAX_PAGE_SIZE = 100;

    /** What a caller gets when they do not say. Small enough that the first page is cheap. */
    public static final int DEFAULT_PAGE_SIZE = 20;

    private final TransferRepository transfers;
    private final SagaInstanceRepository sagas;
    private final SagaStepRepository steps;
    private final SagaOrchestrator orchestrator;
    private final AccountOwnershipGuard ownership;
    private final AdmissionControl admission;

    public TransferService(TransferRepository transfers, SagaInstanceRepository sagas,
                           SagaStepRepository steps, SagaOrchestrator orchestrator,
                           AccountOwnershipGuard ownership, AdmissionControl admission) {
        this.transfers = transfers;
        this.sagas = sagas;
        this.steps = steps;
        this.orchestrator = orchestrator;
        this.ownership = ownership;
        this.admission = admission;
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

        // M8. After validation, so a malformed request is told what is wrong with it rather than
        // to come back later; and here rather than in the controller, because only NEW work
        // reaches this line - the idempotency gate has already answered every retry of an accepted
        // payment. Throws inside this transaction, so the claim rolls back and the key stays
        // unused. See AdmissionControl.
        admission.admit();

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

    /**
     * <b>M6 part 3.</b> One page of the caller's own transfers, newest first.
     *
     * <h2>The scoping is in the query, not in a filter after it</h2>
     *
     * <p>{@link #findTransfer} reads a row and then applies {@link AccountOwnershipGuard#canView},
     * which is fine for one row. Doing the same thing here - fetch a page, drop the rows that are
     * not yours - is wrong in two separate ways. It leaks: the number of rows removed tells the
     * caller how many transfers other people made in that time window. And it is broken: after
     * dropping rows the page is short, so a client cannot tell "end of your list" from "a lot of
     * other people's rows sorted nearby", and the next cursor points into someone else's history.
     * Authorization for a collection belongs in the WHERE clause.
     *
     * <h2>The extra row</h2>
     *
     * <p>The query asks for {@code size + 1}. If that many come back there is at least one more
     * page, and the last row is discarded and its predecessor becomes the cursor. The alternative
     * is a second {@code COUNT} query per page, which costs more than the row it saves and is
     * racy anyway - the count is taken at a different instant from the page.
     *
     * @param cursor where the previous page stopped, or null for the first page
     * @param size   requested page size; clamped to {@link #MAX_PAGE_SIZE}
     */
    @Transactional(readOnly = true)
    public TransferPage listTransfers(String subject, TransferCursor cursor, Integer size) {
        int pageSize = Math.clamp(size == null ? DEFAULT_PAGE_SIZE : size, 1, MAX_PAGE_SIZE);
        int fetch = pageSize + 1;

        List<Transfer> rows = cursor == null
                ? transfers.findFirstPageFor(subject, fetch)
                : transfers.findPageAfter(subject, cursor.createdAt(), cursor.id(), fetch);

        boolean hasMore = rows.size() > pageSize;
        List<Transfer> page = hasMore ? rows.subList(0, pageSize) : rows;

        String nextCursor = null;
        if (hasMore) {
            Transfer last = page.get(page.size() - 1);
            nextCursor = new TransferCursor(last.getCreatedAt(), last.getId()).encode();
        }

        return new TransferPage(page.stream().map(TransferSummary::of).toList(), nextCursor);
    }

    /**
     * <b>M6 part 3.</b> The full history of one transfer: its saga, its stages, and the message
     * behind each one.
     *
     * <p>Scoped exactly like {@link #findTransfer}, and for a stronger reason. This response is a
     * far richer object than the polling read - it names topics, message ids and the trace id -
     * and every one of those is a handle on the internals of somebody's payment. It is also the
     * same {@link Optional#empty()} for "not yours" as for "does not exist", so the controller
     * answers 404 to both: a timeline endpoint that answered 403 for a real transfer would be a
     * particularly good oracle, because the caller would then know the id was worth attacking.
     *
     * <p>Three queries, all by primary key or by an indexed foreign id, none of them a scan. The
     * step trail is a single join rather than a lookup per step; see
     * {@link SagaStepRepository#findTrail}.
     */
    @Transactional(readOnly = true)
    public Optional<TimelineResponse> findTimeline(UUID transferId, String subject) {
        return transfers.findById(transferId)
                .filter(t -> ownership.canView(subject, t.getInitiatedBy()))
                .map(t -> {
                    SagaInstance saga = sagas.findByTransferId(transferId).orElse(null);
                    // A transfer with no saga row cannot happen - they are written in one
                    // transaction - but the timeline renders it rather than throwing, because a
                    // diagnostic endpoint that fails on impossible data is useless on the one
                    // day the data is impossible.
                    List<SagaStepTrail> trail = saga == null
                            ? List.of()
                            : steps.findTrail(saga.getId());
                    return SagaTimelineAssembler.assemble(t, saga, trail);
                });
    }
}
