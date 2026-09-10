package com.dpe.orchestrator.web;

import com.dpe.orchestrator.authz.AccountOwnershipGuard;
import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.transfer.TransferService;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import com.dpe.orchestrator.web.dto.TimelineResponse;
import com.dpe.orchestrator.web.dto.TransferPage;
import com.dpe.orchestrator.web.dto.TransferResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The public transfer API.
 *
 * <p>{@code POST} answers <b>202 Accepted</b>, not 201 Created, and the distinction is the whole
 * saga in one status code. 201 would claim the transfer exists and is done; 202 says it has been
 * accepted for processing and the outcome will be known later. The money has not moved when this
 * returns - the ReserveFunds command has not even left the outbox yet.
 *
 * <p>Callers poll {@code GET /api/v1/transfers/{id}} or, in a real system, subscribe. Holding the
 * connection open until the saga finished would reintroduce synchronous coupling across three
 * services, which is the thing being avoided.
 *
 * <h2>M4: the request no longer goes straight to {@link TransferService}</h2>
 *
 * <p>It goes through {@link IdempotencyGate}, and the controller does nothing except turn headers
 * into arguments and an outcome into a response. That is on purpose: the gate has to own the
 * transaction boundary, and a controller that decided anything about replaying would be deciding
 * it outside that boundary.
 *
 * <h2>M5: two checks now happen before the gate is reached</h2>
 *
 * <p>The filter chain has already established <i>who</i> the caller is, and that they hold
 * {@code ROLE_USER}. What it cannot have checked is <i>which account</i> they named - that value
 * is in the request body, which no filter has parsed. So the ownership check happens here, in the
 * request, before anything is written.
 *
 * <p><b>Order matters: authorize, then claim the idempotency key.</b> A refused request must not
 * consume the key, or a client that fixed a typo in the account id and retried with the same key
 * would be told its intent had already been handled. The gate's own rule is the same one - a
 * failed request rolls its claim back - and this keeps the ordering honest for a failure that
 * happens before the gate is even entered.
 */
@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    /**
     * Set on every response so a retry can be seen to be a retry from the client side. It is
     * what turns "the gate works" from a database query into something demonstrable with curl.
     */
    static final String REPLAYED_HEADER = "Idempotency-Replayed";

    private final TransferService transfers;
    private final IdempotencyGate gate;
    private final AccountOwnershipGuard ownership;

    public TransferController(TransferService transfers, IdempotencyGate gate,
                              AccountOwnershipGuard ownership) {
        this.transfers = transfers;
        this.gate = gate;
        this.ownership = ownership;
    }

    /**
     * Accepts a transfer for processing, at most once per {@code Idempotency-Key}, from an account
     * the caller owns.
     *
     * <h2>What happened to {@code X-Client-Id}</h2>
     *
     * <p>It is gone, and its job is now done by the token's {@code sub} claim. The column it fed -
     * {@code idempotency_records.client_id} - has not changed shape; only where the value comes
     * from has. That is the entire security argument in one substitution: the idempotency
     * namespace used to be <b>chosen by the caller</b>, so anybody could send someone else's
     * client id with someone else's key and be handed their response body, which here is another
     * customer's transfer receipt. Now it is <b>asserted by the issuer</b> and unforgeable without
     * the signing key.
     *
     * <p>{@code Idempotency-Key} stays a required header, for the reasons it always was: a client
     * that omits it has no way to retry safely, and this is a money-movement endpoint where no
     * request is safe to duplicate.
     *
     * <p>The body is returned as raw JSON text rather than a serialized {@link TransferResponse}
     * because a replay must be the response the first request gave, byte for byte - see
     * {@link IdempotentOutcome}.
     */
    @PostMapping
    public ResponseEntity<String> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @AuthenticationPrincipal Jwt caller,
            @Valid @RequestBody CreateTransferRequest request) {

        String subject = caller.getSubject();

        // Per-resource authorization: a valid token proves who is asking, not what they may
        // spend. Throws AccountAccessDeniedException -> 403, before any row is written.
        ownership.requireCanSpendFrom(subject, request.fromAccountId());

        IdempotentOutcome outcome = gate.execute(subject, idempotencyKey, request);

        return ResponseEntity.status(outcome.status())
                .contentType(MediaType.APPLICATION_JSON)
                .header(REPLAYED_HEADER, Boolean.toString(outcome.replayed()))
                .body(outcome.bodyJson());
    }

    /**
     * The polling endpoint. Deliberately NOT behind the gate: a GET is idempotent by definition,
     * and it must report the transfer as it is NOW - which is the exact opposite of the replay
     * rule on the POST.
     *
     * <p>Scoped to the caller. A transfer belonging to somebody else is answered 404, identically
     * to one that does not exist - see {@link TransferService#findTransfer}. Unguessable ids are
     * not an authorization model.
     */
    @GetMapping("/{transferId}")
    public ResponseEntity<TransferResponse> get(@PathVariable UUID transferId,
                                                @AuthenticationPrincipal Jwt caller) {
        return transfers.findTransfer(transferId, caller.getSubject())
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * <b>M6 part 3.</b> The caller's own transfers, newest first, keyset-paged.
     *
     * <p>There is no {@code accountId} or {@code status} filter parameter, and both are the kind
     * of thing that gets added without thinking. A status filter would need its own index -
     * {@code (initiated_by, status, created_at DESC, id DESC)} - or it becomes a filter over the
     * existing one that returns short pages and makes the cursor's meaning depend on the filter.
     * When a filter is genuinely wanted it arrives with the index that serves it, not before.
     *
     * <p>{@code cursor} is opaque; hand back exactly the {@code nextCursor} from the previous
     * page. A cursor that is not one this service issued is a 400 - see
     * {@link InvalidCursorException} for why it is not a silent restart from the top.
     */
    @GetMapping
    public TransferPage list(@AuthenticationPrincipal Jwt caller,
                             @RequestParam(required = false) String cursor,
                             @RequestParam(required = false) Integer size) {
        TransferCursor from = cursor == null ? null : TransferCursor.decode(cursor);
        return transfers.listTransfers(caller.getSubject(), from, size);
    }

    /**
     * <b>M6 part 3.</b> Where a transfer got to, stage by stage, with the message behind each one.
     *
     * <p>Under {@code /api/v1/transfers/**} and therefore covered by the existing {@code USER}
     * rule and the existing ownership scoping - no new security rule was written for it, which is
     * the point of having put the pattern there rather than naming endpoints one at a time.
     *
     * <p>404 for a transfer that is not the caller's, identically to one that does not exist. This
     * response carries more internal detail than any other read in the system, so the argument for
     * not confirming the id exists is stronger here than on the polling GET, not weaker.
     */
    @GetMapping("/{transferId}/timeline")
    public ResponseEntity<TimelineResponse> timeline(@PathVariable UUID transferId,
                                                     @AuthenticationPrincipal Jwt caller) {
        return transfers.findTimeline(transferId, caller.getSubject())
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
