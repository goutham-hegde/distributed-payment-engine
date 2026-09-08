package com.dpe.orchestrator.web;

import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.transfer.TransferService;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import com.dpe.orchestrator.web.dto.TransferResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
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

    public TransferController(TransferService transfers, IdempotencyGate gate) {
        this.transfers = transfers;
        this.gate = gate;
    }

    /**
     * Accepts a transfer for processing, at most once per {@code Idempotency-Key}.
     *
     * <p>Both headers are REQUIRED, and a missing one is a 400 rather than a default.
     *
     * <p>For {@code Idempotency-Key} that is the entire point: a client that omits it has no way
     * to retry safely, and quietly accepting the request would let a caller believe it has an
     * idempotency guarantee it never asked for. This is a money-movement endpoint; there is no
     * such thing as a request here that is fine to duplicate.
     *
     * <p>{@code X-Client-Id} is required so that keys live in per-client namespaces. Defaulting
     * it to something shared would put every caller in one namespace, where a common key like
     * {@code "1"} collides - and a collision does not merely fail, it replays one client
     * response body to another. It is a placeholder and it is trust-the-caller: anyone can send
     * any value. <b>M5 replaces it with the JWT subject</b>, at which point the namespace becomes
     * something the caller cannot choose. The column does not change, only where the value comes
     * from.
     *
     * <p>The body is returned as raw JSON text rather than a serialized {@link TransferResponse}
     * because a replay must be the response the first request gave, byte for byte - see
     * {@link IdempotentOutcome}.
     */
    @PostMapping
    public ResponseEntity<String> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @RequestHeader("X-Client-Id") @NotBlank @Size(max = 64) String clientId,
            @Valid @RequestBody CreateTransferRequest request) {

        IdempotentOutcome outcome = gate.execute(clientId, idempotencyKey, request);

        return ResponseEntity.status(outcome.status())
                .contentType(MediaType.APPLICATION_JSON)
                .header(REPLAYED_HEADER, Boolean.toString(outcome.replayed()))
                .body(outcome.bodyJson());
    }

    /**
     * The polling endpoint. Deliberately NOT behind the gate: a GET is idempotent by definition,
     * and it must report the transfer as it is NOW - which is the exact opposite of the replay
     * rule on the POST.
     */
    @GetMapping("/{transferId}")
    public ResponseEntity<TransferResponse> get(@PathVariable UUID transferId) {
        return transfers.findTransfer(transferId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
