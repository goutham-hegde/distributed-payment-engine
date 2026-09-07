package com.dpe.orchestrator.web;

import com.dpe.orchestrator.transfer.TransferService;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import com.dpe.orchestrator.web.dto.TransferResponse;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
 */
@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    private final TransferService transfers;

    public TransferController(TransferService transfers) {
        this.transfers = transfers;
    }

    @PostMapping
    public ResponseEntity<TransferResponse> create(@Valid @RequestBody CreateTransferRequest request) {
        TransferResponse response = transfers.createTransfer(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{transferId}")
    public ResponseEntity<TransferResponse> get(@PathVariable UUID transferId) {
        return transfers.findTransfer(transferId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
