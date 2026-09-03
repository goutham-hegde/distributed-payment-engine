package com.dpe.account.web;

import com.dpe.account.service.TransferCommand;
import com.dpe.account.service.TransferService;
import com.dpe.account.web.dto.TransferRequest;
import com.dpe.account.web.dto.TransferResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The M1 transfer endpoint: a single-service, in-database money movement.
 *
 * <p>From M3 this becomes the {@code ReserveFunds} / {@code CommitFunds} command handler driven
 * by the orchestrator over Kafka. It stays an HTTP endpoint as well, because being able to drive
 * the ledger directly is what makes the chaos scenarios and the k6 run possible.
 */
@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final TransferService transfers;

    public TransferController(TransferService transfers) {
        this.transfers = transfers;
    }

    @PostMapping
    public TransferResponse transfer(@Valid @RequestBody TransferRequest request) {
        return TransferResponse.from(transfers.transfer(new TransferCommand(
                request.transferId(),
                request.fromAccountId(),
                request.toAccountId(),
                request.amountMinor(),
                request.currency())));
    }
}
