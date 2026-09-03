package com.dpe.account.web;

import com.dpe.account.domain.Account;
import com.dpe.account.service.AccountService;
import com.dpe.account.web.dto.AccountResponse;
import com.dpe.account.web.dto.CreateAccountRequest;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountService accounts;

    public AccountController(AccountService accounts) {
        this.accounts = accounts;
    }

    @PostMapping
    public ResponseEntity<AccountResponse> open(@Valid @RequestBody CreateAccountRequest request) {
        Account account = accounts.open(
                request.ownerId(), request.currency(), request.openingBalanceMinor());
        return ResponseEntity
                .created(URI.create("/accounts/" + account.getId()))
                .body(AccountResponse.from(account));
    }

    @GetMapping("/{accountId}")
    public AccountResponse get(@PathVariable UUID accountId) {
        return AccountResponse.from(accounts.get(accountId));
    }
}
