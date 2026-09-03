package com.dpe.account.service;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.repository.AccountRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Opening and reading accounts. Administrative scaffolding around the ledger - the interesting
 * concurrency lives in {@link TransferService}.
 */
@Service
public class AccountService {

    private final AccountRepository accounts;
    private final TransferService transfers;

    public AccountService(AccountRepository accounts, TransferService transfers) {
        this.accounts = accounts;
        this.transfers = transfers;
    }

    /**
     * Opens an account at a zero balance and, if an opening balance was requested, funds it by
     * transferring from the SYSTEM issuance account.
     *
     * <p>Funding goes through the ordinary transfer path rather than writing a balance directly.
     * That is the point: money entering the ledger is a normal double-entry posting with a real
     * counterpart, so invariants I1 and I2 hold from the first request and there is no privileged
     * code path that can create money.
     *
     * <p>Both steps run in one transaction ({@code REQUIRED} propagation means the nested call
     * joins this one), so a failure to fund cannot leave a stranded empty account.
     *
     * <p>Known contention: every funded opening locks the SYSTEM row, so concurrent openings
     * serialize on it. Acceptable for an administrative operation; a production ledger would
     * shard issuance across several system accounts.
     */
    @Transactional
    public Account open(String ownerId, String currency, long openingBalanceMinor) {
        Account account = accounts.save(
                new Account(UUID.randomUUID(), ownerId, AccountType.CUSTOMER, currency, 0L));

        if (openingBalanceMinor > 0) {
            transfers.transfer(new TransferCommand(
                    UUID.randomUUID(),
                    AccountType.SYSTEM_ACCOUNT_ID,
                    account.getId(),
                    openingBalanceMinor,
                    currency));
        }
        return account;
    }

    @Transactional(readOnly = true)
    public Account get(UUID accountId) {
        return accounts.findById(accountId).orElseThrow(() -> new AccountNotFoundException(accountId));
    }
}
