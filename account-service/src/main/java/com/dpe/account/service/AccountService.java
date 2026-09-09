package com.dpe.account.service;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.repository.AccountRepository;
import com.dpe.events.AccountOpened;
import com.dpe.events.Topics;
import com.dpe.messaging.outbox.OutboxWriter;
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
    private final OutboxWriter outbox;

    public AccountService(AccountRepository accounts, TransferService transfers,
                          OutboxWriter outbox) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.outbox = outbox;
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

        // M5: tell the world who owns this account, so the orchestrator can authorize transfers
        // out of it without reading this database.
        //
        // In the same transaction as the row it describes, through the outbox, for the same
        // reason every other message in this service is: an account that exists but whose
        // ownership was never published is an account nobody can spend from - the edge check
        // would deny every transfer out of it, and no retry would fix it because the event is
        // not coming. Save-then-publish as two operations is exactly the dual-write bug.
        //
        // Keyed by ACCOUNT id, not transfer id - a different aggregate from every other message
        // on this topic. There is deliberately no ordering guarantee between this event and the
        // saga messages for a transfer out of the account: they are different aggregates and land
        // on different partitions. The consequence is bounded and safe - a transfer attempted in
        // the same millisecond as the account opening may be refused as unknown - because a
        // missing projection row denies. See AccountOpened.
        outbox.append("Account", account.getId(), Topics.ACCOUNT_EVENTS, AccountOpened.TYPE,
                new AccountOpened(account.getId(), account.getOwnerId(),
                        account.getAccountType().name(), account.getCurrency(),
                        openingBalanceMinor));

        return account;
    }

    @Transactional(readOnly = true)
    public Account get(UUID accountId) {
        return accounts.findById(accountId).orElseThrow(() -> new AccountNotFoundException(accountId));
    }
}
