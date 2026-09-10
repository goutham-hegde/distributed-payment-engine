package com.dpe.account.service;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import com.dpe.account.web.LedgerCursor;
import com.dpe.account.web.dto.LedgerEntryResponse;
import com.dpe.account.web.dto.LedgerPage;
import com.dpe.events.AccountOpened;
import com.dpe.events.Topics;
import com.dpe.account.domain.LedgerEntry;
import com.dpe.messaging.outbox.OutboxWriter;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Opening and reading accounts. Administrative scaffolding around the ledger - the interesting
 * concurrency lives in {@link TransferService}.
 */
@Service
public class AccountService {

    /** See {@code TransferService.MAX_PAGE_SIZE} in the orchestrator - a cap, not a default. */
    public static final int MAX_PAGE_SIZE = 200;

    /** Enough to fill a screen of history without a second request in the common case. */
    public static final int DEFAULT_PAGE_SIZE = 50;

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledger;
    private final TransferService transfers;
    private final OutboxWriter outbox;

    public AccountService(AccountRepository accounts, LedgerEntryRepository ledger,
                          TransferService transfers, OutboxWriter outbox) {
        this.accounts = accounts;
        this.ledger = ledger;
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

    /**
     * <b>M6 part 3.</b> Reads an account, if this caller is allowed to see it.
     *
     * <h2>The check is against the authoritative row, and that is the difference worth noticing</h2>
     *
     * <p>The orchestrator answers the same shape of question - "is this account this subject's?" -
     * from {@code account_owners}, a projection fed by events, and it has to reason carefully about
     * what staleness can do to the answer. Here there is no projection: {@code accounts.owner_id}
     * IS the fact, read in the transaction. So this check cannot be stale and needs no argument
     * about immutability.
     *
     * <p>That is not an argument for moving the orchestrator's check here. It is the same pairing
     * the write path already has - an edge check over a copy for a fast, local refusal, and the
     * authoritative check where the data lives - and it is the reason the two are not redundant.
     *
     * <h2>404, not 403</h2>
     *
     * <p>An account that exists but belongs to someone else is answered exactly like one that does
     * not exist. This is a read keyed on an id the caller supplied, so a 403 would confirm the
     * account is real to anyone walking ids - and account ids are not secret, they are handed to
     * whoever is meant to send you money. Folding both cases into
     * {@link AccountNotFoundException} means there is no code path that can tell them apart and
     * therefore none that can leak the difference later.
     *
     * @param operator whether the caller holds the OPERATOR role. Operators may read any account,
     *                 and only read - nothing on this path can move money, which is the whole
     *                 content of the role. The write path grants no such bypass.
     */
    @Transactional(readOnly = true)
    public Account getVisibleTo(UUID accountId, String subject, boolean operator) {
        Account account = accounts.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));

        if (!operator && !account.getOwnerId().equals(subject)) {
            throw new AccountNotFoundException(accountId);
        }
        return account;
    }

    /**
     * <b>M6 part 3.</b> One page of an account's ledger history, newest first, with the
     * authoritative balance alongside it.
     *
     * <p>Authorized by the same {@link #getVisibleTo} call that fetches the account, so there is
     * exactly one place the rule is written and no way to reach the entries without passing it.
     * Reading the entries first and checking afterwards would work identically and would be one
     * refactor away from a leak.
     *
     * <p>Both queries run in one read-only transaction, which is what makes the balance and the
     * entries consistent with each other: without it, a posting committing between the two reads
     * would produce a page whose newest entry is not reflected in the balance shown above it - and
     * a ledger view that does not add up is the one thing this screen exists to disprove.
     *
     * @param cursor where the previous page stopped, or null for the newest page
     * @param size   requested page size, clamped to {@link #MAX_PAGE_SIZE}
     */
    @Transactional(readOnly = true)
    public LedgerPage ledgerFor(UUID accountId, String subject, boolean operator,
                                LedgerCursor cursor, Integer size) {
        Account account = getVisibleTo(accountId, subject, operator);

        int pageSize = Math.clamp(size == null ? DEFAULT_PAGE_SIZE : size, 1, MAX_PAGE_SIZE);
        int fetch = pageSize + 1;

        List<LedgerEntry> rows = cursor == null
                ? ledger.findFirstPageFor(accountId, fetch)
                : ledger.findPageAfter(accountId, cursor.entryId(), fetch);

        boolean hasMore = rows.size() > pageSize;
        List<LedgerEntry> page = hasMore ? rows.subList(0, pageSize) : rows;

        String nextCursor = hasMore
                ? new LedgerCursor(page.get(page.size() - 1).getId()).encode()
                : null;

        return new LedgerPage(
                account.getId(),
                account.getBalanceMinor(),
                account.getCurrency(),
                page.stream().map(LedgerEntryResponse::of).toList(),
                nextCursor);
    }
}
