package com.dpe.account.service;
import java.util.*;
import com.dpe.account.outbox.OutboxWriter;
import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import com.dpe.events.FundsTransferred;
import com.dpe.events.Topics;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.dpe.account.domain.Account;
import com.dpe.account.domain.LedgerEntry;
import com.dpe.account.domain.AccountType;
/**
 * Moves money between two accounts in this service, atomically.
 *
 * <h2>What {@link #transfer} does, and why in this order</h2>
 *
 * <ol>
 *   <li><b>Validates before touching any row.</b> A non-positive amount, a self-transfer, and a
 *       currency mismatch are all rejected up front. The self-transfer check matters more than it
 *       looks: it would degenerate the lock ordering below into locking the same row twice, and
 *       would post a debit and a credit that cancel - a no-op that pollutes the ledger.</li>
 *
 *   <li><b>Locks both accounts in a deterministic global order.</b> The two ids are sorted and
 *       {@link AccountRepository#findByIdForUpdate} is called on the lower one first.
 *       <p>Not "lock the debit side first" - that is a per-transfer order, not a global one, and
 *       two opposing transfers between the same pair would still deadlock. The order depends only
 *       on the identities, never on the role an account plays in this particular transfer.
 *       <p>Two separate calls, not a batch {@code IN (...)} fetch, which does not guarantee lock
 *       acquisition order and would silently reintroduce the cycle.</li>
 *
 *   <li><b>Checks the balance only once the lock is held.</b> A check performed before the lock
 *       reads a value another transaction may already be changing.</li>
 *
 *   <li><b>Writes both ledger entries, both balance updates, and the outbox message in this one
 *       transaction.</b> Two entries summing to zero, two balances moving in opposite directions,
 *       one message describing it, one commit. If any part fails all of it rolls back - that is
 *       what keeps invariants I1 and I2 true, and what makes the message impossible to publish
 *       about a transfer that did not happen (M2, {@code learning.md} 4.1).</li>
 *
 *   <li><b>Returns a {@link TransferResult}</b> built from the balances just written, not from a
 *       fresh read.</li>
 * </ol>
 *
 * <h2>Things the database will catch if this gets it wrong</h2>
 *
 * <ul>
 *   <li>{@code accounts_balance_non_negative} - a missed balance check becomes a constraint
 *       violation, never an overdraft (invariant I5).</li>
 *   <li>{@code ledger_entries_sign_matches_type} - a debit that increases a balance is rejected.</li>
 *   <li>{@code ledger_entries_one_leg_per_account_per_transfer} - the same transfer id cannot
 *       post the same leg twice. It surfaces as a
 *       {@link org.springframework.dao.DataIntegrityViolationException}; turning that into a
 *       clean idempotent response is M4's job.</li>
 * </ul>
 *
 * <h2>Why {@code @Transactional} is on the method and not somewhere convenient</h2>
 *
 * The row locks taken in step 2 are held until this transaction commits or rolls back - that is
 * the only thing making steps 3 and 4 atomic with respect to a concurrent transfer. Spring's
 * proxying means a call to this method from inside the same class would bypass the proxy and run
 * with no transaction at all, so the locks would be released immediately after each read. Call it
 * from outside, as the controller does.
 */
@Service
public class TransferService {

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledgerEntries;
    private final OutboxWriter outbox;

    public TransferService(AccountRepository accounts, LedgerEntryRepository ledgerEntries,
                           OutboxWriter outbox) {
        this.accounts = accounts;
        this.ledgerEntries = ledgerEntries;
        this.outbox = outbox;
    }

    @Transactional
    public TransferResult transfer(TransferCommand command) {
        if (command.amountMinor() <= 0) {
            throw new InvalidTransferException("Amount must be positive");
        }
        if (command.fromAccountId().equals(command.toAccountId())) {
            throw new InvalidTransferException("Cannot transfer to the same account");
        }
        UUID first = command.fromAccountId().compareTo(command.toAccountId()) < 0 ? command.fromAccountId() : command.toAccountId();
        UUID second = first.equals(command.fromAccountId()) ? command.toAccountId() : command.fromAccountId();

        Account a = accounts.findByIdForUpdate(first).orElseThrow(() -> new AccountNotFoundException(first));
        Account b = accounts.findByIdForUpdate(second).orElseThrow(() -> new AccountNotFoundException(second));
        Account source;
        Account destination;
        if (first.equals(command.fromAccountId())) {
            source = a;
            destination = b;
        } else {
            source = b;
            destination = a;
        }

        if (!source.getCurrency().equals(command.currency()) || !destination.getCurrency().equals(command.currency())) {
            throw new InvalidTransferException("Currency mismatch");
        }
        if (source.getAccountType()==AccountType.CUSTOMER && source.getBalanceMinor() < command.amountMinor()) {
            throw new InsufficientFundsException(source.getId(), source.getBalanceMinor(), command.amountMinor());
        }

        long amount = command.amountMinor();

        ledgerEntries.saveAll(List.of(
            LedgerEntry.debit(command.transferId(), source.getId(), amount, command.currency()),
            LedgerEntry.credit(command.transferId(), destination.getId(), amount, command.currency())
        ));

        source.applyDelta(-amount);
        destination.applyDelta(amount);

        // The message is written HERE, by this transaction, alongside the ledger entries above.
        // It is an INSERT, not a network call: if the transfer rolls back the message vanishes
        // with it, and if the message is durable the transfer already is. A kafkaTemplate.send()
        // on this line would be the dual-write bug - two systems, no transaction spanning them,
        // and a crash in between publishing a fact about money that never moved.
        outbox.append("Transfer", command.transferId(), Topics.ACCOUNT_EVENTS,
            FundsTransferred.TYPE,
            new FundsTransferred(command.transferId(), source.getId(), destination.getId(),
                amount, command.currency()));

        return new TransferResult(
            command.transferId(),
            source.getId(),
            source.getBalanceMinor(),
            destination.getId(),
            destination.getBalanceMinor()
        );
    }
}
