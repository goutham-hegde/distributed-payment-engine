package com.dpe.account.service;
import java.util.*;
import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.dpe.account.domain.Account;
import com.dpe.account.domain.LedgerEntry;
import com.dpe.account.domain.AccountType;
/**
 * Moves money between two accounts in this service, atomically.
 *
 * <p>This is the heart of M1 and it is deliberately left unimplemented. Everything around it -
 * schema, entities, locked-read query, DTOs, controller, tests - is scaffolding. The method
 * below is the part an interviewer will actually probe.
 *
 * <h2>The contract</h2>
 *
 * <ol>
 *   <li><b>Validate first, before touching any row.</b> Reject a non-positive amount, a
 *       self-transfer ({@code from.equals(to)}), and later a currency mismatch. A self-transfer
 *       matters more than it looks: it degenerates the lock ordering below into locking the same
 *       row twice, and it would post a debit and a credit that cancel - a no-op that pollutes
 *       the ledger.</li>
 *
 *   <li><b>Lock both accounts in a deterministic global order.</b> Sort the two ids (they are
 *       {@link java.util.UUID}, which is {@link Comparable}) and call
 *       {@link AccountRepository#findByIdForUpdate} on the lower one first, then the higher.
 *       <p>Not "lock the debit side first" - that is a per-transfer order, not a global one, and
 *       two opposing transfers between the same pair will still deadlock. The order must depend
 *       only on the identities, never on the role an account plays in this particular transfer.
 *       <p>Do this in two separate calls. A batch {@code IN (...)} fetch does not guarantee lock
 *       acquisition order and silently reintroduces the cycle.</li>
 *
 *   <li><b>Check the balance only after the lock is held.</b> A check performed before the lock
 *       is a read of a value another transaction may already be changing. Throw
 *       {@link InsufficientFundsException} if the debit side cannot cover the amount.</li>
 *
 *   <li><b>Write both ledger entries and both balance updates in this one transaction.</b>
 *       Use {@link com.dpe.account.domain.LedgerEntry#debit} and
 *       {@link com.dpe.account.domain.LedgerEntry#credit} (they apply the signs for you) and
 *       {@link com.dpe.account.domain.Account#applyDelta} with the same signed values. Two
 *       entries summing to zero, two balances moving in opposite directions, one commit. If any
 *       part fails, all of it must roll back - that is what keeps invariants I1 and I2 true.</li>
 *
 *   <li><b>Return a {@link TransferResult}</b> built from the balances you just wrote, not from
 *       a fresh read.</li>
 * </ol>
 *
 * <h2>Things the database will catch if you get it wrong</h2>
 *
 * <ul>
 *   <li>{@code accounts_balance_non_negative} - a missed balance check becomes a constraint
 *       violation, never an overdraft (invariant I5).</li>
 *   <li>{@code ledger_entries_sign_matches_type} - a debit that increases a balance is rejected.</li>
 *   <li>{@code ledger_entries_one_leg_per_account_per_transfer} - the same transfer id cannot
 *       post the same leg twice. In M1 this surfaces as a
 *       {@link org.springframework.dao.DataIntegrityViolationException}; turning that into a
 *       clean idempotent response is M4's job, not yours today.</li>
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

    public TransferService(AccountRepository accounts, LedgerEntryRepository ledgerEntries) {
        this.accounts = accounts;
        this.ledgerEntries = ledgerEntries;
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

        return new TransferResult(
            command.transferId(),
            source.getId(),
            source.getBalanceMinor(),
            destination.getId(),
            destination.getBalanceMinor()
        );
    }
}
