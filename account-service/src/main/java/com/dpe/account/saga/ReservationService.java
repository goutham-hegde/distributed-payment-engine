package com.dpe.account.saga;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.domain.Hold;
import com.dpe.account.domain.LedgerEntry;
import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.HoldRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import com.dpe.events.CommitFunds;
import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import com.dpe.events.ReserveRejected;
import com.dpe.events.Topics;
import com.dpe.messaging.outbox.OutboxWriter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * account-service's half of the saga: reserve, commit, release.
 *
 * <h2>The contract every method here honours</h2>
 *
 * <p><b>One transaction, three writes.</b> The business rows, the inbox row and the outbox row
 * commit together. {@link AccountCommandHandler} opens the transaction and writes the inbox row
 * before calling in here; {@code @Transactional} below uses the default {@code REQUIRED}
 * propagation, so these methods <i>join</i> that transaction rather than starting a second one.
 * That is what makes them callable directly from a test as well - a test gets a transaction of its
 * own, the handler gets one shared with the inbox row, and the atomicity is the same either way.
 *
 * <p>{@code REQUIRES_NEW} here would be a serious bug: the ledger would commit independently of
 * the inbox row that is supposed to make it idempotent, and a crash between the two would either
 * lose the command or apply it twice.
 *
 * <p><b>Locks are taken in a globally deterministic order.</b> Every method touches two accounts,
 * and one of them - CLEARING - is touched by <i>every concurrent saga in the system</i>. Locking
 * "the sender, then clearing" is a per-transfer order, not a global one, and two sagas can still
 * form a cycle. The two ids are sorted and the lower one is locked first, exactly as
 * {@code TransferService} does, so deadlock is structurally impossible rather than merely
 * detected and retried.
 *
 * <p><b>The money rules live in constraints.</b> The overdraft check is
 * {@code accounts_customer_balance_non_negative}; one-hold-per-transfer is
 * {@code holds_one_per_account_per_transfer}; commit-or-release-but-never-both is
 * {@code ledger_entries_one_leg_per_account_per_transfer}. The checks in this file make the common
 * case fast and give a decent error message; the constraints are what make the uncommon case safe.
 *
 * <h2>The ledger movements</h2>
 *
 * <pre>
 *   reserve   DEBIT  sender   -X     CREDIT clearing  +X     hold ACTIVE
 *   commit    DEBIT  clearing -X     CREDIT recipient +X     hold COMMITTED
 *   release   DEBIT  clearing -X     CREDIT sender    +X     hold RELEASED
 * </pre>
 *
 * <p>Commit and release write the identical ledger leg on the clearing side. That is not an
 * accident - see {@code V3__holds_and_inbox.sql} for why it is the best structural property in
 * this milestone.
 *
 * <h2>Business failure vs. technical failure</h2>
 *
 * <p>The distinction this file turns on. A rejected reserve <b>commits</b>: no ledger entries, no
 * hold, but an outbox {@link ReserveRejected} so the saga is told. Throwing would roll back the
 * handler's transaction, take the inbox row with it, and build an infinite redelivery loop around
 * a condition that will never change.
 *
 * <p>A technical failure - the database is down, a row is deadlocked - is the opposite case and
 * <i>should</i> propagate, because a retry genuinely might succeed. Nothing in this file catches
 * those.
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledgerEntries;
    private final HoldRepository holds;
    private final OutboxWriter outbox;

    public ReservationService(AccountRepository accounts, LedgerEntryRepository ledgerEntries,
                              HoldRepository holds, OutboxWriter outbox) {
        this.accounts = accounts;
        this.ledgerEntries = ledgerEntries;
        this.holds = holds;
        this.outbox = outbox;
    }

    /**
     * Handles {@link ReserveFunds}: take the money from the sender and park it in CLEARING.
     *
     * <p>Validation happens before any row is locked, and the balance check happens only after the
     * lock is held - a check performed before the lock reads a value another transaction is
     * already changing.
     */
    @Transactional
    public void reserve(ReserveFunds command) {
        if (command.amountMinor() <= 0) {
            reject(command, ReserveRejected.INVALID_TRANSFER, "amount must be positive");
            return;
        }
        if (command.fromAccountId().equals(command.toAccountId())) {
            reject(command, ReserveRejected.INVALID_TRANSFER,
                    "cannot transfer to the same account");
            return;
        }

        // The recipient is validated but NOT locked: this step does not touch its balance, and
        // locking a row we do not write would widen the deadlock surface for nothing. Reading it
        // here rather than at commit time means a transfer to a nonexistent account fails before
        // any money moves, so there is no hold to compensate.
        Optional<Account> maybeRecipient = accounts.findById(command.toAccountId());
        if (maybeRecipient.isEmpty()) {
            reject(command, ReserveRejected.ACCOUNT_NOT_FOUND,
                    "recipient account " + command.toAccountId() + " does not exist");
            return;
        }

        // Sorted by id, never by the role the account plays in this transfer. CLEARING is in every
        // saga, so a role-based order would put it on both sides of a cycle.
        UUID clearingId = AccountType.CLEARING_ACCOUNT_ID;
        UUID first = lower(command.fromAccountId(), clearingId);
        UUID second = first.equals(command.fromAccountId()) ? clearingId : command.fromAccountId();

        Optional<Account> a = accounts.findByIdForUpdate(first);
        Optional<Account> b = accounts.findByIdForUpdate(second);
        if (a.isEmpty() || b.isEmpty()) {
            reject(command, ReserveRejected.ACCOUNT_NOT_FOUND,
                    "sender account " + command.fromAccountId() + " does not exist");
            return;
        }
        Account sender = first.equals(command.fromAccountId()) ? a.get() : b.get();
        Account clearing = first.equals(clearingId) ? a.get() : b.get();
        Account recipient = maybeRecipient.get();

        // ------------------------------------------------------------------------------
        // M5: THE AUTHORITATIVE OWNERSHIP CHECK.
        // ------------------------------------------------------------------------------
        //
        // A rejection COMMITS and replies, exactly like the checks above. Throwing would roll
        // back the inbox row and redeliver a command whose ownership will never change, forever.
        //
        // A null initiatedBy is a rejection too. Adding a field to a record is backward
        // compatible on the wire, so during a rolling upgrade an old orchestrator's command
        // arrives without one - and "I cannot tell who asked" is not a reason to move money.
        //
        // FOUR THINGS WORTH BEING ABLE TO DEFEND ABOUT THIS CHECK:
        //
        // 1. The orchestrator already refused this at the edge, with a 403, against its
        //    projection of who owns what. This is not that check repeated for comfort - it is
        //    the same question asked where it can be answered AUTHORITATIVELY: owner_id is a
        //    column in THIS database, read under the row lock that is about to move the money.
        //    The edge check is over a copy that can be stale or missing.
        //
        // 2. A Kafka command is not proof of an HTTP request. Anything that can produce to
        //    dpe.account.commands.v1 can ask this service to move money - a replayed dead
        //    letter, a hand-produced rpk message (M4 proved that path is real), a compromised
        //    producer. "It came through the API" is an assumption, and this check is what makes
        //    it unnecessary.
        //
        // 3. It goes AFTER the lock is taken, not before. Ownership is immutable so a pre-lock
        //    read would give the same answer - but the habit of reading a value before locking
        //    the row it lives on is how the balance check would be got wrong, and there is no
        //    reason to practise it here.
        //
        // 4. A transfer INTO an account needs no such check. Anyone may be paid; consent is only
        //    required to take money out. That asymmetry is worth stating rather than leaving as
        //    an omission somebody later "fixes".
        // ------------------------------------------------------------------------------
        if (command.initiatedBy() == null
                || !command.initiatedBy().equals(sender.getOwnerId())) {
            reject(command, ReserveRejected.NOT_ACCOUNT_OWNER,
                    "subject '" + command.initiatedBy() + "' does not own account "
                            + sender.getId());
            return;
        }

        // All three, including CLEARING - otherwise INR ends up parked in a USD account and
        // nothing notices until someone reconciles.
        if (!sender.getCurrency().equals(command.currency())
                || !recipient.getCurrency().equals(command.currency())
                || !clearing.getCurrency().equals(command.currency())) {
            reject(command, ReserveRejected.CURRENCY_MISMATCH,
                    "currency mismatch for " + command.currency());
            return;
        }

        long amount = command.amountMinor();
        if (sender.getAccountType() == AccountType.CUSTOMER
                && sender.getBalanceMinor() < amount) {
            reject(command, ReserveRejected.INSUFFICIENT_FUNDS,
                    "balance is " + sender.getBalanceMinor() + ", needed " + amount);
            return;
        }

        ledgerEntries.saveAll(List.of(
                LedgerEntry.debit(command.transferId(), sender.getId(), amount, command.currency()),
                LedgerEntry.credit(command.transferId(), clearing.getId(), amount, command.currency())
        ));
        sender.applyDelta(-amount);
        clearing.applyDelta(amount);

        UUID holdId = UUID.randomUUID();
        holds.save(new Hold(holdId, command.transferId(), sender.getId(), amount,
                command.currency()));

        outbox.append("Transfer", command.transferId(), Topics.ACCOUNT_EVENTS,
                FundsReserved.TYPE,
                new FundsReserved(command.transferId(), holdId, sender.getId(),
                        recipient.getId(), amount, command.currency(),
                        sender.getBalanceMinor()));
    }

    /**
     * Handles {@link CommitFunds}: settle the hold in favour of the recipient.
     */
    @Transactional
    public void commit(CommitFunds command) {
        Optional<Hold> maybeHold = holds.findByIdForUpdate(command.holdId());
        if (maybeHold.isEmpty()) {
            // The orchestrator is quoting an id from another database, or from a wiped one.
            // Logged and dropped rather than thrown: creating a hold here would invent money,
            // and retrying forever will not make the row appear.
            log.error("CommitFunds names hold {} which does not exist (transfer {})",
                    command.holdId(), command.transferId());
            return;
        }
        Hold hold = maybeHold.get();

        if (!hold.isActive()) {
            // The race that actually happens: the sweeper compensated a saga whose approval was
            // merely slow, and the commit arrived afterwards.
            //
            // Deliberately NOT thrown. Throwing would roll back the inbox row and redeliver a
            // command that can never succeed, forever. The money is safe either way - the UNIQUE
            // constraint on (transfer_id, clearing, DEBIT) already made the double settlement
            // impossible - so the only thing left to choose is whether this is loud or infinite.
            log.error("CommitFunds for hold {} ignored: already {} (transfer {})",
                    hold.getId(), hold.getStatus(), command.transferId());
            return;
        }

        UUID clearingId = AccountType.CLEARING_ACCOUNT_ID;
        Accounts locked = lockPair(clearingId, command.toAccountId());
        if (locked == null) {
            log.error("CommitFunds cannot settle transfer {}: recipient {} does not exist",
                    command.transferId(), command.toAccountId());
            return;
        }
        Account clearing = locked.of(clearingId);
        Account recipient = locked.of(command.toAccountId());

        long amount = hold.getAmountMinor();
        ledgerEntries.saveAll(List.of(
                LedgerEntry.debit(hold.getTransferId(), clearing.getId(), amount, hold.getCurrency()),
                LedgerEntry.credit(hold.getTransferId(), recipient.getId(), amount, hold.getCurrency())
        ));
        clearing.applyDelta(-amount);
        recipient.applyDelta(amount);

        hold.commit();

        outbox.append("Transfer", hold.getTransferId(), Topics.ACCOUNT_EVENTS,
                FundsCommitted.TYPE,
                new FundsCommitted(hold.getTransferId(), hold.getId(), recipient.getId(),
                        amount, hold.getCurrency()));
    }

    /**
     * Handles {@link ReleaseFunds}: THE COMPENSATION. Give the sender their money back.
     *
     * <p>The mirror of {@link #commit}, with the recipient replaced by the account the hold names.
     *
     * <p>This does not erase the reserve. Its ledger entries stay exactly where they are, and the
     * sender's statement shows the debit and this credit as two separate facts - which is correct,
     * because that is what happened to their money. An audit trail that hid the round trip would
     * be a lie.
     *
     * <p>It is also safe to call after a long delay: the sweeper may fire this thirty seconds
     * after the reserve, while a gateway reply is still in flight, so nothing here assumes the
     * hold is untouched.
     */
    @Transactional
    public void release(ReleaseFunds command) {
        Optional<Hold> maybeHold = holds.findByIdForUpdate(command.holdId());
        if (maybeHold.isEmpty()) {
            log.error("ReleaseFunds names hold {} which does not exist (transfer {})",
                    command.holdId(), command.transferId());
            return;
        }
        Hold hold = maybeHold.get();

        if (!hold.isActive()) {
            // Either a redelivered release, or a release that lost the race to a commit. Same
            // reasoning as in commit(): loud, not infinite.
            log.warn("ReleaseFunds for hold {} ignored: already {} (transfer {})",
                    hold.getId(), hold.getStatus(), command.transferId());
            return;
        }

        UUID clearingId = AccountType.CLEARING_ACCOUNT_ID;
        Accounts locked = lockPair(clearingId, hold.getAccountId());
        if (locked == null) {
            log.error("ReleaseFunds cannot compensate transfer {}: account {} does not exist",
                    command.transferId(), hold.getAccountId());
            return;
        }
        Account clearing = locked.of(clearingId);
        Account sender = locked.of(hold.getAccountId());

        long amount = hold.getAmountMinor();
        ledgerEntries.saveAll(List.of(
                LedgerEntry.debit(hold.getTransferId(), clearing.getId(), amount, hold.getCurrency()),
                LedgerEntry.credit(hold.getTransferId(), sender.getId(), amount, hold.getCurrency())
        ));
        clearing.applyDelta(-amount);
        sender.applyDelta(amount);

        hold.release();

        outbox.append("Transfer", hold.getTransferId(), Topics.ACCOUNT_EVENTS,
                FundsReleased.TYPE,
                new FundsReleased(hold.getTransferId(), hold.getId(), sender.getId(), amount,
                        hold.getCurrency(), command.reason()));
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Writes the rejection reply and nothing else. The caller returns immediately afterwards, so
     * the transaction commits with an inbox row, a message, and no ledger movement.
     */
    private void reject(ReserveFunds command, String reason, String detail) {
        log.info("rejecting reserve for transfer {}: {} ({})",
                command.transferId(), reason, detail);
        outbox.append("Transfer", command.transferId(), Topics.ACCOUNT_EVENTS,
                ReserveRejected.TYPE,
                new ReserveRejected(command.transferId(), command.fromAccountId(), reason, detail));
    }

    /**
     * Locks two accounts in sorted id order, or returns {@code null} if either is missing.
     *
     * <p>Two single-row calls, never a batch {@code IN (...)} fetch: the order in which Postgres
     * locks the rows matched by an {@code IN} list is not part of the contract even with an
     * {@code ORDER BY}, so a batch fetch silently reintroduces the deadlock.
     */
    private Accounts lockPair(UUID one, UUID two) {
        UUID first = lower(one, two);
        UUID second = first.equals(one) ? two : one;
        Optional<Account> a = accounts.findByIdForUpdate(first);
        Optional<Account> b = accounts.findByIdForUpdate(second);
        if (a.isEmpty() || b.isEmpty()) {
            return null;
        }
        return new Accounts(a.get(), b.get());
    }

    private static UUID lower(UUID one, UUID two) {
        return one.compareTo(two) < 0 ? one : two;
    }

    /** Two locked accounts, addressable by id so the caller never has to track which came first. */
    private record Accounts(Account first, Account second) {
        Account of(UUID id) {
            if (first.getId().equals(id)) {
                return first;
            }
            if (second.getId().equals(id)) {
                return second;
            }
            throw new IllegalArgumentException("account " + id + " was not locked");
        }
    }
}
