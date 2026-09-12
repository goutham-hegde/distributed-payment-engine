package com.dpe.account.saga;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.domain.EntryType;
import com.dpe.account.domain.Hold;
import com.dpe.account.domain.HoldStatus;
import com.dpe.account.domain.LedgerEntry;
import com.dpe.account.domain.TransferVoid;
import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.HoldRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import com.dpe.account.repository.TransferVoidRepository;
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
 * and one of them is a CLEARING shard that other sagas touch at the same moment. Locking "the
 * sender, then clearing" is a per-transfer order, not a global one, and two sagas can still form a
 * cycle. The two ids are sorted and the lower one is locked first, exactly as
 * {@code TransferService} does, so deadlock is structurally impossible rather than merely
 * detected and retried. ({@code UUID.compareTo} is SIGNED, so for about half of all random ids the
 * sender sorts before the shard. That is fine - one total order is all the argument needs - but
 * "the shard is always locked first" is not true, and nothing may rely on it.)
 *
 * <p><b>CLEARING is sharded (M8, V8).</b> With one clearing row, every reserve and commit in the
 * system took the same row lock and held it to commit, so account-service ran one money movement at
 * a time however many consumer threads it had. A reserve now picks one of several shards
 * ({@link ClearingAccounts}) and RECORDS it on the hold; commit and release read it back. They
 * never recompute it: the commit/release mutual exclusion below depends on both writing the SAME
 * clearing leg, and a recomputed shard is only the same one while the shard list never changes.
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
 *
 * <h2>Two rules added by the M7 chaos suite</h2>
 *
 * <p><b>Every command is answered with the truth.</b> A commit or release that finds its hold
 * already settled replies with the event describing how it settled, rather than logging and
 * returning. A saga that gets no reply can only learn from its deadline.
 *
 * <p><b>A compensation commutes with the reserve it undoes.</b> Commands are addressed by transfer
 * id, and a release that finds no hold records a tombstone ({@link TransferVoid}) that refuses the
 * reserve if it arrives later. Every method takes the per-transfer lock
 * ({@link TransferVoidRepository#lockTransfer}) before any other, so the check-then-write on each
 * side cannot interleave with the other.
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledgerEntries;
    private final HoldRepository holds;
    private final TransferVoidRepository voids;
    private final ClearingAccounts clearingAccounts;
    private final OutboxWriter outbox;

    public ReservationService(AccountRepository accounts, LedgerEntryRepository ledgerEntries,
                              HoldRepository holds, TransferVoidRepository voids,
                              ClearingAccounts clearingAccounts, OutboxWriter outbox) {
        this.accounts = accounts;
        this.ledgerEntries = ledgerEntries;
        this.holds = holds;
        this.voids = voids;
        this.clearingAccounts = clearingAccounts;
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
        // M7, Fix C. Serialize with any compensation for this transfer, then honour one that got
        // here first. This is the half of the tombstone that makes a late reserve harmless: the
        // saga has already told the customer "failed", so the only correct amount to reserve now
        // is nothing - and the refusal is still a reply, so the saga hears the truth.
        voids.lockTransfer(command.transferId());
        if (voids.existsById(command.transferId())) {
            reject(command, ReserveRejected.TRANSFER_VOIDED,
                    "the saga gave up on this transfer before the reserve arrived");
            return;
        }

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

        // M8: which CLEARING shard this transfer parks its money in. Chosen here, once, and written
        // onto the hold below; commit and release read it from the hold and never ask again. No
        // shard in this currency is the same refusal the currency check further down gives - it is
        // just discovered before any row is locked, since there is nothing to lock.
        Optional<UUID> shard = clearingAccounts.forTransfer(command.transferId(), command.currency());
        if (shard.isEmpty()) {
            reject(command, ReserveRejected.CURRENCY_MISMATCH,
                    "no clearing account in " + command.currency());
            return;
        }
        UUID clearingId = shard.get();

        // Sorted by id, never by the role the account plays in this transfer. Every saga touches a
        // CLEARING shard, so a role-based order would put one on both sides of a cycle. With the
        // shards this matters MORE than it did with one clearing row, not less: two reserves can now
        // genuinely run at once (three consumer threads), and they deadlock the moment one locks
        // sender-then-shard while another locks shard-then-sender. One global order (UUID.compareTo,
        // the same comparator TransferService uses) makes that cycle impossible to form.
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
        holds.save(new Hold(holdId, command.transferId(), sender.getId(), clearing.getId(), amount,
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
        voids.lockTransfer(command.transferId());
        Optional<Hold> maybeHold = holds.findByTransferIdForUpdate(command.transferId());
        if (maybeHold.isEmpty() || !maybeHold.get().getId().equals(command.holdId())) {
            // The orchestrator is quoting an id from another database, or from a wiped one. A
            // saga only sends CommitFunds from CHARGED, which it reaches only after this service
            // replied FundsReserved - so no saga is waiting on an answer to this, and there is no
            // true answer to give. Logged and dropped rather than thrown: creating a hold here
            // would invent money, and retrying forever will not make the row appear.
            log.error("CommitFunds names hold {} which is not transfer {}'s hold ({})",
                    command.holdId(), command.transferId(),
                    maybeHold.map(h -> h.getId().toString()).orElse("none"));
            return;
        }
        Hold hold = maybeHold.get();

        if (!hold.isActive()) {
            // A second copy of a commit the saga re-sent because it had not heard the first
            // reply (M7, Fix A), or a commit that lost a race to a release.
            //
            // Not thrown - throwing would roll back the inbox row and redeliver a command that
            // can never succeed, forever. And, since M7, not silent either: chaos scenario 2 showed
            // a saga that got no reply can only learn from its deadline, which had already passed.
            // The third option is the truth - answer with what actually happened to the hold.
            answerWithWhatHappened(hold, CommitFunds.TYPE, null);
            return;
        }

        // The shard the reserve parked the money in - read from the hold, never recomputed (V8).
        UUID clearingId = hold.getClearingAccountId();
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
        // Addressed by TRANSFER id (M7). A saga that timed out in STARTED never heard which hold
        // it got, or whether it got one - so it names the transfer and leaves holdId null.
        voids.lockTransfer(command.transferId());
        Optional<Hold> maybeHold = holds.findByTransferIdForUpdate(command.transferId());
        if (maybeHold.isEmpty()) {
            // Fix C, the other half of the tombstone. Nothing has been reserved for this transfer
            // YET - the ReserveFunds may still be in the orchestrator's outbox, in the topic, or in
            // this service's dead letter table. Remember the void so that the reserve, whenever it
            // lands, is refused; then answer. "Nothing was reserved, and nothing will be" is
            // precisely what ReserveRejected means, so it is the reply rather than a new type.
            if (!voids.existsById(command.transferId())) {
                voids.save(new TransferVoid(command.transferId(), command.reason()));
            }
            log.info("ReleaseFunds for transfer {} arrived before any reserve; transfer voided",
                    command.transferId());
            outbox.append("Transfer", command.transferId(), Topics.ACCOUNT_EVENTS,
                    ReserveRejected.TYPE,
                    new ReserveRejected(command.transferId(), null,
                            ReserveRejected.TRANSFER_VOIDED,
                            "released before any reserve; nothing will be reserved for it"));
            return;
        }
        Hold hold = maybeHold.get();
        if (command.holdId() != null && !command.holdId().equals(hold.getId())) {
            // The transfer id is the address; a disagreeing hold id means the sender's state is
            // corrupt, not that the money should stay put. Release what the transfer holds.
            log.error("ReleaseFunds for transfer {} quotes hold {} but the transfer's hold is {}; "
                    + "releasing by transfer id", command.transferId(), command.holdId(),
                    hold.getId());
        }

        if (!hold.isActive()) {
            // A redelivered or re-sent release, or one that lost the race to a commit. Not
            // thrown, for the reason in commit(); answered, for the reason in commit(). If the hold
            // was COMMITTED the answer is FundsCommitted: the recipient has the money, and a saga
            // that believed it was compensating needs to learn that.
            answerWithWhatHappened(hold, ReleaseFunds.TYPE, command.reason());
            return;
        }

        // The shard the reserve parked the money in - read from the hold, never recomputed (V8).
        UUID clearingId = hold.getClearingAccountId();
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

        hold.release(command.reason());

        outbox.append("Transfer", hold.getTransferId(), Topics.ACCOUNT_EVENTS,
                FundsReleased.TYPE,
                new FundsReleased(hold.getTransferId(), hold.getId(), sender.getId(), amount,
                        hold.getCurrency(), command.reason()));
    }

    // ------------------------------------------------------------------ helpers

    /**
     * M7, Fix B: replies to a command about a hold that has already settled, with the event that
     * describes how it settled - whichever command was asked.
     *
     * <p>The reply is a statement of fact about the hold, not an acknowledgement of the command, so
     * a CommitFunds that finds a RELEASED hold is answered FundsReleased and vice versa. The saga
     * decides what that means for it; this service only refuses to leave it guessing.
     *
     * @param fallbackReason used for a hold released before V7 recorded the reason on the row
     */
    private void answerWithWhatHappened(Hold hold, String asked, String fallbackReason) {
        log.warn("{} for transfer {}: hold {} is already {}; replying with that outcome",
                asked, hold.getTransferId(), hold.getId(), hold.getStatus());

        if (hold.getStatus() == HoldStatus.COMMITTED) {
            outbox.append("Transfer", hold.getTransferId(), Topics.ACCOUNT_EVENTS,
                    FundsCommitted.TYPE,
                    new FundsCommitted(hold.getTransferId(), hold.getId(),
                            recipientOf(hold), hold.getAmountMinor(), hold.getCurrency()));
        } else {
            String reason = hold.getReleaseReason() != null ? hold.getReleaseReason()
                    : fallbackReason;
            outbox.append("Transfer", hold.getTransferId(), Topics.ACCOUNT_EVENTS,
                    FundsReleased.TYPE,
                    new FundsReleased(hold.getTransferId(), hold.getId(), hold.getAccountId(),
                            hold.getAmountMinor(), hold.getCurrency(), reason));
        }
    }

    /**
     * Who a committed hold paid. The hold does not record it - the commit's CREDIT leg does, and
     * it is the only CREDIT for the transfer that did not go to the hold's CLEARING shard.
     */
    private UUID recipientOf(Hold hold) {
        return ledgerEntries.findByTransferIdOrderByIdAsc(hold.getTransferId()).stream()
                .filter(e -> e.getEntryType() == EntryType.CREDIT)
                .map(LedgerEntry::getAccountId)
                .filter(id -> !id.equals(hold.getClearingAccountId()))
                .findFirst()
                .orElse(null);
    }

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
