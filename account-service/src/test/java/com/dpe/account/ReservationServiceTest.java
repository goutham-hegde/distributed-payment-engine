package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.domain.AccountType;
import com.dpe.account.saga.ReservationService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import com.dpe.events.CommitFunds;
import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import com.dpe.events.ReserveRejected;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * THE SPECIFICATION for {@link ReservationService}. These tests are red until you write it.
 *
 * <p>Read them as the requirements document for the three method bodies, in order. Each one
 * states a property that must hold; none of them tells you how to satisfy it.
 *
 * <p>No broker here. Every property below is a database property, and putting Kafka in the middle
 * would add timing noise to assertions about a transaction. The delivery path has its own tests.
 */
@Import(TestLedger.class)
class ReservationServiceTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    ReservationService reservations;

    @Autowired
    TestLedger ledger;

    // ------------------------------------------------------------------ reserve

    @Test
    @DisplayName("reserve debits the sender, credits CLEARING, and creates an ACTIVE hold")
    void reserveMovesMoneyIntoClearing() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR, TestLedger.OWNER));

        assertThat(ledger.balanceOf(sender))
                .as("the money has genuinely left the sender - a hold is not a promise")
                .isEqualTo(70_000L);
        assertThat(ledger.balanceOf(AccountType.CLEARING_ACCOUNT_ID))
                .as("and it is sitting in CLEARING, which is where money in flight lives")
                .isEqualTo(30_000L);
        assertThat(ledger.balanceOf(recipient))
                .as("the recipient gets nothing until the hold is committed")
                .isZero();

        Map<String, Object> hold = jdbc.queryForMap(
                "SELECT * FROM holds WHERE transfer_id = ?", transferId);
        assertThat(hold.get("status")).isEqualTo("ACTIVE");
        assertThat(hold.get("amount_minor")).isEqualTo(30_000L);
        assertThat(hold.get("account_id"))
                .as("the hold names the account the money came FROM, so a release knows where "
                        + "to send it back")
                .isEqualTo(sender);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    @Test
    @DisplayName("reserve emits FundsReserved carrying the hold id")
    void reserveEmitsTheReply() {
        UUID sender = ledger.seedAccount(50_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 20_000L, INR, TestLedger.OWNER));

        Map<String, Object> message = jdbc.queryForMap(
                "SELECT event_type, payload->'payload'->>'holdId' AS hold_id, aggregate_id "
                        + "FROM outbox WHERE aggregate_id = ?", transferId);

        assertThat(message.get("event_type")).isEqualTo(FundsReserved.TYPE);
        assertThat(message.get("aggregate_id"))
                .as("keyed by transfer id, so one saga's messages share a partition and stay "
                        + "in order relative to each other")
                .isEqualTo(transferId);

        UUID holdIdInMessage = UUID.fromString((String) message.get("hold_id"));
        UUID holdIdInTable = jdbc.queryForObject(
                "SELECT id FROM holds WHERE transfer_id = ?", UUID.class, transferId);
        assertThat(holdIdInMessage)
                .as("the orchestrator addresses the hold by this id in CommitFunds and "
                        + "ReleaseFunds, so it must be the real one")
                .isEqualTo(holdIdInTable);
    }

    // Note for whoever reads a failure here: fields are read with ->> rather than by string
    // matching payload::text. Postgres reparses jsonb and re-renders it with its own key order
    // and spacing, so a substring assertion tests Postgres's serializer, not your producer.

    @Test
    @DisplayName("an unaffordable reserve COMMITS a rejection instead of throwing")
    void insufficientFundsIsRejectedNotThrown() {
        UUID sender = ledger.seedAccount(10_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        // Must not throw. Throwing would roll back the handler's transaction, take the inbox row
        // with it, and put the message into an infinite redelivery loop over a condition that is
        // never going to change. A business failure has to commit.
        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 999_999L, INR, TestLedger.OWNER));

        assertThat(ledger.entryCountFor(transferId))
                .as("a rejected reserve must post no ledger entries at all")
                .isZero();
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM holds WHERE transfer_id = ?", Integer.class, transferId))
                .as("and must create no hold - there is nothing to compensate later")
                .isZero();
        assertThat(ledger.balanceOf(sender)).isEqualTo(10_000L);

        Map<String, Object> message = jdbc.queryForMap(
                "SELECT event_type, payload->'payload'->>'reason' AS reason FROM outbox "
                        + "WHERE aggregate_id = ?", transferId);
        assertThat(message.get("event_type")).isEqualTo(ReserveRejected.TYPE);
        assertThat(message.get("reason")).isEqualTo(ReserveRejected.INSUFFICIENT_FUNDS);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ------------------------------------------------------------------ commit

    @Test
    @DisplayName("commit moves the held money out of CLEARING and into the recipient")
    void commitSettlesTheHold() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR, TestLedger.OWNER));
        UUID holdId = holdIdFor(transferId);

        reservations.commit(new CommitFunds(transferId, holdId, recipient));

        assertThat(ledger.balanceOf(sender)).isEqualTo(70_000L);
        assertThat(ledger.balanceOf(recipient)).isEqualTo(30_000L);
        assertThat(ledger.balanceOf(AccountType.CLEARING_ACCOUNT_ID))
                .as("CLEARING is back to zero: nothing is in flight any more")
                .isZero();
        assertThat(statusOfHold(holdId)).isEqualTo("COMMITTED");

        assertThat(outboxTypeFor(transferId, FundsCommitted.TYPE))
                .as("the saga only learns the transfer completed from this reply")
                .isEqualTo(1);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ------------------------------------------------------------------ release: THE COMPENSATION

    @Test
    @DisplayName("release returns the money to the sender and leaves both postings in the ledger")
    void releaseCompensates() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR, TestLedger.OWNER));
        UUID holdId = holdIdFor(transferId);

        reservations.release(new ReleaseFunds(transferId, holdId, ReleaseFunds.GATEWAY_DECLINED));

        assertThat(ledger.balanceOf(sender))
                .as("the sender is whole again")
                .isEqualTo(100_000L);
        assertThat(ledger.balanceOf(recipient))
                .as("the recipient never received anything")
                .isZero();
        assertThat(ledger.balanceOf(AccountType.CLEARING_ACCOUNT_ID)).isZero();
        assertThat(statusOfHold(holdId)).isEqualTo("RELEASED");

        assertThat(ledger.entryCountFor(transferId))
                .as("COMPENSATION IS NOT ROLLBACK. Four entries, not zero: the reserve's debit "
                        + "and credit are still there, and the release wrote two more. The "
                        + "sender's statement shows the money leaving AND coming back, which is "
                        + "what actually happened.")
                .isEqualTo(4);

        assertThat(outboxTypeFor(transferId, FundsReleased.TYPE)).isEqualTo(1);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    @Test
    @DisplayName("a transfer can be committed or released, never both - enforced by the schema")
    void commitAndReleaseAreMutuallyExclusive() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR, TestLedger.OWNER));
        UUID holdId = holdIdFor(transferId);
        reservations.release(new ReleaseFunds(transferId, holdId, ReleaseFunds.SAGA_TIMEOUT));

        // The race this stands in for is real: the sweeper compensates a saga whose approval was
        // merely slow, and the CommitFunds arrives afterwards. Whatever your code does about it,
        // the money must not move twice - and the reason it cannot is the UNIQUE constraint on
        // (transfer_id, account_id, entry_type), which both settlements would violate on the
        // CLEARING debit. You are choosing the error, not the safety.
        try {
            reservations.commit(new CommitFunds(transferId, holdId, recipient));
        } catch (RuntimeException expected) {
            // Any failure is acceptable here. Silently succeeding is not.
        }

        assertThat(ledger.balanceOf(sender))
                .as("the sender must still be whole - a double settlement would show up here")
                .isEqualTo(100_000L);
        assertThat(ledger.balanceOf(recipient)).isZero();
        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ------------------------------------------------------------------ helpers

    private UUID holdIdFor(UUID transferId) {
        return jdbc.queryForObject(
                "SELECT id FROM holds WHERE transfer_id = ?", UUID.class, transferId);
    }

    private String statusOfHold(UUID holdId) {
        return jdbc.queryForObject(
                "SELECT status FROM holds WHERE id = ?", String.class, holdId);
    }

    private int outboxTypeFor(UUID transferId, String eventType) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                Integer.class, eventType, transferId);
        return count == null ? 0 : count;
    }
}
