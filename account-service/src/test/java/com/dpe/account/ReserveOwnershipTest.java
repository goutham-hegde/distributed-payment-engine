package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.saga.ReservationService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import com.dpe.events.ReserveFunds;
import com.dpe.events.ReserveRejected;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * The authoritative half of M5's authorization: account-service checks ownership itself, under
 * the lock that moves the money.
 *
 * <p>Why check again, when the orchestrator already refused the same request with a 403? Because
 * the two checks answer the same question from different positions. The orchestrator reads a
 * PROJECTION - a copy, fed asynchronously, which can be stale or absent. This service reads the
 * column, in the row, under the {@code FOR UPDATE} it is about to write through. And because
 * {@code dpe.account.commands.v1} is reachable by anything that can produce to it: a replayed
 * dead letter, a hand-produced message (M4 proved that is not hypothetical), a compromised
 * producer. "It came through the API" is an assumption, and this check makes it unnecessary.
 *
 * <p>Every rejection here <b>commits</b>. That is the rule this file shares with every other
 * refusal in the reserve path: a business outcome writes its reply and returns, so the saga is
 * told and can fail cleanly. Throwing would roll back the inbox row and redeliver a command whose
 * ownership will never change - an infinite retry loop around a permanent condition.
 */
@Import(TestLedger.class)
class ReserveOwnershipTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    ReservationService reservations;

    @Autowired
    TestLedger ledger;

    @Test
    @DisplayName("a reserve initiated by somebody who does not own the sender is rejected")
    void rejectsWhenSubjectDoesNotOwnTheAccount() {
        UUID sender = ledger.seedAccountOwnedBy("alice", 100_000L);
        UUID recipient = ledger.seedAccountOwnedBy("bob", 0L);
        UUID transferId = UUID.randomUUID();

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR,
                "mallory"));

        Map<String, Object> reply = jdbc.queryForMap(
                "SELECT event_type, payload->'payload'->>'reason' AS reason "
                        + "FROM outbox WHERE aggregate_id = ?", transferId);
        assertThat(reply.get("event_type")).isEqualTo(ReserveRejected.TYPE);
        assertThat(reply.get("reason")).isEqualTo(ReserveRejected.NOT_ACCOUNT_OWNER);

        assertThat(ledger.balanceOf(sender))
                .as("not a paise moved")
                .isEqualTo(100_000L);
        assertThat(ledger.entryCountFor(transferId))
                .as("a refused reserve posts no ledger entries at all")
                .isZero();
        assertThat(holdCount(transferId))
                .as("and creates no hold, so there is nothing to compensate")
                .isZero();
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("a command with no subject at all is rejected, not trusted")
    void rejectsWhenSubjectIsMissing() {
        // The rolling-upgrade case: adding a field to a record is backward compatible on the
        // wire, so an old orchestrator's command arrives with initiatedBy null. Those are exactly
        // the messages nobody authorized, and "I cannot tell who asked" is not a reason to move
        // money.
        UUID sender = ledger.seedAccountOwnedBy("alice", 100_000L);
        UUID recipient = ledger.seedAccountOwnedBy("bob", 0L);
        UUID transferId = UUID.randomUUID();

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR, null));

        assertThat(jdbc.queryForObject(
                "SELECT payload->'payload'->>'reason' FROM outbox WHERE aggregate_id = ?",
                String.class, transferId))
                .isEqualTo(ReserveRejected.NOT_ACCOUNT_OWNER);
        assertThat(ledger.balanceOf(sender)).isEqualTo(100_000L);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("the owner's own reserve still goes through - the check refuses nothing else")
    void ownersReserveSucceeds() {
        UUID sender = ledger.seedAccountOwnedBy("alice", 100_000L);
        UUID recipient = ledger.seedAccountOwnedBy("bob", 0L);
        UUID transferId = UUID.randomUUID();

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR,
                "alice"));

        assertThat(ledger.balanceOf(sender)).isEqualTo(70_000L);
        assertThat(ledger.clearingBalance())
                .as("money in flight lives in CLEARING, never nowhere")
                .isEqualTo(30_000L);
        assertThat(holdCount(transferId)).isEqualTo(1);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("being paid needs no consent - only spending does")
    void recipientOwnershipIsNotChecked() {
        // Worth pinning as a test rather than leaving as an omission somebody later "fixes":
        // the check is on the account money leaves, not the one it arrives in. Anyone may
        // receive a payment.
        UUID sender = ledger.seedAccountOwnedBy("alice", 100_000L);
        UUID recipient = ledger.seedAccountOwnedBy("someone-else-entirely", 0L);
        UUID transferId = UUID.randomUUID();

        reservations.reserve(new ReserveFunds(transferId, sender, recipient, 30_000L, INR,
                "alice"));

        assertThat(ledger.balanceOf(sender)).isEqualTo(70_000L);
        LedgerInvariants.assertAll(jdbc);
    }

    private int holdCount(UUID transferId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM holds WHERE transfer_id = ?", Integer.class, transferId);
        return count == null ? 0 : count;
    }
}
