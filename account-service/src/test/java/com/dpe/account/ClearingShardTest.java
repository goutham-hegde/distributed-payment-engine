package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.account.domain.AccountType;
import com.dpe.account.saga.ClearingAccounts;
import com.dpe.account.saga.ReservationService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.Concurrently;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import com.dpe.events.CommitFunds;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * M8: CLEARING is sharded (V8), and the three properties that make that safe.
 *
 * <ol>
 *   <li>Money is parked in one of several clearing accounts, and every shard still balances
 *       against the holds that name it.</li>
 *   <li>A hold is settled against the shard it RECORDED, never one recomputed from the transfer
 *       id - otherwise adding a shard splits a transfer's legs across two accounts and the
 *       commit/release mutual exclusion stops covering it.</li>
 *   <li>With the hot row gone, reserves and commits genuinely run in parallel, and the lock order
 *       keeps them deadlock-free.</li>
 * </ol>
 */
@Import(TestLedger.class)
class ClearingShardTest extends AbstractPostgresIT {

    private static final String INR = "INR";
    private static final UUID ORIGINAL_CLEARING = AccountType.CLEARING_ACCOUNT_ID;

    @Autowired
    ReservationService reservations;

    @Autowired
    ClearingAccounts clearingAccounts;

    @Autowired
    TestLedger ledger;

    @Test
    @DisplayName("reserves park money across several clearing shards, each holding exactly its holds")
    void reservesSpreadAcrossShards() {
        UUID sender = ledger.seedAccount(1_000_000L);
        UUID recipient = ledger.seedAccount(0L);

        List<UUID> transfers = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            UUID transferId = UUID.randomUUID();
            transfers.add(transferId);
            reservations.reserve(new ReserveFunds(transferId, sender, recipient, 1_000L, INR,
                    TestLedger.OWNER));
        }

        Set<UUID> used = new HashSet<>();
        for (UUID transferId : transfers) {
            UUID shard = ledger.clearingShardOf(transferId);
            used.add(shard);
            assertThat(jdbc.queryForObject(
                    "SELECT account_id FROM ledger_entries WHERE transfer_id = ? AND entry_type = 'CREDIT'",
                    UUID.class, transferId))
                    .as("the reserve's credit leg lands in the shard the hold records")
                    .isEqualTo(shard);
        }
        // Eight shards and forty random transfer ids: fewer than four distinct would be a broken
        // hash (or one shard), not bad luck.
        assertThat(used).hasSizeGreaterThanOrEqualTo(4);
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM accounts WHERE account_type = 'CLEARING'", Integer.class))
                .isEqualTo(8);

        assertThat(ledger.clearingBalance()).isEqualTo(40_000L);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("a commit debits the shard the hold recorded, not the one the transfer id maps to now")
    void commitSettlesAgainstTheRecordedShard() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID recipient = ledger.seedAccount(0L);
        UUID transferId = transferMappedAwayFrom(ORIGINAL_CLEARING);
        UUID mappedShard = clearingAccounts.forTransfer(transferId, INR).orElseThrow();
        UUID holdId = holdParkedInOriginalClearing(transferId, sender, 30_000L);

        reservations.commit(new CommitFunds(transferId, holdId, recipient));

        assertThat(ledger.balanceOf(recipient)).isEqualTo(30_000L);
        assertThat(ledger.balanceOf(ORIGINAL_CLEARING))
                .as("the money leaves the account it was parked in")
                .isZero();
        assertThat(ledger.balanceOf(mappedShard))
                .as("and never touches the shard a recomputation would have picked - that would "
                        + "leave it at -30,000 and the original at +30,000 forever")
                .isZero();
        assertThat(clearingDebitAccount(transferId)).isEqualTo(ORIGINAL_CLEARING);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("a release debits the shard the hold recorded, too")
    void releaseSettlesAgainstTheRecordedShard() {
        UUID sender = ledger.seedAccount(100_000L);
        UUID transferId = transferMappedAwayFrom(ORIGINAL_CLEARING);
        UUID holdId = holdParkedInOriginalClearing(transferId, sender, 30_000L);

        reservations.release(new ReleaseFunds(transferId, holdId, ReleaseFunds.SAGA_TIMEOUT));

        assertThat(ledger.balanceOf(sender)).isEqualTo(100_000L);
        assertThat(ledger.balanceOf(ORIGINAL_CLEARING)).isZero();
        assertThat(clearingDebitAccount(transferId)).isEqualTo(ORIGINAL_CLEARING);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("the database refuses a hold parked in anything but a CLEARING account")
    void holdMustBeParkedInAClearingAccount() {
        UUID customer = ledger.seedAccount(0L);

        // A plain foreign key would take this: a customer's balance would then be carrying
        // somebody else's money in flight.
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO holds (id, transfer_id, account_id, clearing_account_id, amount_minor,
                                   currency, status)
                VALUES (?, ?, ?, ?, 1000, 'INR', 'ACTIVE')
                """, UUID.randomUUID(), UUID.randomUUID(), customer, customer))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("holds_parked_in_a_clearing_account");

        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO holds (id, transfer_id, account_id, clearing_account_id,
                                   clearing_account_type, amount_minor, currency, status)
                VALUES (?, ?, ?, ?, 'SYSTEM', 1000, 'INR', 'ACTIVE')
                """, UUID.randomUUID(), UUID.randomUUID(), customer, AccountType.SYSTEM_ACCOUNT_ID))
                .as("and the type column cannot be pointed at another type to get round it")
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("holds_clearing_account_type_is_clearing");
    }

    @Test
    @DisplayName("45 transfers round a ring of accounts, reserved and committed in parallel: no deadlock, exact balances")
    void parallelReservesAndCommitsAreDeadlockFree() {
        List<UUID> ring = List.of(ledger.seedAccount(1_000_000L), ledger.seedAccount(1_000_000L),
                ledger.seedAccount(1_000_000L));
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        // A RING - A pays B, B pays C, C pays A - so every account is a sender in some transfers and
        // a recipient in others at the same moment. That is what gives a wrong lock order something
        // to deadlock on: a reserve out of A locks (A, shard) while a commit into A locks (shard, A).
        // With disjoint senders and recipients a per-role order ("sender, then clearing, then
        // recipient") is accidentally global and this test would pass it. Eight shards are shared
        // by all forty-five transfers, so they meet too. Only the one global id order survives.
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            UUID from = ring.get(i % 3);
            UUID to = ring.get((i + 1) % 3);
            tasks.add(() -> {
                UUID transferId = UUID.randomUUID();
                reservations.reserve(new ReserveFunds(transferId, from, to, 1_000L, INR,
                        TestLedger.OWNER));
                UUID holdId = jdbc.queryForObject(
                        "SELECT id FROM holds WHERE transfer_id = ?", UUID.class, transferId);
                reservations.commit(new CommitFunds(transferId, holdId, to));
                return null;
            });
        }

        List<Concurrently.Outcome<Void>> outcomes = Concurrently.runAll(tasks);

        assertThat(outcomes.stream().map(Concurrently.Outcome::failure).filter(f -> f != null))
                .as("no task failed - in particular, no deadlock was detected")
                .isEmpty();
        for (UUID account : ring) {
            assertThat(ledger.balanceOf(account))
                    .as("each account paid fifteen transfers and received fifteen")
                    .isEqualTo(1_000_000L);
            assertThat(jdbc.queryForObject(
                    "SELECT COUNT(*) FROM ledger_entries WHERE account_id = ?", Integer.class, account))
                    .as("one funding leg, fifteen reserve debits, fifteen commit credits")
                    .isEqualTo(31);
        }
        assertThat(ledger.clearingBalance()).as("everything committed; nothing left in flight").isZero();
        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ------------------------------------------------------------------ fixtures

    /** A transfer id whose shard, computed today, is NOT the given account. */
    private UUID transferMappedAwayFrom(UUID shard) {
        while (true) {
            UUID candidate = UUID.randomUUID();
            if (!clearingAccounts.forTransfer(candidate, INR).orElseThrow().equals(shard)) {
                return candidate;
            }
        }
    }

    /**
     * A reserve exactly as it was written before V8: the money parked in the original clearing
     * account and the hold naming it - which is also how V8 backfills every pre-existing hold.
     */
    private UUID holdParkedInOriginalClearing(UUID transferId, UUID sender, long amount) {
        jdbc.update("""
                INSERT INTO ledger_entries (transfer_id, account_id, amount_minor, entry_type, currency)
                VALUES (?, ?, ?, 'DEBIT', 'INR'), (?, ?, ?, 'CREDIT', 'INR')
                """, transferId, sender, -amount, transferId, ORIGINAL_CLEARING, amount);
        jdbc.update("UPDATE accounts SET balance_minor = balance_minor - ? WHERE id = ?", amount, sender);
        jdbc.update("UPDATE accounts SET balance_minor = balance_minor + ? WHERE id = ?", amount,
                ORIGINAL_CLEARING);
        UUID holdId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO holds (id, transfer_id, account_id, clearing_account_id, amount_minor,
                                   currency, status)
                VALUES (?, ?, ?, ?, ?, 'INR', 'ACTIVE')
                """, holdId, transferId, sender, ORIGINAL_CLEARING, amount);
        LedgerInvariants.assertAll(jdbc);
        return holdId;
    }

    private UUID clearingDebitAccount(UUID transferId) {
        return jdbc.queryForObject("""
                SELECT e.account_id FROM ledger_entries e JOIN accounts a ON a.id = e.account_id
                 WHERE e.transfer_id = ? AND e.entry_type = 'DEBIT' AND a.account_type = 'CLEARING'
                """, UUID.class, transferId);
    }
}
