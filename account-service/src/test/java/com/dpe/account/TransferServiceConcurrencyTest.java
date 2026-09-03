package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.account.service.AccountNotFoundException;
import com.dpe.account.service.InsufficientFundsException;
import com.dpe.account.service.InvalidTransferException;
import com.dpe.account.service.TransferCommand;
import com.dpe.account.service.TransferService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.Concurrently;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.PessimisticLockingFailureException;

/**
 * The specification for {@code TransferService.transfer}.
 *
 * <p>Every test here fails until that method is implemented, and each one fails for a different
 * reason. Read them as a checklist:
 *
 * <ul>
 *   <li>{@link #singleTransferPostsABalancedPair()} - double-entry basics</li>
 *   <li>{@link #hotAccountUnderConcurrencyLosesNoUpdate()} - {@code FOR UPDATE} is present</li>
 *   <li>{@link #concurrentOverdraftAttemptsNeverGoNegative()} - the balance check sits inside
 *       the lock</li>
 *   <li>{@link #opposingTransfersDoNotDeadlock()} - locks are acquired in a global order</li>
 *   <li>the validation tests - the cheap rules that must run before any row is touched</li>
 * </ul>
 */
@Import(TestLedger.class)
class TransferServiceConcurrencyTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    private TransferService transfers;

    @Autowired
    private TestLedger ledger;

    // ---------------------------------------------------------------- double entry

    @Test
    @DisplayName("a transfer posts two entries summing to zero and moves both balances")
    void singleTransferPostsABalancedPair() {
        UUID alice = ledger.seedAccount(1_000_00L);
        UUID bob = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();

        transfers.transfer(new TransferCommand(transferId, alice, bob, 300_00L, INR));

        assertThat(ledger.balanceOf(alice)).isEqualTo(700_00L);
        assertThat(ledger.balanceOf(bob)).isEqualTo(300_00L);
        assertThat(ledger.entryCountFor(transferId))
                .as("exactly one debit and one credit")
                .isEqualTo(2);
        LedgerInvariants.assertAll(jdbc);
    }

    // ---------------------------------------------------------------- the lost update

    @Test
    @DisplayName("32 concurrent transfers off one hot account lose no update")
    void hotAccountUnderConcurrencyLosesNoUpdate() {
        int transferCount = 32;
        long amount = 10_00L;

        UUID source = ledger.seedAccount(transferCount * amount);
        List<UUID> destinations = new ArrayList<>();
        for (int i = 0; i < transferCount; i++) {
            destinations.add(ledger.seedAccount(0L));
        }
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (UUID destination : destinations) {
            tasks.add(() -> {
                transfers.transfer(
                        new TransferCommand(UUID.randomUUID(), source, destination, amount, INR));
                return null;
            });
        }
        List<Concurrently.Outcome<Void>> outcomes = Concurrently.runAll(tasks);

        assertThat(outcomes)
                .as("every transfer is funded, so every transfer must succeed")
                .allMatch(Concurrently.Outcome::succeeded);

        // Without SELECT ... FOR UPDATE this is the assertion that fails: concurrent readers all
        // see the same starting balance, each writes back its own view of the result, and the
        // source is left holding money it has already sent.
        assertThat(ledger.balanceOf(source))
                .as("source must be drained exactly - a leftover balance means a lost update")
                .isZero();
        destinations.forEach(destination ->
                assertThat(ledger.balanceOf(destination)).isEqualTo(amount));

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    @Test
    @DisplayName("concurrent overdraft attempts: only the funded ones succeed, never negative")
    void concurrentOverdraftAttemptsNeverGoNegative() {
        long amount = 100_00L;
        int funded = 10;
        int attempts = 40;

        UUID source = ledger.seedAccount(funded * amount);
        UUID destination = ledger.seedAccount(0L);
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        List<Concurrently.Outcome<Void>> outcomes = Concurrently.run(attempts, () -> {
            transfers.transfer(
                    new TransferCommand(UUID.randomUUID(), source, destination, amount, INR));
            return null;
        });

        long succeeded = outcomes.stream().filter(Concurrently.Outcome::succeeded).count();
        long rejected = outcomes.stream()
                .filter(outcome -> outcome.failedWith(InsufficientFundsException.class))
                .count();

        assertThat(succeeded)
                .as("exactly the funded number of transfers may succeed")
                .isEqualTo(funded);
        assertThat(rejected)
                .as("every other attempt must be rejected as insufficient funds, not by a "
                        + "constraint violation or a lock error")
                .isEqualTo(attempts - funded);
        assertThat(ledger.balanceOf(source)).isZero();
        assertThat(ledger.balanceOf(destination)).isEqualTo(funded * amount);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ---------------------------------------------------------------- deadlock ordering

    @Test
    @DisplayName("opposing transfers between the same pair do not deadlock")
    void opposingTransfersDoNotDeadlock() {
        long amount = 1_00L;
        int rounds = 40;

        UUID alice = ledger.seedAccount(500_00L);
        UUID bob = ledger.seedAccount(500_00L);
        long baseline = LedgerInvariants.totalCustomerMoney(jdbc);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            // Half go alice to bob, half go bob to alice, all released at once. If locks are
            // taken in the order the accounts appear in the command rather than in a global
            // order, one thread holds alice and wants bob while another holds bob and wants
            // alice. Postgres detects the cycle and kills one transaction, which arrives here
            // as a CannotAcquireLockException.
            boolean forward = i % 2 == 0;
            UUID from = forward ? alice : bob;
            UUID to = forward ? bob : alice;
            tasks.add(() -> {
                transfers.transfer(new TransferCommand(UUID.randomUUID(), from, to, amount, INR));
                return null;
            });
        }
        List<Concurrently.Outcome<Void>> outcomes = Concurrently.runAll(tasks);

        List<Throwable> lockFailures = outcomes.stream()
                .map(Concurrently.Outcome::failure)
                .filter(failure -> failure instanceof CannotAcquireLockException
                        || failure instanceof PessimisticLockingFailureException)
                .toList();

        assertThat(lockFailures)
                .as("deadlock must be structurally impossible: sort the account ids and always "
                        + "lock the lower one first, whatever direction the money moves")
                .isEmpty();
        assertThat(outcomes).allMatch(Concurrently.Outcome::succeeded);

        // Equal traffic in both directions, so both accounts end where they started.
        assertThat(ledger.balanceOf(alice)).isEqualTo(500_00L);
        assertThat(ledger.balanceOf(bob)).isEqualTo(500_00L);

        LedgerInvariants.assertAll(jdbc);
        LedgerInvariants.assertI3TotalIsConserved(jdbc, baseline);
    }

    // ---------------------------------------------------------------- validation

    @Test
    @DisplayName("a self-transfer is rejected before any row is locked")
    void selfTransferIsRejected() {
        UUID alice = ledger.seedAccount(1_000_00L);

        assertThatThrownBy(() -> transfers.transfer(
                new TransferCommand(UUID.randomUUID(), alice, alice, 100_00L, INR)))
                .isInstanceOf(InvalidTransferException.class);

        assertThat(ledger.balanceOf(alice)).isEqualTo(1_000_00L);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("a non-positive amount is rejected")
    void nonPositiveAmountIsRejected() {
        UUID alice = ledger.seedAccount(1_000_00L);
        UUID bob = ledger.seedAccount(0L);

        assertThatThrownBy(() -> transfers.transfer(
                new TransferCommand(UUID.randomUUID(), alice, bob, 0L, INR)))
                .isInstanceOf(InvalidTransferException.class);
        assertThatThrownBy(() -> transfers.transfer(
                new TransferCommand(UUID.randomUUID(), alice, bob, -100L, INR)))
                .isInstanceOf(InvalidTransferException.class);

        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("an unknown account is reported, not silently ignored")
    void unknownAccountIsRejected() {
        UUID alice = ledger.seedAccount(1_000_00L);

        assertThatThrownBy(() -> transfers.transfer(
                new TransferCommand(UUID.randomUUID(), alice, UUID.randomUUID(), 100_00L, INR)))
                .isInstanceOf(AccountNotFoundException.class);

        assertThat(ledger.balanceOf(alice)).isEqualTo(1_000_00L);
        LedgerInvariants.assertAll(jdbc);
    }
}
