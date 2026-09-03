package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import com.dpe.account.service.AccountService;
import com.dpe.account.service.InsufficientFundsException;
import com.dpe.account.service.TransferCommand;
import com.dpe.account.service.TransferService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Covers the path where money ENTERS the ledger.
 *
 * <p>This class exists because of a bug that fourteen passing tests did not catch. Every other
 * test seeds its accounts through {@link TestLedger}, which writes rows with raw SQL so that the
 * fixture cannot be corrupted by the code under test. That is the right call - but it meant no
 * test ever routed an issuance through {@code TransferService}, and the affordability check
 * rejected the SYSTEM account as overdrawn. Opening the first funded account was impossible, and
 * the suite was entirely green.
 *
 * <p>The lesson is worth more than the tests below: wherever a fixture takes a shortcut past the
 * production path, that is exactly where "all tests pass" stops meaning "the system works".
 * These tests deliberately take the long way round, through the real service.
 */
@Import(TestLedger.class)
class AccountIssuanceTest extends AbstractPostgresIT {

    @Autowired
    private AccountService accounts;

    @Autowired
    private TransferService transfers;

    @Autowired
    private TestLedger ledger;

    @Test
    @DisplayName("opening a funded account issues money: SYSTEM goes negative by the same amount")
    void openingAFundedAccountDrivesSystemNegative() {
        long opening = 100_000L;

        Account alice = accounts.open("alice", "INR", opening);

        assertThat(ledger.balanceOf(alice.getId())).isEqualTo(opening);
        assertThat(ledger.balanceOf(AccountType.SYSTEM_ACCOUNT_ID))
                .as("the SYSTEM balance is the total money issued, carried as a negative")
                .isEqualTo(-opening);

        // The point of routing issuance through the ordinary transfer path: it is a real posting
        // with a real counterpart, so the global sum never moves off zero.
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("issuance is not an overdraft: SYSTEM funds accounts from a zero balance")
    void systemIssuesFromZeroBalance() {
        assertThat(ledger.balanceOf(AccountType.SYSTEM_ACCOUNT_ID))
                .as("V1 seeds SYSTEM at zero")
                .isZero();

        // Before the fix this threw InsufficientFundsException, because the affordability check
        // was applied to every source account rather than mirroring the CHECK constraint, which
        // exempts SYSTEM. A source that issues money can always cover the amount by definition.
        Account alice = accounts.open("alice", "INR", 250_00L);

        assertThat(ledger.balanceOf(alice.getId())).isEqualTo(250_00L);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("issuing to several accounts leaves SYSTEM holding the exact total issued")
    void systemBalanceEqualsTotalIssued() {
        accounts.open("alice", "INR", 100_00L);
        accounts.open("bob", "INR", 250_00L);
        accounts.open("carol", "INR", 75_00L);

        assertThat(ledger.balanceOf(AccountType.SYSTEM_ACCOUNT_ID)).isEqualTo(-425_00L);
        assertThat(LedgerInvariants.totalCustomerMoney(jdbc)).isEqualTo(425_00L);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("opening with a zero balance issues nothing and writes no entries")
    void openingWithZeroBalanceWritesNoEntries() {
        Account bob = accounts.open("bob", "INR", 0L);

        assertThat(ledger.balanceOf(bob.getId())).isZero();
        assertThat(ledger.balanceOf(AccountType.SYSTEM_ACCOUNT_ID)).isZero();

        Integer entries = jdbc.queryForObject(
                "SELECT COUNT(*) FROM ledger_entries WHERE account_id = ?", Integer.class,
                bob.getId());
        assertThat(entries)
                .as("an unfunded account has no ledger history at all")
                .isZero();
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("a customer still cannot overdraw - the exemption is SYSTEM only")
    void customerAccountsAreStillConstrained() {
        Account alice = accounts.open("alice", "INR", 100_00L);
        Account bob = accounts.open("bob", "INR", 0L);

        // Guards against the obvious over-correction. Dropping the balance check entirely also
        // makes issuance work, and every other test still passes - I5 would then be enforced
        // only by the CHECK constraint, surfacing as a DataIntegrityViolationException and a
        // 409 instead of a clean 422. The exemption must be for SYSTEM specifically.
        assertThatThrownBy(() -> transfers.transfer(new TransferCommand(
                UUID.randomUUID(), alice.getId(), bob.getId(), 999_00L, "INR")))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(ledger.balanceOf(alice.getId())).isEqualTo(100_00L);
        assertThat(ledger.balanceOf(bob.getId())).isZero();
        LedgerInvariants.assertAll(jdbc);
    }
}
