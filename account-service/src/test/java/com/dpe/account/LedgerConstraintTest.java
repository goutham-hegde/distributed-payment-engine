package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.account.domain.AccountType;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.TestLedger;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * Proves that the ledger's rules are enforced by PostgreSQL, not by application {@code if}
 * statements.
 *
 * <p>This distinction is the whole point. An application check reads a value, decides, then
 * writes - and another transaction can change the world in the gap. A constraint is evaluated by
 * the database at write time under the same lock as the write itself, so there is no gap. These
 * tests therefore attempt the bad writes directly over JDBC, bypassing every line of service
 * code: if they still fail, no service bug can produce them either.
 *
 * <p>These pass before {@code TransferService.transfer} is implemented - they verify the
 * scaffolding, not the logic.
 */
@Import(TestLedger.class)
class LedgerConstraintTest extends AbstractPostgresIT {

    @Autowired
    private TestLedger ledger;

    @Test
    @DisplayName("I5 is a CHECK constraint: a customer balance cannot be driven negative")
    void customerBalanceCannotGoNegative() {
        UUID account = ledger.seedAccount(1_000L);

        assertThatThrownBy(() -> jdbc.update(
                "UPDATE accounts SET balance_minor = -1 WHERE id = ?", account))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("accounts_customer_balance_non_negative");
    }

    @Test
    @DisplayName("the SYSTEM issuance account is allowed to go negative - that is money issued")
    void systemAccountMayGoNegative() {
        assertThatCode(() -> jdbc.update(
                "UPDATE accounts SET balance_minor = -500 WHERE id = ?",
                AccountType.SYSTEM_ACCOUNT_ID))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a DEBIT that increases a balance is rejected: sign and type must agree")
    void debitMustBeNegative() {
        UUID account = ledger.seedAccount(1_000L);

        assertThatThrownBy(() -> insertEntry(UUID.randomUUID(), account, +100L, "DEBIT"))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ledger_entries_sign_matches_type");
    }

    @Test
    @DisplayName("a zero-amount entry is rejected")
    void zeroAmountEntryIsRejected() {
        UUID account = ledger.seedAccount(1_000L);

        assertThatThrownBy(() -> insertEntry(UUID.randomUUID(), account, 0L, "CREDIT"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("one transfer cannot post the same leg to the same account twice")
    void duplicateLegIsRejected() {
        UUID account = ledger.seedAccount(1_000L);
        UUID transferId = UUID.randomUUID();
        insertEntry(transferId, account, 100L, "CREDIT");

        assertThatThrownBy(() -> insertEntry(transferId, account, 100L, "CREDIT"))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ledger_entries_one_leg_per_account_per_transfer");
    }

    @Test
    @DisplayName("the opposite leg on the same transfer IS allowed - that is how M3 compensates")
    void oppositeLegOnSameTransferIsAllowed() {
        UUID account = ledger.seedAccount(1_000L);
        UUID transferId = UUID.randomUUID();
        insertEntry(transferId, account, 100L, "CREDIT");

        assertThatCode(() -> insertEntry(transferId, account, -100L, "DEBIT"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("there can be only one SYSTEM account")
    void systemAccountIsASingleton() {
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO accounts (id, owner_id, account_type, currency, balance_minor)
                VALUES (?, 'rogue', 'SYSTEM', 'INR', 0)
                """, UUID.randomUUID()))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("idx_accounts_single_system");
    }

    private void insertEntry(UUID transferId, UUID accountId, long amountMinor, String type) {
        jdbc.update("""
                INSERT INTO ledger_entries (transfer_id, account_id, amount_minor, entry_type, currency)
                VALUES (?, ?, ?, ?, 'INR')
                """, transferId, accountId, amountMinor, type);
    }
}
