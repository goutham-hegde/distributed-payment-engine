package com.dpe.account.support;

import com.dpe.account.domain.AccountType;
import java.util.UUID;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Seeds funded accounts by writing rows directly.
 *
 * <p>Deliberately bypasses {@link com.dpe.account.service.TransferService}: a test must not seed
 * its fixture with the very code it is trying to falsify. A bug in {@code transfer()} would
 * otherwise corrupt the setup and the assertions would be measuring nothing.
 *
 * <p>The seed still writes a balanced pair of entries against the SYSTEM issuance account, so
 * invariants I1 and I2 hold before a single test line runs. A fixture that starts out violating
 * the invariants would make every later assertion meaningless.
 */
@TestComponent
public class TestLedger {

    private static final String CURRENCY = "INR";

    private final JdbcTemplate jdbc;

    public TestLedger(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public UUID seedAccount(long balanceMinor) {
        UUID accountId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO accounts (id, owner_id, account_type, currency, balance_minor)
                VALUES (?, ?, 'CUSTOMER', ?, 0)
                """, accountId, "test-owner", CURRENCY);

        if (balanceMinor > 0) {
            UUID fundingId = UUID.randomUUID();
            insertEntry(fundingId, AccountType.SYSTEM_ACCOUNT_ID, -balanceMinor, "DEBIT");
            insertEntry(fundingId, accountId, balanceMinor, "CREDIT");
            jdbc.update("UPDATE accounts SET balance_minor = balance_minor - ? WHERE id = ?",
                    balanceMinor, AccountType.SYSTEM_ACCOUNT_ID);
            jdbc.update("UPDATE accounts SET balance_minor = ? WHERE id = ?",
                    balanceMinor, accountId);
        }
        return accountId;
    }

    private void insertEntry(UUID transferId, UUID accountId, long amountMinor, String type) {
        jdbc.update("""
                INSERT INTO ledger_entries (transfer_id, account_id, amount_minor, entry_type, currency)
                VALUES (?, ?, ?, ?, ?)
                """, transferId, accountId, amountMinor, type, CURRENCY);
    }

    public long balanceOf(UUID accountId) {
        Long balance = jdbc.queryForObject(
                "SELECT balance_minor FROM accounts WHERE id = ?", Long.class, accountId);
        return balance == null ? 0L : balance;
    }

    public int entryCountFor(UUID transferId) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM ledger_entries WHERE transfer_id = ?", Integer.class, transferId);
        return count == null ? 0 : count;
    }
}
