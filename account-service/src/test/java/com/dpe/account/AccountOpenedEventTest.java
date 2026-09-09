package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.domain.Account;
import com.dpe.account.service.AccountService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.events.AccountOpened;
import com.dpe.events.Topics;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Opening an account publishes who owns it, in the same transaction as the account row.
 *
 * <p>The atomicity is the whole point, and the failure it prevents is quiet: an account that
 * exists here but whose ownership never reached the orchestrator is an account nobody can spend
 * from. Every transfer out of it would be refused by the edge check as an unknown account, and no
 * retry would help, because the event is not coming. Save-then-publish as two operations is the
 * dual-write bug wearing an authorization costume.
 */
class AccountOpenedEventTest extends AbstractPostgresIT {

    @Autowired
    AccountService accounts;

    @Test
    @DisplayName("opening an account writes an AccountOpened row to the outbox, keyed by account")
    void publishesOwnership() {
        Account account = accounts.open("alice", "INR", 100_000L);

        Map<String, Object> row = jdbc.queryForMap("""
                SELECT topic, event_type, aggregate_type, aggregate_id,
                       payload->'payload'->>'ownerId'     AS owner_id,
                       payload->'payload'->>'accountType' AS account_type
                  FROM outbox
                 WHERE event_type = ?
                """, AccountOpened.TYPE);

        assertThat(row.get("topic")).isEqualTo(Topics.ACCOUNT_EVENTS);
        assertThat(row.get("owner_id")).isEqualTo("alice");
        assertThat(row.get("account_type")).isEqualTo("CUSTOMER");

        assertThat(row.get("aggregate_id"))
                .as("keyed by ACCOUNT, not by transfer - a different aggregate from every other "
                        + "message on this topic, so it is ordered against other events about "
                        + "this account and against nothing else")
                .isEqualTo(account.getId());
        assertThat(row.get("aggregate_type")).isEqualTo("Account");

        // Read with ->> rather than by string-matching payload::text. Postgres reparses jsonb and
        // renders it with its own key order and spacing, so a substring assertion tests
        // Postgres's serializer, not this service's.
    }

    @Test
    @DisplayName("the funding transfer and the ownership event are one commit, not two")
    void fundingAndOwnershipCommitTogether() {
        Account account = accounts.open("alice", "INR", 100_000L);

        // Two messages, one transaction: FundsTransferred for the opening balance (from M2) and
        // AccountOpened for the ownership (M5). Neither can exist without the account row,
        // because all three are the same commit.
        Integer opened = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                Integer.class, AccountOpened.TYPE, account.getId());
        assertThat(opened).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                "SELECT balance_minor FROM accounts WHERE id = ?", Long.class, account.getId()))
                .isEqualTo(100_000L);
    }
}
