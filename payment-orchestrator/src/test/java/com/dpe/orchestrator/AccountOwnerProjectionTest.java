package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.AccountOpened;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import com.dpe.orchestrator.readmodel.AccountOwnerHandler;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The ownership projection: the table the edge authorization check reads.
 *
 * <p>Three properties, and the second two are the ones that would go unnoticed if broken.
 *
 * <ol>
 *   <li>An AccountOpened event puts a row in {@code account_owners}.</li>
 *   <li>A redelivered message is a no-op - the inbox gate, exactly as everywhere else.</li>
 *   <li>A <b>different</b> message claiming an account already projected does NOT overwrite the
 *       owner. The inbox cannot catch that one: it dedupes on message id, and this is two
 *       distinct messages. Ownership is immutable, so the first row wins - otherwise a producer
 *       bug (or a compromised producer) could hand somebody else's account to a new owner and
 *       the edge check would authorize it.</li>
 * </ol>
 */
class AccountOwnerProjectionTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    AccountOwnerHandler handler;

    @BeforeEach
    void clearProjection() {
        jdbc.execute("TRUNCATE TABLE account_owners");
    }

    @Test
    @DisplayName("an AccountOpened event projects the owner")
    void projectsOwner() {
        UUID accountId = UUID.randomUUID();

        boolean applied = handler.handle(UUID.randomUUID(), Topics.ACCOUNT_EVENTS,
                envelope(accountId, "alice", "CUSTOMER"));

        assertThat(applied).isTrue();
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT * FROM account_owners WHERE account_id = ?", accountId);
        assertThat(row.get("owner_id")).isEqualTo("alice");
        assertThat(row.get("account_type")).isEqualTo("CUSTOMER");
    }

    @Test
    @DisplayName("a redelivery of the same message changes nothing")
    void duplicateDeliveryIsANoOp() {
        UUID accountId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();

        assertThat(handler.handle(messageId, Topics.ACCOUNT_EVENTS,
                envelope(accountId, "alice", "CUSTOMER"))).isTrue();
        assertThat(handler.handle(messageId, Topics.ACCOUNT_EVENTS,
                envelope(accountId, "alice", "CUSTOMER")))
                .as("the inbox recognises the message id it has already committed")
                .isFalse();

        assertThat(rowCount(accountId)).isEqualTo(1);
    }

    @Test
    @DisplayName("a second, different message cannot change who owns an account")
    void ownershipCannotBeOverwritten() {
        UUID accountId = UUID.randomUUID();

        handler.handle(UUID.randomUUID(), Topics.ACCOUNT_EVENTS,
                envelope(accountId, "alice", "CUSTOMER"));

        // A NEW message id, so the inbox lets it through - this is the case dedup cannot see.
        boolean applied = handler.handle(UUID.randomUUID(), Topics.ACCOUNT_EVENTS,
                envelope(accountId, "mallory", "CUSTOMER"));

        assertThat(applied)
                .as("it was not a duplicate message: it was applied and then refused by the "
                        + "ON CONFLICT DO NOTHING, which is a different fact and is logged")
                .isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT owner_id FROM account_owners WHERE account_id = ?",
                String.class, accountId))
                .as("ownership is decided once. An upsert here would let a replayed or forged "
                        + "event transfer an account to somebody else")
                .isEqualTo("alice");
    }

    private static EventEnvelope<AccountOpened> envelope(UUID accountId, String owner,
                                                         String type) {
        return new EventEnvelope<>(UUID.randomUUID(), AccountOpened.TYPE, accountId,
                Instant.now(), new AccountOpened(accountId, owner, type, INR, 100_000L));
    }

    private int rowCount(UUID accountId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM account_owners WHERE account_id = ?",
                Integer.class, accountId);
        return count == null ? 0 : count;
    }
}
