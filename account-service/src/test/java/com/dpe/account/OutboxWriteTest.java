package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.account.service.InsufficientFundsException;
import com.dpe.account.service.TransferCommand;
import com.dpe.account.service.TransferService;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.LedgerInvariants;
import com.dpe.account.support.TestLedger;
import com.dpe.events.Topics;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * The producer half of M2: that a transfer and the message describing it commit together.
 *
 * <p>No broker here on purpose. These tests are about the transaction boundary, and the whole
 * point of the outbox is that publishing is no longer part of it - a message is a row, and a row
 * obeys the same rules as the ledger entries next to it.
 *
 * <p>The test that matters most is {@link #rejectedTransferLeavesNoOutboxRow()}. It is the one
 * that would fail if someone replaced the outbox write with a {@code kafkaTemplate.send()}, and
 * the failure it describes - the rest of the system being told about money that never moved -
 * is the reason this milestone exists.
 */
@Import(TestLedger.class)
class OutboxWriteTest extends AbstractPostgresIT {

    @Autowired
    TransferService transfers;

    @Autowired
    TestLedger ledger;

    @Test
    @DisplayName("a committed transfer leaves exactly one unpublished outbox row")
    void transferWritesOneOutboxRow() {
        UUID alice = ledger.seedAccount(100_000L);
        UUID bob = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();

        transfers.transfer(new TransferCommand(transferId, alice, bob, 30_000L, "INR"));

        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM outbox");
        assertThat(rows)
                .as("one transfer should produce exactly one message")
                .hasSize(1);
        assertThat(rows.get(0).get("published_at"))
                .as("the producer must never mark a row published; only the relay may")
                .isNull();
        assertThat(rows.get(0).get("attempts")).isEqualTo(0);
        LedgerInvariants.assertAll(jdbc);
    }

    @Test
    @DisplayName("the outbox row carries the routing and the partition key the relay needs")
    void outboxRowCarriesRouting() {
        UUID alice = ledger.seedAccount(100_000L);
        UUID bob = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();

        transfers.transfer(new TransferCommand(transferId, alice, bob, 30_000L, "INR"));

        Map<String, Object> row = jdbc.queryForMap("SELECT * FROM outbox");
        assertThat(row.get("topic")).isEqualTo(Topics.ACCOUNT_EVENTS);
        assertThat(row.get("event_type")).isEqualTo("FundsTransferred");
        assertThat(row.get("aggregate_id"))
                .as("aggregate_id becomes the Kafka key, so it must be the transfer id - "
                        + "keying by anything else forfeits per-transfer ordering")
                .hasToString(transferId.toString());
    }

    @Test
    @DisplayName("a rejected transfer leaves no outbox row at all")
    void rejectedTransferLeavesNoOutboxRow() {
        UUID alice = ledger.seedAccount(10_000L);
        UUID bob = ledger.seedAccount(0L);

        assertThatThrownBy(() -> transfers.transfer(
                new TransferCommand(UUID.randomUUID(), alice, bob, 999_999L, "INR")))
                .isInstanceOf(InsufficientFundsException.class);

        Long messages = jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Long.class);
        assertThat(messages)
                .as("the message must roll back with the transfer. If this fails, something "
                        + "published a fact about money that never moved - the dual-write bug")
                .isZero();
        LedgerInvariants.assertAll(jdbc);
    }

    /**
     * Read through {@code ->>} rather than by matching the text of {@code payload::text}.
     *
     * <p>{@code jsonb} does not store the bytes it was given: it parses them into a binary tree
     * and renders that back with its own key order and its own {@code ": "} spacing. So
     * {@code payload::text} is Postgres's serialization of the document, not Jackson's, and a
     * {@code contains("\"eventType\":\"FundsTransferred\"")} assertion fails against a perfectly
     * correct row. Asserting on extracted fields tests what the consumer will actually read.
     */
    @Test
    @DisplayName("the stored payload is the envelope a consumer will receive")
    void payloadIsAnEnvelope() {
        UUID alice = ledger.seedAccount(100_000L);
        UUID bob = ledger.seedAccount(0L);
        UUID transferId = UUID.randomUUID();

        transfers.transfer(new TransferCommand(transferId, alice, bob, 30_000L, "INR"));

        Map<String, Object> envelope = jdbc.queryForMap("""
                SELECT payload ->> 'messageId'              AS message_id,
                       payload ->> 'eventType'              AS event_type,
                       payload ->> 'aggregateId'            AS aggregate_id,
                       payload ->> 'occurredAt'             AS occurred_at,
                       payload -> 'payload' ->> 'amountMinor' AS amount_minor,
                       payload -> 'payload' ->> 'currency'    AS currency
                FROM outbox
                """);

        assertThat(envelope.get("message_id")).isNotNull();
        assertThat(envelope.get("event_type")).isEqualTo("FundsTransferred");
        assertThat(envelope.get("aggregate_id")).isEqualTo(transferId.toString());
        assertThat(envelope.get("occurred_at")).isNotNull();
        assertThat(envelope.get("amount_minor"))
                .as("the amount must survive as an integer count of minor units, never a decimal")
                .isEqualTo("30000");
        assertThat(envelope.get("currency")).isEqualTo("INR");
    }

    @Test
    @DisplayName("the message id in the payload is the outbox row id, not a second identifier")
    void messageIdMatchesRowId() {
        UUID alice = ledger.seedAccount(100_000L);
        UUID bob = ledger.seedAccount(0L);

        transfers.transfer(new TransferCommand(UUID.randomUUID(), alice, bob, 1_000L, "INR"));

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT id, payload ->> 'messageId' AS payload_message_id FROM outbox");
        assertThat(row.get("payload_message_id"))
                .as("a message id minted separately from the row id would change on every "
                        + "republish, and the consumer's inbox would never recognise a duplicate")
                .isEqualTo(row.get("id").toString());
    }
}
