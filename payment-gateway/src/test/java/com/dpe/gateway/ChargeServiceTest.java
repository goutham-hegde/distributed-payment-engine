package com.dpe.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.events.ChargeGateway;
import com.dpe.events.ChargeVoided;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.Topics;
import com.dpe.events.VoidCharge;
import com.dpe.gateway.saga.GatewayCommandHandler;
import com.dpe.gateway.service.ChargeService;
import com.dpe.gateway.service.GatewayTimeoutException;
import com.dpe.gateway.sim.GatewaySimulationProperties;
import com.dpe.gateway.support.AbstractPostgresIT;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The simulated PSP, and the two properties that make it safe to talk to.
 *
 * <p>Unlike the saga and reservation tests, this one is green already - the gateway is
 * infrastructure for the milestone rather than the thing being learned. It is here because the
 * saga tests depend on the gateway behaving exactly as described.
 */
class ChargeServiceTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    ChargeService charges;

    @Autowired
    GatewayCommandHandler handler;

    @Autowired
    GatewaySimulationProperties simulation;

    @AfterEach
    void resetSimulation() {
        // The properties bean is mutable and shared across the context, so a test that turns the
        // decline rate up would silently poison every test that ran after it.
        simulation.setFailureRate(0.0);
        simulation.setTimeoutRate(0.0);
        simulation.setDuplicateCallbackRate(0.0);
        simulation.setLatencyMs(0);
    }

    @Test
    @DisplayName("an approved charge writes the charge row and the reply in one transaction")
    void approves() {
        UUID transferId = UUID.randomUUID();

        charges.charge(new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR));

        assertThat(chargeStatus(transferId)).isEqualTo("APPROVED");
        assertThat(outboxCount(transferId, GatewayApproved.TYPE)).isEqualTo(1);
        assertThat(outboxTopic(transferId, GatewayApproved.TYPE)).isEqualTo(Topics.GATEWAY_EVENTS);
    }

    @Test
    @DisplayName("a declined charge carries the PSP's own reason code through to the saga")
    void declines() {
        simulation.setFailureRate(1.0);
        UUID transferId = UUID.randomUUID();

        charges.charge(new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR));

        assertThat(chargeStatus(transferId)).isEqualTo("DECLINED");
        assertThat(outboxCount(transferId, GatewayDeclined.TYPE)).isEqualTo(1);
        assertThat(outboxField(transferId, GatewayDeclined.TYPE, "reason"))
                .as("the explanation a caller eventually sees should originate with the party "
                        + "that actually refused")
                .isNotBlank();
    }

    @Test
    @DisplayName("a redelivered command republishes the original outcome and charges once")
    void chargingIsIdempotent() {
        UUID transferId = UUID.randomUUID();
        ChargeGateway command = new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR);

        charges.charge(command);
        String chargeId = chargeId(transferId);

        // The command arrived again - the inbox row rolled back with a crash, or the relay
        // republished. Whatever the cause, a second charge would take real money twice.
        charges.charge(command);

        assertThat(chargeCount(transferId))
                .as("AN EXTERNAL CHARGE IS THE ONE STEP THAT CANNOT BE COMPENSATED BY WRITING AN "
                        + "OPPOSITE ROW. Exactly one, always.")
                .isEqualTo(1);
        assertThat(chargeId(transferId)).isEqualTo(chargeId);
        assertThat(outboxCount(transferId, GatewayApproved.TYPE))
                .as("the outcome IS republished, rather than the replay erroring - otherwise the "
                        + "saga never learns what happened and compensates a transfer whose "
                        + "money was genuinely taken")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a simulated timeout writes nothing at all, so the command can be retried")
    void timeoutRollsEverythingBack() {
        simulation.setTimeoutRate(1.0);
        UUID transferId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();

        assertThatThrownBy(() -> handler.handle(messageId, Topics.GATEWAY_COMMANDS, ChargeGateway.TYPE,
                new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR)))
                .isInstanceOf(GatewayTimeoutException.class);

        assertThat(chargeCount(transferId)).isZero();
        assertThat(outboxCount(transferId, GatewayApproved.TYPE)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM inbox WHERE message_id = ?",
                Integer.class, messageId))
                .as("the INBOX ROW rolls back too. That is the point: it is what makes the "
                        + "command redeliverable rather than permanently marked as consumed.")
                .isZero();
    }

    @Test
    @DisplayName("the inbox stops a duplicate delivery before the charge is even attempted")
    void inboxGateStopsDuplicates() {
        UUID transferId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        ChargeGateway command = new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR);

        assertThat(handler.handle(messageId, Topics.GATEWAY_COMMANDS, ChargeGateway.TYPE, command)).isTrue();
        assertThat(handler.handle(messageId, Topics.GATEWAY_COMMANDS, ChargeGateway.TYPE, command))
                .as("a duplicate is a normal event in an at-least-once system, not an error")
                .isFalse();

        assertThat(chargeCount(transferId)).isEqualTo(1);
        assertThat(outboxCount(transferId, GatewayApproved.TYPE))
                .as("the second delivery does not even reach ChargeService, so nothing is "
                        + "republished either")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a duplicate PSP callback publishes twice - which is what the saga's inbox absorbs")
    void duplicateCallbackIsSimulated() {
        simulation.setDuplicateCallbackRate(1.0);
        UUID transferId = UUID.randomUUID();

        charges.charge(new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR));

        assertThat(chargeCount(transferId)).isEqualTo(1);
        assertThat(outboxCount(transferId, GatewayApproved.TYPE))
                .as("real PSPs retry their webhooks. Two messages with DIFFERENT ids, so the "
                        + "orchestrator's inbox has to be the thing that catches it.")
                .isEqualTo(2);
    }

    // ------------------------------------------------------------------ M7: the void

    @Test
    @DisplayName("a void reverses an approved charge, and says so")
    void voidReversesAnApprovedCharge() {
        UUID transferId = UUID.randomUUID();
        chargeIt(transferId);

        voidIt(transferId);

        assertThat(chargeStatus(transferId)).isEqualTo("VOIDED");
        assertThat(outboxField(transferId, ChargeVoided.TYPE, "outcome"))
                .isEqualTo(ChargeVoided.REVERSED);
    }

    @Test
    @DisplayName("a void that arrives BEFORE the charge makes the charge impossible - it commutes")
    void voidBeforeChargeLeavesATombstone() {
        UUID transferId = UUID.randomUUID();

        voidIt(transferId);
        // The late charge: replayed from the dead letter table after the saga compensated. This is
        // chaos scenario 5 part B, which before M7 charged ten already-refunded customers.
        chargeIt(transferId);

        assertThat(chargeCount(transferId)).isEqualTo(1);
        assertThat(chargeStatus(transferId))
                .as("the tombstone, not an approval - applying the void first and the charge "
                        + "second must end where the other order ends")
                .isEqualTo("VOIDED");
        assertThat(outboxCount(transferId, GatewayApproved.TYPE)).isZero();
        assertThat(outboxField(transferId, ChargeVoided.TYPE, "outcome"))
                .isEqualTo(ChargeVoided.PRE_EMPTED);
        assertThat(outboxField(transferId, GatewayDeclined.TYPE, "reason"))
                .as("and the late charge is still answered - a participant never goes silent")
                .isEqualTo("voided");
    }

    @Test
    @DisplayName("a void of a declined charge, or a second void, changes nothing and still answers")
    void voidIsIdempotent() {
        UUID transferId = UUID.randomUUID();
        simulation.setFailureRate(1.0);
        chargeIt(transferId);

        voidIt(transferId);
        voidIt(transferId);

        assertThat(chargeStatus(transferId)).isEqualTo("DECLINED");
        assertThat(outboxCount(transferId, ChargeVoided.TYPE)).isEqualTo(2);
    }

    // ------------------------------------------------------------------ helpers

    /** Through the handler, so each command gets the transaction a real delivery gets. */
    private void chargeIt(UUID transferId) {
        handler.handle(UUID.randomUUID(), Topics.GATEWAY_COMMANDS, ChargeGateway.TYPE,
                new ChargeGateway(transferId, UUID.randomUUID(), 30_000L, INR));
    }

    private void voidIt(UUID transferId) {
        handler.handle(UUID.randomUUID(), Topics.GATEWAY_COMMANDS, VoidCharge.TYPE,
                new VoidCharge(transferId, 30_000L, INR, "SAGA_TIMEOUT"));
    }

    private String chargeStatus(UUID transferId) {
        return jdbc.queryForObject(
                "SELECT status FROM gateway_charges WHERE transfer_id = ?", String.class, transferId);
    }

    private String chargeId(UUID transferId) {
        return jdbc.queryForObject(
                "SELECT id::text FROM gateway_charges WHERE transfer_id = ?", String.class, transferId);
    }

    private int chargeCount(UUID transferId) {
        Integer c = jdbc.queryForObject(
                "SELECT COUNT(*) FROM gateway_charges WHERE transfer_id = ?", Integer.class, transferId);
        return c == null ? 0 : c;
    }

    private int outboxCount(UUID transferId, String eventType) {
        Integer c = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                Integer.class, eventType, transferId);
        return c == null ? 0 : c;
    }

    private String outboxTopic(UUID transferId, String eventType) {
        return jdbc.queryForObject(
                "SELECT topic FROM outbox WHERE event_type = ? AND aggregate_id = ? LIMIT 1",
                String.class, eventType, transferId);
    }

    private String outboxField(UUID transferId, String eventType, String field) {
        return jdbc.queryForObject(
                "SELECT payload->'payload'->>'" + field + "' FROM outbox "
                        + "WHERE event_type = ? AND aggregate_id = ? LIMIT 1",
                String.class, eventType, transferId);
    }
}
