package com.dpe.gateway.service;

import com.dpe.events.ChargeGateway;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.Topics;
import com.dpe.gateway.domain.GatewayCharge;
import com.dpe.gateway.repository.GatewayChargeRepository;
import com.dpe.gateway.sim.GatewaySimulationProperties;
import com.dpe.messaging.outbox.OutboxWriter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The simulated PSP call, and the reply it produces.
 *
 * <p>Must be called from inside {@link com.dpe.gateway.saga.GatewayCommandHandler}'s transaction:
 * the charge row and the outbox row commit together, or a charge happens that the saga never
 * hears about. That is the dual-write bug in its most expensive form - the orchestrator would
 * wait, time out, and compensate a transfer whose money had genuinely been taken.
 */
@Service
public class ChargeService {

    private static final Logger log = LoggerFactory.getLogger(ChargeService.class);

    /** Reason codes borrowed from real card rails, so the failure text reads like the real thing. */
    private static final String[] DECLINE_REASONS = {
            "do_not_honour", "insufficient_funds", "card_expired", "suspected_fraud"
    };

    private final GatewayChargeRepository charges;
    private final OutboxWriter outbox;
    private final GatewaySimulationProperties simulation;

    public ChargeService(GatewayChargeRepository charges, OutboxWriter outbox,
                         GatewaySimulationProperties simulation) {
        this.charges = charges;
        this.outbox = outbox;
        this.simulation = simulation;
    }

    /**
     * Authorizes (or refuses) a payment and writes the reply to the outbox.
     *
     * <h2>The replay path, which is the interesting half</h2>
     *
     * <p>If this transfer has already been charged, no second charge is made. The original
     * outcome is looked up and re-published instead.
     *
     * <p>That is not merely an optimisation - it is the correct answer to the hardest failure in
     * the system. If this service crashes after committing the charge but before the relay
     * publishes, the command is redelivered; the inbox will usually catch it, but if the inbox
     * row rolled back with the crash it will not. Re-publishing the original outcome means the
     * saga always learns what actually happened, and the customer is never charged twice.
     *
     * <p>A gateway that instead returned an error on replay would be technically correct and
     * operationally useless: the saga would have no answer and would compensate a transfer whose
     * money was taken.
     */
    public void charge(ChargeGateway command) {
        Optional<GatewayCharge> existing = charges.findByTransferId(command.transferId());
        if (existing.isPresent()) {
            GatewayCharge charge = existing.get();
            log.info("transfer {} already charged ({}); republishing the original outcome",
                    command.transferId(), charge.getStatus());
            publishOutcome(charge);
            return;
        }

        int latency = simulation.getLatencyMs();
        sleepFor(latency);

        // Rolled once and reused, so the two draws below cannot disagree about this attempt.
        double roll = ThreadLocalRandom.current().nextDouble();

        if (roll < simulation.getTimeoutRate()) {
            // The ambiguous failure: no answer at all. Nothing is written - no charge row, no
            // outbox row - and the exception rolls back the inbox row with it, so the command is
            // redelivered. From the orchestrator's side this is indistinguishable from the
            // gateway being dead, which is exactly what it is simulating: the saga waits, and
            // the timeout sweeper is what eventually resolves it.
            throw new GatewayTimeoutException(
                    "simulated PSP timeout for transfer " + command.transferId());
        }

        GatewayCharge charge;
        if (roll < simulation.getTimeoutRate() + simulation.getFailureRate()) {
            String reason = DECLINE_REASONS[ThreadLocalRandom.current().nextInt(DECLINE_REASONS.length)];
            charge = GatewayCharge.declined(UUID.randomUUID(), command.transferId(),
                    command.amountMinor(), command.currency(), reason, latency);
        } else {
            charge = GatewayCharge.approved(UUID.randomUUID(), command.transferId(),
                    command.amountMinor(), command.currency(), latency);
        }

        charges.save(charge);
        publishOutcome(charge);

        // A real PSP retries its webhook when our acknowledgement is slow, and the duplicate is
        // its normal behaviour rather than its failure. Publishing a second identical message -
        // with a DIFFERENT message id, since it is a genuinely separate delivery attempt - is
        // what makes the orchestrator's inbox earn its place. A duplicate that reused the
        // message id would be deduped by the relay's own bookkeeping and would prove nothing.
        if (ThreadLocalRandom.current().nextDouble() < simulation.getDuplicateCallbackRate()) {
            log.info("simulating a duplicate PSP callback for transfer {}", command.transferId());
            publishOutcome(charge);
        }
    }

    private void publishOutcome(GatewayCharge charge) {
        if (charge.isApproved()) {
            outbox.append("Transfer", charge.getTransferId(), Topics.GATEWAY_EVENTS,
                    GatewayApproved.TYPE,
                    new GatewayApproved(charge.getTransferId(), charge.getId(),
                            charge.getAmountMinor(), charge.getCurrency()));
        } else {
            outbox.append("Transfer", charge.getTransferId(), Topics.GATEWAY_EVENTS,
                    GatewayDeclined.TYPE,
                    new GatewayDeclined(charge.getTransferId(), charge.getId(),
                            charge.getDeclineReason(),
                            "the payment service provider refused this charge"));
        }
    }

    private static void sleepFor(int millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GatewayTimeoutException("interrupted while calling the PSP");
        }
    }
}
