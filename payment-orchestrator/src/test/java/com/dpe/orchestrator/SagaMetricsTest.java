package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReserved;
import com.dpe.events.GatewayApproved;
import com.dpe.events.ReserveRejected;
import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.saga.SagaMetrics;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import com.dpe.orchestrator.saga.SagaStatus;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.transfer.Transfer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * M6. What the saga publishes about itself.
 *
 * <p>These assertions exist because a metric is the one kind of code that can be completely broken
 * and completely silent. Nothing calls it, nothing depends on its value, and a counter that is
 * never incremented looks exactly like a system that is idle. The compensation rate reading zero
 * during an outage is indistinguishable from the compensation rate reading zero because someone
 * deleted the increment.
 */
class SagaMetricsTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    SagaOrchestrator orchestrator;

    @Autowired
    SagaMetrics metrics;

    @Autowired
    MeterRegistry registry;

    @Test
    @DisplayName("a completed saga counts as started, as COMPLETED, and is timed")
    void completedSagaIsCounted() {
        double startedBefore = counter("dpe.saga.started");
        double completedBefore = terminal(SagaStatus.COMPLETED);
        long timedBefore = timerCount(SagaStatus.COMPLETED);

        runToCompletion();

        assertThat(counter("dpe.saga.started")).isEqualTo(startedBefore + 1);
        assertThat(terminal(SagaStatus.COMPLETED)).isEqualTo(completedBefore + 1);

        // Relative, not absolute. The MeterRegistry is a singleton in the shared Spring test
        // context, so every other class that ran a saga to completion in this JVM has already
        // incremented these. An absolute assertion passes when the class runs alone and fails
        // under `verify` - which is the worst way round, because it is green on the machine
        // where you wrote it.
        assertThat(timerCount(SagaStatus.COMPLETED))
                .as("the duration histogram is what makes a saga's latency visible at all - the "
                        + "HTTP timer stopped at the 202, long before this")
                .isEqualTo(timedBefore + 1);
    }

    @Test
    @DisplayName("a rejected reserve counts as FAILED, and does NOT count as a compensation")
    void failedSagaIsNotACompensation() {
        double compensatedBefore = terminal(SagaStatus.COMPENSATED);
        double failedBefore = terminal(SagaStatus.FAILED);

        Transfer transfer = newTransfer();
        orchestrator.start(transfer);
        orchestrator.onReserveRejected(
                new ReserveRejected(transfer.getId(), transfer.getFromAccountId(),
                        "INSUFFICIENT_FUNDS", "balance too low"),
                UUID.randomUUID());

        assertThat(terminal(SagaStatus.FAILED)).isEqualTo(failedBefore + 1);

        // The distinction the dashboard's headline number depends on. A rejected reserve moved no
        // money and had nothing to give back; a compensation did. Counting them together would
        // make the compensation rate rise every time a customer was short of funds, which is a
        // normal Tuesday and not a signal about the gateway at all.
        assertThat(terminal(SagaStatus.COMPENSATED))
                .as("a FAILED saga is not a compensation")
                .isEqualTo(compensatedBefore);
    }

    @Test
    @DisplayName("in-flight gauges track state, and drain back to zero")
    void inFlightGaugesFollowTheState() {
        Transfer transfer = newTransfer();
        orchestrator.start(transfer);

        metrics.refresh();
        assertThat(gauge("STARTED")).isEqualTo(1.0);
        assertThat(gauge("RESERVED")).isEqualTo(0.0);

        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());

        metrics.refresh();
        assertThat(gauge("STARTED"))
                .as("a state that has drained must be written back to zero, not left at its last "
                        + "non-zero value - otherwise the graph alarms forever about a system "
                        + "that is fine")
                .isEqualTo(0.0);
        assertThat(gauge("RESERVED")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("every non-terminal state has a series from startup, before anything has happened")
    void allNonTerminalStatesArePreRegistered() {
        for (SagaStatus status : SagaStatus.values()) {
            if (status.isTerminal()) {
                continue;
            }
            // A gauge that springs into existence only when it first goes non-zero produces a gap
            // rather than a line, and absent() in an alert then cannot tell "healthy" from "the
            // exporter died".
            assertThat(registry.find("dpe.saga.inflight").tag("state", status.name()).gauge())
                    .as("no series for non-terminal state %s", status)
                    .isNotNull();
        }
    }

    @Test
    @DisplayName("terminal states get no in-flight gauge - they are not a thing that can be stuck")
    void terminalStatesHaveNoGauge() {
        assertThat(registry.find("dpe.saga.inflight").tag("state", "COMPLETED").gauge()).isNull();
        assertThat(registry.find("dpe.saga.inflight").tag("state", "COMPENSATED").gauge()).isNull();
        assertThat(registry.find("dpe.saga.inflight").tag("state", "FAILED").gauge()).isNull();
    }

    // ------------------------------------------------------------------ helpers

    private void runToCompletion() {
        Transfer transfer = newTransfer();
        orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();

        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), holdId,
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());
        orchestrator.onGatewayApproved(
                new GatewayApproved(transfer.getId(), UUID.randomUUID(), 30_000L, INR),
                UUID.randomUUID());
        orchestrator.onFundsCommitted(
                new FundsCommitted(transfer.getId(), holdId, transfer.getToAccountId(),
                        30_000L, INR),
                UUID.randomUUID());
    }

    private Transfer newTransfer() {
        return new Transfer(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                30_000L, INR, "alice");
    }

    private double counter(String name) {
        try {
            return registry.get(name).counter().count();
        } catch (MeterNotFoundException e) {
            return 0.0;
        }
    }

    private double terminal(SagaStatus status) {
        try {
            return registry.get("dpe.saga.terminal").tag("status", status.name())
                    .counter().count();
        } catch (MeterNotFoundException e) {
            // Tagged counters are registered on first use, so "not there yet" is zero rather
            // than a failure - which is also why every assertion here is relative to a
            // before-value instead of an absolute one: the registry is shared across the class.
            return 0.0;
        }
    }

    private long timerCount(SagaStatus status) {
        try {
            return registry.get("dpe.saga.duration").tag("status", status.name())
                    .timer().count();
        } catch (MeterNotFoundException e) {
            return 0L;
        }
    }

    private double gauge(String state) {
        return registry.get("dpe.saga.inflight").tag("state", state).gauge().value();
    }
}
