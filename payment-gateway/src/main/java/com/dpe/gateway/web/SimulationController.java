package com.dpe.gateway.web;

import com.dpe.gateway.sim.GatewaySimulationProperties;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Turns the failure knobs at runtime. This is the chaos suite's control plane.
 *
 * <p>Deliberately unauthenticated and deliberately not behind the JWT filter M5 adds. It is a
 * test affordance on a simulated third party, not a production API - a real PSP does not offer
 * "please start declining now". If this service were ever deployed anywhere real, this
 * controller would be the first thing deleted.
 *
 * <p>It exists so M7 can force the compensation path on demand rather than waiting for a genuine
 * decline, and so the README's demo can show a saga compensating live.
 */
@RestController
@RequestMapping("/admin/simulation")
public class SimulationController {

    private static final Logger log = LoggerFactory.getLogger(SimulationController.class);

    private final GatewaySimulationProperties simulation;

    public SimulationController(GatewaySimulationProperties simulation) {
        this.simulation = simulation;
    }

    @GetMapping
    public SimulationView current() {
        return SimulationView.of(simulation);
    }

    /**
     * Updates any subset of the knobs. Absent fields are left alone, so a chaos script can raise
     * the decline rate without having to restate the latency it did not want to change.
     */
    @PostMapping
    public ResponseEntity<SimulationView> update(@RequestBody SimulationUpdate update) {
        if (update.failureRate() != null) {
            simulation.setFailureRate(update.failureRate());
        }
        if (update.latencyMs() != null) {
            simulation.setLatencyMs(update.latencyMs());
        }
        if (update.timeoutRate() != null) {
            simulation.setTimeoutRate(update.timeoutRate());
        }
        if (update.duplicateCallbackRate() != null) {
            simulation.setDuplicateCallbackRate(update.duplicateCallbackRate());
        }

        SimulationView view = SimulationView.of(simulation);
        // Logged at INFO because a chaos run's timeline is unreadable without knowing exactly
        // when the knobs moved.
        log.info("gateway simulation updated: {}", view);
        return ResponseEntity.status(HttpStatus.OK).body(view);
    }

    /** All fields optional; null means "leave this one as it is". */
    public record SimulationUpdate(
            @DecimalMin("0.0") @DecimalMax("1.0") Double failureRate,
            @Min(0) Integer latencyMs,
            @DecimalMin("0.0") @DecimalMax("1.0") Double timeoutRate,
            @DecimalMin("0.0") @DecimalMax("1.0") Double duplicateCallbackRate) {
    }

    public record SimulationView(
            double failureRate,
            int latencyMs,
            double timeoutRate,
            double duplicateCallbackRate) {

        static SimulationView of(GatewaySimulationProperties p) {
            return new SimulationView(p.getFailureRate(), p.getLatencyMs(),
                    p.getTimeoutRate(), p.getDuplicateCallbackRate());
        }
    }
}
