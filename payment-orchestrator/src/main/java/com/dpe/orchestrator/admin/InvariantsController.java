package com.dpe.orchestrator.admin;

import com.dpe.orchestrator.saga.SagaInstanceRepository;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code GET /admin/invariants} - the half of the five that this database can answer.
 *
 * <h2>Why this endpoint is per-service and not one endpoint somewhere</h2>
 *
 * <p>I1, I2, I3 and I5 are statements about {@code accounts_db}. I4 is a statement about
 * {@code payments_db}. There is no connection string that can see both, and adding one would undo
 * the single most important structural decision in the system: each service owns its data, and
 * nothing else reads it. An "invariants service" with credentials to both databases would be a
 * shared-database architecture reintroduced through the monitoring door - and it would be the
 * component with the widest access in the estate, added for a dashboard.
 *
 * <p>So the console asks both services and joins the two lists in the browser. That is the same
 * answer {@code scripts/verify-invariants.sh} already gives, one layer down: the script runs five
 * queries against two databases through the psql container, because that is what checking these
 * five things honestly requires.
 *
 * <h2>Why this is not a Prometheus gauge</h2>
 *
 * <p>Because of the budget, and the distinction is worth being precise about since M6 part 1 spent
 * a lot of effort on the opposite rule. A gauge is evaluated on <b>every scrape</b> - every ten
 * seconds, forever, by however many scrapers exist - so a gauge whose query cannot ride a partial
 * index is an outage waiting for a busy day. That rule made {@code dpe.saga.inflight} a scheduled
 * refresh over {@code idx_saga_instances_in_flight}.
 *
 * <p>This endpoint has a different budget: it is called by a human, or by a chaos scenario at the
 * end of a run. It may therefore afford a query that a gauge may not - which in account-service's
 * half is a full comparison of every account against its ledger entries. <b>The rule that follows
 * from that is the one worth writing down: nothing here may ever be turned into a gauge.</b> The
 * change would look like a one-line improvement and would put a full scan of the ledger on the
 * payment path every ten seconds.
 *
 * <p>Operator-only, by the existing {@code /admin/**} rule in {@code SecurityConfig}. It is
 * read-only and moves nothing, which is precisely what the OPERATOR role is for.
 */
@RestController
@RequestMapping("/admin/invariants")
public class InvariantsController {

    private final SagaInstanceRepository sagas;

    public InvariantsController(SagaInstanceRepository sagas) {
        this.sagas = sagas;
    }

    /**
     * I4: no saga in a non-terminal state.
     *
     * <p>Reported with {@code requiresQuiescence} set, because a non-zero answer is not by itself
     * a violation - a system under load has sagas in flight by design, and that is what
     * {@code RESERVED} means. It is a violation <i>after quiescence</i>, which only the caller can
     * know it has reached. The endpoint reports the number and says which kind of check it is; it
     * does not pretend to know whether the system is at rest.
     *
     * <p>The breakdown by state is returned alongside for the reason
     * {@link SagaInstanceRepository#countInFlightByStatus} exists: the total says something is
     * stuck, the shape says which hop is stuck. STARTED means account-service is not replying,
     * RESERVED means the gateway is not answering, COMPENSATING means the release is not landing.
     */
    @GetMapping
    public InvariantsResponse invariants() {
        long nonTerminal = sagas.countNonTerminal();

        Map<String, Long> byState = sagas.countInFlightByStatus().stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> ((Number) row[1]).longValue()));

        InvariantCheck i4 = InvariantCheck.atQuiescence("I4",
                "No saga left in a non-terminal state",
                nonTerminal == 0,
                nonTerminal + " saga(s) in flight");

        return new InvariantsResponse("payment-orchestrator", "payments_db", List.of(i4), byState);
    }

    /**
     * @param service  which service answered, so the console can label a failure with the thing
     *                 that has to be looked at rather than just the invariant that broke
     * @param database the database these checks were run against. Named explicitly because the
     *                 whole point of this endpoint being per-service is that the answer is only
     *                 ever about one database
     * @param inFlight non-terminal sagas by state; empty when nothing is in flight
     */
    public record InvariantsResponse(
            String service,
            String database,
            List<InvariantCheck> checks,
            Map<String, Long> inFlight) {
    }
}
