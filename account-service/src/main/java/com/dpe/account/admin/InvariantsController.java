package com.dpe.account.admin;

import com.dpe.account.repository.AccountRepository;
import com.dpe.account.repository.HoldRepository;
import com.dpe.account.repository.LedgerEntryRepository;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code GET /admin/invariants} - the four of the five that live in {@code accounts_db}.
 *
 * <p>I1, I2 and I5 are checkable here and now. I4 belongs to payment-orchestrator, which publishes
 * the same shape from its own copy of this endpoint; nothing joins the two on the server, because
 * joining them would mean one process with credentials to both databases. See that class for the
 * full argument.
 *
 * <h2>I3 is not a check, and that is the honest answer</h2>
 *
 * <p>I3 says total money is <b>conserved across a test run</b>. Conservation is a statement about
 * two instants, and this endpoint only ever sees one - it has no idea when a run started or
 * whether an account was legitimately opened and funded since. So it reports the total rather than
 * a verdict, and the caller compares. {@code scripts/verify-invariants.sh} does exactly this and
 * keeps the earlier value in a baseline file it was told to write.
 *
 * <p>Returning {@code holds: true} for I3 would have been easy, would have made the console show
 * five green lights instead of four and a number, and would have been a lie in the one place the
 * system claims to prove something. A check that cannot fail is not a check.
 *
 * <h2>The total includes active holds</h2>
 *
 * <p>Money in flight is money. A reserve debits the sender and credits CLEARING, so the customer
 * balance total drops while a saga is running; without adding active holds back in, the "constant"
 * total would dip and recover on every transfer and the invariant would be unusable under load.
 * Same reason the shell script adds them.
 *
 * <h2>Cost, and the rule that comes with it</h2>
 *
 * <p>I2 compares every account against every ledger entry. That is affordable because a human or a
 * chaos scenario calls this on demand. <b>It must never become a Prometheus gauge</b>, which would
 * run it on every scrape, forever, against the tables on the payment write path.
 */
@RestController
@RequestMapping("/admin/invariants")
public class InvariantsController {

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledger;
    private final HoldRepository holds;

    public InvariantsController(AccountRepository accounts, LedgerEntryRepository ledger,
                                HoldRepository holds) {
        this.accounts = accounts;
        this.ledger = ledger;
        this.holds = holds;
    }

    @GetMapping
    public InvariantsResponse invariants() {
        long ledgerSum = ledger.sumAllAmounts();
        long entryCount = ledger.countEntries();
        long drifted = accounts.countAccountsDriftedFromLedger();
        long accountCount = accounts.countAllAccounts();
        long negative = accounts.countCustomerAccountsWithNegativeBalance();

        long customerBalance = accounts.totalCustomerBalance();
        long activeHolds = holds.sumActiveHolds();

        List<InvariantCheck> checks = List.of(
                InvariantCheck.of("I1", "Global ledger sum is zero",
                        ledgerSum == 0,
                        "sum " + ledgerSum + " over " + entryCount + " entries"),

                InvariantCheck.of("I2", "Every account balance equals the sum of its entries",
                        drifted == 0,
                        drifted + " of " + accountCount + " accounts drifted"),

                InvariantCheck.of("I5", "No customer account holds a negative balance",
                        negative == 0,
                        negative + " customer account(s) negative"));

        return new InvariantsResponse("account-service", "accounts_db", checks,
                new Conservation(customerBalance, activeHolds, customerBalance + activeHolds));
    }

    /**
     * The I3 total, broken into its two parts.
     *
     * <p>Both halves are returned rather than only the sum, because when the sum <i>does</i> move
     * the next question is always which half moved - a change in balances with no matching change
     * in holds is money created or destroyed; a change in holds alone is a saga in flight.
     *
     * @param totalMinor {@code customerBalanceMinor + activeHoldsMinor}. This is the number that
     *                   must not change across a run
     */
    public record Conservation(
            long customerBalanceMinor,
            long activeHoldsMinor,
            long totalMinor) {
    }

    public record InvariantsResponse(
            String service,
            String database,
            List<InvariantCheck> checks,
            Conservation conservation) {
    }
}
