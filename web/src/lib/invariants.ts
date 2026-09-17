import { query } from "./db";

/**
 * The invariants, as queries against the database rather than as assertions in the code that
 * wrote it. A service that answers every request successfully can still have lost your money;
 * these are how the system is actually judged.
 *
 * `holds` is null rather than false when a check cannot be answered yet. I4 and S1 are only
 * meaningful once the system is at rest: a saga in RESERVED is correct now and a violation ten
 * minutes from now, and a console that paints it red under load teaches its operators to ignore
 * it. Reporting "not yet" is the honest answer, and it is what the deployed system does too.
 */
export type Check = {
  id: string;
  holds: boolean | null;
  title: string;
  detail: string;
};

export type InvariantReport = {
  quiescent: boolean;
  checks: Check[];
  allHold: boolean;
};

export async function invariants(sessionId: string): Promise<InvariantReport> {
  const [agg] = await query<{
    ledger_sum: string;
    mismatched_accounts: string;
    system_issued: string;
    customer_total: string;
    active_holds: string;
    negative_customers: string;
    in_flight_sagas: string;
    unpublished: string;
    undelivered: string;
    orphan_charges: string;
    uncharged_completions: string;
    pending_under_terminal: string;
  }>(
    `WITH
      led AS (
        SELECT COALESCE(SUM(amount_minor), 0) AS s
          FROM ledger_entries WHERE session_id = $1
      ),
      per_account AS (
        SELECT a.id, a.balance_minor,
               COALESCE((SELECT SUM(e.amount_minor) FROM ledger_entries e WHERE e.account_id = a.id), 0) AS entries
          FROM accounts a WHERE a.session_id = $1
      )
     SELECT
       (SELECT s FROM led)                                                          AS ledger_sum,
       (SELECT COUNT(*) FROM per_account WHERE balance_minor <> entries)             AS mismatched_accounts,
       (SELECT COALESCE(-SUM(balance_minor), 0) FROM accounts
         WHERE session_id = $1 AND account_type = 'SYSTEM')                          AS system_issued,
       (SELECT COALESCE(SUM(balance_minor), 0) FROM accounts
         WHERE session_id = $1 AND account_type = 'CUSTOMER')                        AS customer_total,
       (SELECT COALESCE(SUM(amount_minor), 0) FROM holds
         WHERE session_id = $1 AND status = 'ACTIVE')                                AS active_holds,
       (SELECT COUNT(*) FROM accounts
         WHERE session_id = $1 AND account_type = 'CUSTOMER' AND balance_minor < 0)  AS negative_customers,
       (SELECT COUNT(*) FROM saga_instances
         WHERE session_id = $1 AND status NOT IN ('COMPLETED','COMPENSATED','FAILED')) AS in_flight_sagas,
       (SELECT COUNT(*) FROM outbox
         WHERE session_id = $1 AND published_at IS NULL)                             AS unpublished,
       (SELECT COUNT(*) FROM outbox
         WHERE session_id = $1 AND published_at IS NOT NULL AND consumed_at IS NULL) AS undelivered,
       (SELECT COUNT(*) FROM gateway_charges g
         WHERE g.session_id = $1 AND g.status = 'APPROVED'
           AND NOT EXISTS (SELECT 1 FROM transfers t
                            WHERE t.id = g.transfer_id AND t.status = 'COMPLETED'))  AS orphan_charges,
       (SELECT COUNT(*) FROM transfers t
         WHERE t.session_id = $1 AND t.status = 'COMPLETED'
           AND NOT EXISTS (SELECT 1 FROM gateway_charges g
                            WHERE g.transfer_id = t.id AND g.status = 'APPROVED'))   AS uncharged_completions,
       (SELECT COUNT(*) FROM transfers t
          JOIN saga_instances s ON s.transfer_id = t.id
         WHERE t.session_id = $1 AND t.status = 'PENDING'
           AND s.status IN ('COMPLETED','COMPENSATED','FAILED'))                     AS pending_under_terminal
    `,
    [sessionId]
  );

  const n = (v: string) => Number(v ?? 0);

  const quiescent =
    n(agg.in_flight_sagas) === 0 && n(agg.unpublished) === 0 && n(agg.undelivered) === 0;

  const conserved = n(agg.customer_total) + n(agg.active_holds);

  const checks: Check[] = [
    {
      id: "I1",
      holds: n(agg.ledger_sum) === 0,
      title: "SUM(ledger_entries) = 0",
      detail:
        n(agg.ledger_sum) === 0
          ? "No money was created or destroyed."
          : `The ledger sums to ${agg.ledger_sum}, not zero.`,
    },
    {
      id: "I2",
      holds: n(agg.mismatched_accounts) === 0,
      title: "Every balance agrees with its entries",
      detail:
        n(agg.mismatched_accounts) === 0
          ? "Each stored balance equals the sum of its own ledger entries."
          : `${agg.mismatched_accounts} account(s) disagree with their entries.`,
    },
    {
      id: "I3",
      holds: conserved === n(agg.system_issued),
      title: "Money is conserved",
      detail: `Customer balances plus money in flight is ${fmt(conserved)}; ${fmt(
        n(agg.system_issued)
      )} was ever issued.`,
    },
    {
      id: "I4",
      holds: quiescent ? n(agg.in_flight_sagas) === 0 : null,
      title: "Nothing left half-finished",
      detail: quiescent
        ? "No payment is in a non-terminal state."
        : `${agg.in_flight_sagas} payment(s) still in flight - ask again once it is at rest.`,
    },
    {
      id: "I5",
      holds: n(agg.negative_customers) === 0,
      title: "No customer is overdrawn",
      detail:
        n(agg.negative_customers) === 0
          ? "No customer account is negative."
          : `${agg.negative_customers} customer account(s) went negative.`,
    },
    {
      id: "S1",
      holds: quiescent ? n(agg.active_holds) === 0 : null,
      title: "No money stranded in flight",
      detail: quiescent
        ? "No hold is still active."
        : "Only meaningful once the system is at rest.",
    },
    {
      id: "S2",
      holds: n(agg.orphan_charges) === 0,
      title: "Every approved charge belongs to a completed payment",
      detail:
        n(agg.orphan_charges) === 0
          ? "No charge was kept for a payment that did not complete."
          : `${agg.orphan_charges} approved charge(s) with no completed payment behind them - somebody was refunded out of our own books while the card network kept the money.`,
    },
    {
      id: "S3",
      holds: n(agg.uncharged_completions) === 0,
      title: "Every completed payment has an approved charge",
      detail:
        n(agg.uncharged_completions) === 0
          ? "No payment completed without the provider agreeing."
          : `${agg.uncharged_completions} completed payment(s) with no approved charge.`,
    },
    {
      id: "S4",
      holds: n(agg.pending_under_terminal) === 0,
      title: "Nobody is told 'processing' about a finished payment",
      detail:
        n(agg.pending_under_terminal) === 0
          ? "No pending payment sits under a finished saga."
          : `${agg.pending_under_terminal} payment(s) still say pending under a finished saga.`,
    },
  ];

  return {
    quiescent,
    checks,
    allHold: checks.every((c) => c.holds !== false),
  };
}

function fmt(paise: number): string {
  const neg = paise < 0;
  const v = Math.abs(paise);
  return `${neg ? "-" : ""}₹${Math.floor(v / 100).toLocaleString("en-IN")}.${String(v % 100).padStart(2, "0")}`;
}
