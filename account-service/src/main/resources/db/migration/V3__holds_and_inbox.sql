-- M3: this service becomes a saga participant.
--
-- Two things change. It now RECEIVES commands (ReserveFunds, CommitFunds, ReleaseFunds), so it
-- needs an inbox to dedupe them. And it gains reserve-then-commit semantics, so it needs holds.
--
-- The design problem this file solves is subtler than "add a holds table", and it is worth
-- stating precisely, because the shape of the table falls out of it.


-- =====================================================================================
-- The problem: a reserve must satisfy I1, I2, I3 and I5 simultaneously
-- =====================================================================================
--
-- Recall the invariants:
--
--   I1  SUM(amount_minor) over ALL ledger_entries = 0
--   I2  accounts.balance_minor = SUM(its ledger_entries)
--   I3  SUM(CUSTOMER balances) + SUM(ACTIVE holds) is constant
--   I5  no CUSTOMER balance < 0
--
-- I3 is the binding one, and reading it carefully rules out both of the obvious designs.
--
-- ATTEMPT A - "a hold is just a row; leave the balance alone."
--   Available balance becomes (balance_minor - active holds), checked in the service.
--   Breaks I3: the customer's balance is unchanged while SUM(ACTIVE holds) grows by the
--   amount, so the total climbs by the reserved amount on every reserve. It also puts the
--   overdraft rule back into application code, where I5's CHECK constraint can no longer see
--   it - a service-layer bug could reserve money that is not there and nothing in Postgres
--   would object.
--
-- ATTEMPT B - "debit the sender now, and let the hold be the record of it."
--   Balance and hold move in opposite directions, so I3 holds. But the debit has no matching
--   credit, so SUM(ledger_entries) is no longer zero and I1 breaks on the first reserve.
--
-- Both fail because money in flight has to BE somewhere. In double-entry there is no such
-- place as "in transit" unless you make one.


-- =====================================================================================
-- The fix: a CLEARING account, which is what real payment rails actually do
-- =====================================================================================
--
-- Reserve debits the sender and credits a CLEARING account. The money has genuinely left the
-- sender and genuinely landed somewhere; it is simply somewhere that is not the recipient yet.
-- Every invariant then holds at once, with no special-casing:
--
--   reserve   DEBIT  sender   -X      CREDIT clearing  +X    hold ACTIVE X
--   commit    DEBIT  clearing -X      CREDIT recipient +X    hold COMMITTED
--   release   DEBIT  clearing -X      CREDIT sender    +X    hold RELEASED
--
--   I1  every step writes a matching pair, so the global sum never leaves zero.
--   I2  each entry is written with its balance update in the same transaction.
--   I3  CLEARING is not a CUSTOMER account, so its balance is outside I3's sum. After a
--       reserve the sender is down X and the ACTIVE hold is up X - constant. After a commit
--       the recipient is up X and the hold is no longer ACTIVE - constant. After a release
--       the sender is back up X and the hold is no longer ACTIVE - constant.
--   I5  the sender's debit is a real UPDATE, so accounts_customer_balance_non_negative still
--       rejects an overdraft. The rule stays in the database.
--
-- There is a bonus, and it is the best structural property in this milestone.
--
-- ledger_entries_one_leg_per_account_per_transfer is UNIQUE (transfer_id, account_id,
-- entry_type). Commit and release BOTH write (transfer_id, clearing, DEBIT). So for a given
-- transfer, the database physically permits one or the other and never both. A saga that tried
-- to complete and compensate the same transfer - through a bug, a redelivered command, or a
-- timeout sweeper racing a late approval - does not double-spend; the second one hits a unique
-- violation and rolls back. The mutual exclusion is a property of the schema, not a promise
-- made by the orchestrator.


-- Third account type. The CHECK from V1 named only CUSTOMER and SYSTEM, so it has to be
-- replaced rather than added to; Postgres has no "extend this constraint".
ALTER TABLE accounts DROP CONSTRAINT accounts_type_known;
ALTER TABLE accounts ADD CONSTRAINT accounts_type_known
    CHECK (account_type IN ('CUSTOMER', 'SYSTEM', 'CLEARING'));

-- 'CLEARING' is 8 characters and the column is VARCHAR(8). Deliberate, but it sits exactly at
-- the limit: a fourth type with a longer name needs an ALTER first, and the Account entity's
-- @Column(length = 8) has to move with it or ddl-auto=validate refuses to boot.

-- One clearing account, enforced the same way V1 enforces one SYSTEM account: a partial unique
-- index over a constant value, so a second insert collides. Two clearing accounts would not
-- corrupt anything, but they would make "how much money is in flight" a question with two
-- answers, and the reconciliation identity below would need to know about both.
CREATE UNIQUE INDEX idx_accounts_single_clearing
    ON accounts (account_type) WHERE account_type = 'CLEARING';

-- Starts at zero with no entries, so I1 and I2 hold the moment this migration finishes. Its
-- balance at any instant equals the total value of ACTIVE holds - that equality is a free
-- reconciliation check, and a divergence means a reserve wrote a hold without its ledger pair
-- or a ledger pair without its hold.
INSERT INTO accounts (id, owner_id, account_type, currency, balance_minor)
VALUES ('00000000-0000-0000-0000-000000000002', 'system', 'CLEARING', 'INR', 0);


CREATE TABLE holds (
    -- Assigned by the application, like the outbox id, and for the same reason: the id has to
    -- exist before the INSERT because it travels back to the orchestrator in FundsReserved and
    -- returns in CommitFunds / ReleaseFunds.
    id            UUID         PRIMARY KEY,

    -- Not a foreign key: transfers live in payments_db, owned by payment-orchestrator. The saga
    -- is what keeps the two databases agreeing, not referential integrity across a service
    -- boundary.
    transfer_id   UUID         NOT NULL,

    account_id    UUID         NOT NULL REFERENCES accounts (id),

    -- POSITIVE magnitude, unlike ledger_entries.amount_minor which is signed. A hold has no
    -- direction to encode - it is an amount set aside on one named account - and I3's query
    -- sums this column directly, so a signed value here would make that sum meaningless.
    amount_minor  BIGINT       NOT NULL,

    currency      VARCHAR(3)   NOT NULL,

    -- ACTIVE is the only state that counts toward I3. COMMITTED and RELEASED are both terminal
    -- and both mean "this money is no longer in flight"; they are kept distinct because which
    -- one a hold ended in is the difference between a completed transfer and a compensated one,
    -- and that is the first thing anyone asks when reading the audit trail.
    status        VARCHAR(9)   NOT NULL DEFAULT 'ACTIVE',

    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT holds_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT holds_status_known    CHECK (status IN ('ACTIVE', 'COMMITTED', 'RELEASED')),

    -- Idempotency for ReserveFunds, at the schema level.
    --
    -- A redelivered ReserveFunds command that gets past the inbox - or arrives before the first
    -- one has committed - cannot create a second hold on the same account for the same transfer.
    -- The inbox is the first line of defence and this is the second, and the second is the one
    -- that holds under concurrency, because it is a unique index rather than a check.
    CONSTRAINT holds_one_per_account_per_transfer UNIQUE (transfer_id, account_id)
);

-- I3 sums ACTIVE holds on every invariant check, and the timeout sweeper scans for holds whose
-- saga has stalled. Partial, for the same reason the outbox's index is: the ACTIVE set stays
-- small no matter how many holds have been settled, so both queries stay flat over time.
CREATE INDEX idx_holds_active ON holds (created_at) WHERE status = 'ACTIVE';

-- The saga addresses a hold by (transfer_id) when a command arrives, so make that lookup an
-- index seek. Already covered as the leading column of the UNIQUE constraint above.

COMMENT ON TABLE holds IS
    'Authorization records - the auth half of auth-then-capture. A hold reserves money that has '
    'already left the sender and is sitting in the CLEARING account. Committing it credits the '
    'recipient; releasing it returns the money to the sender, which is the saga compensation. '
    'Holds are never DELETEd: the row IS the audit trail of what was reserved and how it ended.';


-- =====================================================================================
-- The inbox, identical in shape to payment-orchestrator's
-- =====================================================================================
--
-- The DDL is duplicated across the three services' migrations rather than shared, and that is
-- correct: each service has its own database, so each needs its own table. The Java that drives
-- them was extracted to common-messaging at this milestone, but a shared TABLE would be a shared
-- database - the exact coupling database-per-service exists to prevent.
--
-- account-service needs one now because M3 makes it a command consumer. Until this milestone it
-- only produced events and had nothing to dedupe.

CREATE TABLE inbox (
    -- The producer's outbox row id, stable across redelivery. See payment-orchestrator's
    -- V1__inbox_and_read_model.sql for why every other candidate key is wrong.
    message_id    UUID         PRIMARY KEY,
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE inbox IS
    'Idempotency ledger for consumed messages. The row and the business write it guards MUST be '
    'committed by the same transaction, or a crash between them either loses the command '
    'permanently or applies it twice.';
