-- M3: the simulated external PSP.
--
-- This service stands in for Stripe/Razorpay/an acquiring bank. It exists for three reasons,
-- none of them "we need a fourth service":
--
--   1. It gives compensation a LEGITIMATE cause. Without an external party that can say no
--      after the money has already moved, the saga's compensation path is unreachable except by
--      injecting an artificial fault, and a code path only exercised by artificial faults is a
--      code path nobody trusts.
--
--   2. It is the one participant that CANNOT be made transactional with the ledger. Its side
--      effect is a call to somebody else's system. That is the whole reason sagas exist, and
--      modelling it explicitly keeps the design honest - two of the three services could
--      otherwise have been one database transaction.
--
--   3. It is the chaos target. Failure rate, latency and duplicate callbacks are tunable at
--      runtime (see GatewaySimulationProperties), so M7 can force a decline on demand rather
--      than waiting for one.


CREATE TABLE gateway_charges (
    -- The PSP's own reference. Assigned here, returned in GatewayApproved, and stored by the
    -- orchestrator as saga_instances.gateway_charge_id - it is the value a support ticket about
    -- a real charge would start from.
    id             UUID         PRIMARY KEY,

    -- The saga's correlation id. Not a foreign key - transfers live in payments_db, in another
    -- service's database.
    --
    -- UNIQUE, and this is the single most important line in the file.
    --
    -- Charging an external party is the one step in this system that CANNOT be undone by writing
    -- an opposite row: a duplicate charge takes real money from a real person and the fix is a
    -- refund, an apology and a chargeback window. So the protection against a redelivered
    -- ChargeGateway command has to be structural. The inbox catches the duplicate first; this
    -- constraint catches it even if the inbox is bypassed, misconfigured, or the two deliveries
    -- race inside the same instant. Two lines of defence, and the inner one is an index, because
    -- an index cannot be raced.
    --
    -- This is also the concrete answer to "compensation is not rollback": the ledger can be
    -- compensated because a reversal is just another row. THIS cannot, which is exactly why the
    -- saga is ordered to take the money before calling the gateway rather than after.
    transfer_id    UUID         NOT NULL UNIQUE,

    amount_minor   BIGINT       NOT NULL,
    currency       VARCHAR(3)   NOT NULL,

    -- APPROVED and DECLINED are both final answers from the PSP. FAILED is different in kind and
    -- the distinction matters to the orchestrator: a DECLINE is an answer ("the card was
    -- refused"), while a FAILURE is the absence of one ("the PSP timed out and we do not know
    -- whether it charged"). A decline is safe to compensate immediately. A failure is the
    -- ambiguous case that the UNIQUE constraint above makes safe to retry.
    status         VARCHAR(8)   NOT NULL,

    -- The PSP's reason code, e.g. 'insufficient_funds', 'do_not_honour'. Simulated here, but
    -- carried through to the saga so the failure reason a caller eventually sees originated
    -- with the party that actually refused.
    decline_reason VARCHAR(64),

    -- How long the simulated call took. M6 graphs it; M7 turns it up until the orchestrator's
    -- deadline fires, which is how the timeout path gets tested without killing a container.
    latency_ms     INT,

    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT gateway_charges_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT gateway_charges_status_known
        CHECK (status IN ('APPROVED', 'DECLINED', 'FAILED')),

    -- A decline without a reason is a support ticket with no first question to ask.
    CONSTRAINT gateway_charges_decline_has_reason
        CHECK (status <> 'DECLINED' OR decline_reason IS NOT NULL)
);

COMMENT ON TABLE gateway_charges IS
    'One row per charge attempt against the simulated PSP. The UNIQUE constraint on transfer_id '
    'is what makes a redelivered ChargeGateway command safe: an external charge is the one step '
    'in this system that cannot be compensated by writing an opposite row.';


-- =====================================================================================
-- outbox and inbox - same shape as the other two services, different database
-- =====================================================================================
--
-- The gateway is a full saga participant, so it needs both: an inbox to dedupe the
-- ChargeGateway commands it consumes, and an outbox so the approval or decline it produces is
-- written in the same transaction as the charge row it describes.
--
-- That last point is the whole pattern in one sentence. If this service wrote gateway_charges
-- and then published to Kafka as two operations, a crash in between would leave a charge that
-- happened and a saga that will never hear about it - waiting on a reply that no retry will
-- produce, until the timeout sweeper compensates a transfer whose money was already taken.

CREATE TABLE outbox (
    id              UUID         PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    UUID         NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,

    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

CREATE INDEX idx_outbox_unpublished
    ON outbox (created_at, id) WHERE published_at IS NULL;

CREATE TABLE inbox (
    message_id    UUID         PRIMARY KEY,
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE inbox IS
    'Idempotency ledger for consumed commands. Written in the SAME transaction as the charge it '
    'guards - see gateway_charges.transfer_id for the second line of defence behind it.';
