-- M2: the consumer side of the outbox pattern.
--
-- The relay in account-service gives at-least-once delivery: if it crashes between the Kafka ack
-- and the UPDATE that marks the row published, it republishes on restart. That duplicate is not
-- a defect to be engineered away - closing the window would require exactly-once delivery across
-- a network, which is the Two Generals Problem and is impossible. So the duplicate is absorbed
-- on THIS side instead, by making the second delivery a no-op.
--
-- The combination - at-least-once delivery plus idempotent consumers - is called
-- EFFECTIVELY-ONCE PROCESSING. Never "exactly-once delivery"; that claim cannot be defended.

CREATE TABLE inbox (
    -- The producer's outbox row id, carried in the message. Not a Kafka offset, and not an id
    -- minted when the message was sent: both of those change when the relay republishes, which
    -- is exactly the case this table exists to catch, so dedup would silently never fire.
    --
    -- PRIMARY KEY, so the second delivery collides. The check is performed by Postgres, at the
    -- moment of the write, holding the relevant lock. A SELECT-then-INSERT in Java would have a
    -- window between the two statements in which a concurrent consumer thread could pass the
    -- same check, and both would proceed to apply the message.
    message_id    UUID         PRIMARY KEY,

    -- Diagnostics only. Enough to answer "what was this message and when did we first see it"
    -- from psql without reaching for the broker.
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE inbox IS
    'Idempotency ledger for consumed messages. The row and the business write it guards MUST be '
    'committed by the same transaction: recorded separately, a crash between them either loses '
    'the message permanently (marked consumed, work never done) or lets it be applied twice.';

COMMENT ON COLUMN inbox.message_id IS
    'One consumer group per service, so the message id alone is a sufficient key. A service that '
    'later runs two independent consumer groups over the same topic must widen this to '
    '(message_id, consumer_group), or the second group will find every message already "seen".';


-- A small read model, so the inbox is guarding something real.
--
-- Not part of the saga - the orchestrator's own state machine arrives in M3. This is a
-- projection of what account-service has reported, and it exists so that M2 has an observable,
-- non-idempotent side effect to protect.
CREATE TABLE transfer_projection (
    transfer_id     UUID         PRIMARY KEY,
    from_account_id UUID         NOT NULL,
    to_account_id   UUID         NOT NULL,
    amount_minor    BIGINT       NOT NULL,
    currency        VARCHAR(3)   NOT NULL,
    status          VARCHAR(16)  NOT NULL,

    -- The point of this column.
    --
    -- The handler's write is an upsert that INCREMENTS this on conflict, which makes it
    -- deliberately NOT idempotent on its own. So a duplicate delivery that gets past the inbox
    -- is visible as apply_count = 2 rather than being silently swallowed by the primary key -
    -- and a test can tell the difference between "the inbox worked" and "the PRIMARY KEY on
    -- transfer_id happened to save us". Without it, a completely broken dedup gate passes.
    --
    -- Real read models are usually idempotent by construction (last-write-wins on a full row).
    -- This one is not, on purpose, because M2 is about proving the gate works.
    apply_count     INT          NOT NULL DEFAULT 1,

    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT transfer_projection_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT transfer_projection_status_known    CHECK (status IN ('COMPLETED'))
);
