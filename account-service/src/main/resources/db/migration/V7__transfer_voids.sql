-- M7: compensation that commutes with the step it compensates.
--
-- The chaos suite found the gap this closes, three times by three different routes (a dead
-- participant, a deaf orchestrator, a stalled consumer): the orchestrator's timeout gives up on a
-- saga in STARTED and marks it FAILED, but it cannot un-send the ReserveFunds that is still in its
-- outbox, in the topic, or in this service's dead letter table. When that command lands, this
-- service obeys it, the reply reaches a terminal saga and is skipped, and the customer - told
-- "failed" - has their money parked in CLEARING forever. I1-I5 stay green throughout, because I3
-- counts held money as conserved.
--
-- The root cause is ordering. Kafka orders one partition; a timeout is not a message on that
-- partition, it is a decision made from a clock. So the compensation can be decided BEFORE the
-- forward step has even arrived here, and a compensation is only safe if applying it first and the
-- forward step second gives the same result as the other way round. That requires remembering the
-- compensation when there is nothing yet to compensate. This table is that memory: a TOMBSTONE.
--
--   ReleaseFunds, hold exists     release it, as before
--   ReleaseFunds, no hold yet     INSERT a void here; reply "nothing reserved, nothing will be"
--   ReserveFunds, void exists     refuse it (ReserveRejected TRANSFER_VOIDED); no money moves
--
-- Keyed by TRANSFER id because a saga that timed out in STARTED never learned a hold id; the
-- transfer id is the only name both sides have for the thing being compensated.

CREATE TABLE transfer_voids (
    -- Owned by payment-orchestrator; not a foreign key across a service boundary.
    transfer_id  UUID         PRIMARY KEY,
    reason       VARCHAR(32)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE transfer_voids IS
    'Tombstones: transfers whose saga gave up before any money was reserved for them. A ReserveFunds '
    'for a transfer listed here is refused. Never deleted while a command for the transfer could '
    'still be delivered - the same retention rule as the inbox, for the same reason.';


-- Fix B: a participant always answers, including to a command it will not act on. A ReleaseFunds or
-- CommitFunds that finds the hold already settled now replies with what actually happened to it -
-- and a FundsReleased carries a reason, which until now lived only on the message. Recorded on the
-- hold so a repeated question gets the original answer rather than whichever reason the repeat
-- happened to quote.
ALTER TABLE holds ADD COLUMN release_reason VARCHAR(32);

ALTER TABLE holds ADD CONSTRAINT holds_release_reason_only_when_released
    CHECK (release_reason IS NULL OR status = 'RELEASED');
