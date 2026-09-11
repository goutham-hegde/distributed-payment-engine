-- M7: the gateway learns to take a charge back - including one it has not made yet.
--
-- Chaos scenario 5 part B found the gap. The PSP stopped answering, every ChargeGateway was
-- dead-lettered, the orchestrator's sweeper timed each saga out in RESERVED and released its hold -
-- the customer got their money back. Then an operator replayed the dead letters, as the DLQ exists
-- to let them do, and the gateway charged all ten. Ten customers refunded in our ledger and charged
-- at the card network; I1-I5 green, because no invariant reads this database.
--
-- The saga's compensation had reached account-service and never reached the PSP. The fix is a
-- compensation for the charge step itself - VoidCharge - and it has to work in both orders, because
-- a timeout is decided from a clock and can overtake the step it undoes:
--
--   VoidCharge, charge APPROVED     reverse it: status APPROVED -> VOIDED
--   VoidCharge, no charge yet       INSERT a VOIDED row: a tombstone
--   ChargeGateway, row exists       the existing replay path - republish the recorded outcome,
--                                   which for VOIDED is a decline. No second charge.
--
-- The tombstone is a ROW IN THIS TABLE rather than a separate one, and that is the design decision
-- worth defending: gateway_charges.transfer_id is already UNIQUE, so a late charge and an earlier
-- void compete for the same index entry, and the loser cannot win however the two race. A separate
-- voids table would need a lock to make "check for a void, then charge" atomic; this needs nothing
-- the schema did not already enforce. (The void writes its tombstone with INSERT ... ON CONFLICT DO
-- NOTHING, which blocks on a concurrent uncommitted charge and then sees it - see
-- GatewayChargeRepository.insertTombstone for why that means the void cannot lose the race.)
--
-- The cost is that a charge row is no longer immutable: status may move APPROVED -> VOIDED, once.
-- That is still history rather than a rewrite of it - the approval happened, voided_at says when it
-- was undone, and the GatewayApproved message stays in the outbox as the record of the first fact.

ALTER TABLE gateway_charges DROP CONSTRAINT gateway_charges_status_known;
ALTER TABLE gateway_charges ADD CONSTRAINT gateway_charges_status_known
    CHECK (status IN ('APPROVED', 'DECLINED', 'FAILED', 'VOIDED'));

ALTER TABLE gateway_charges ADD COLUMN voided_at   TIMESTAMPTZ;
ALTER TABLE gateway_charges ADD COLUMN void_reason VARCHAR(32);

-- Voided exactly when stamped, the same shape as saga_completed_at_iff_terminal.
ALTER TABLE gateway_charges ADD CONSTRAINT gateway_charges_voided_at_iff_voided
    CHECK ((status = 'VOIDED') = (voided_at IS NOT NULL));

ALTER TABLE gateway_charges ADD CONSTRAINT gateway_charges_void_has_reason
    CHECK (status <> 'VOIDED' OR void_reason IS NOT NULL);
