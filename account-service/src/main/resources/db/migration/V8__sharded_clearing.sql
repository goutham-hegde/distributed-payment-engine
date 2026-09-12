-- M8: CLEARING was a hot row. Shard it.
--
-- V3 made CLEARING the place money in flight lives, and that is still right. What it also made,
-- without saying so, was ONE ROW that every saga transaction in the system writes: a reserve
-- credits it, a commit or a release debits it, and each does so under SELECT ... FOR UPDATE held to
-- commit - through the ledger inserts, the hold, the outbox and inbox rows and the WAL flush. So
-- however many consumer threads account-service runs, reserves and commits execute one at a time.
-- The load test found it: once the gateway and the orchestrator were given threads, this service's
-- single consumer was the knee (~35 transfers/s, 90-99% busy), and adding threads here would only
-- have moved the queue from Kafka into a lock wait on this row.
--
-- The fix is the standard one for a hot account: split it. N CLEARING accounts, and each transfer
-- parks its money in one of them. Nothing about the accounting changes, because nothing ever needed
-- "the" clearing balance - only that money in flight is somewhere, and that it is counted:
--
--   I1  every step still writes a matching pair.
--   I2  each shard is an ordinary account whose balance is the sum of its own entries.
--   I3  sums CUSTOMER balances and ACTIVE holds; CLEARING shards are in neither, as before.
--   commit/release exclusivity: both still write (transfer_id, <the transfer's shard>, DEBIT), so
--       ledger_entries_one_leg_per_account_per_transfer still makes them mutually exclusive -
--       PROVIDED the two find the same shard. Hence the column below.
--
-- WHICH shard is decided once, at reserve, and RECORDED ON THE HOLD. Commit and release read it from
-- the hold rather than recomputing it. Recomputing (hash(transfer_id) mod N) would be correct only
-- while N and the list of shards never change: add a shard and every hold reserved before the change
-- settles against a different account than it was parked in. I1 would not notice (the legs still
-- balance); one shard would drift positive forever and another negative, and the commit/release
-- exclusivity would silently stop covering those transfers, because their two DEBIT legs could now
-- land on two different accounts. A decision that has to be the same at two moments is stored, not
-- re-derived.


-- 1. More than one CLEARING account is now the design, not a mistake.
--
-- V3's comment on this index said two clearing accounts "would make 'how much money is in flight' a
-- question with two answers". The answer is now a SUM over the shards, and it is still checkable:
-- each shard's balance equals the ACTIVE holds recorded against it (the tests assert that identity
-- per shard, which is stronger than the single-account version).
DROP INDEX idx_accounts_single_clearing;


-- 2. Seven more, for eight in total. The original keeps its id and becomes shard 0, so every hold
-- and ledger entry written before this migration still names a real clearing account.
--
-- Why eight: the service runs three consumer threads (one per partition of the command topic), so
-- two transactions meeting on one shard needs two of three concurrent transfers to hash alike - a
-- 1-in-8 event per pair instead of every time. More shards cost only rows; the count is data, not
-- configuration, and adding more later is safe precisely because holds record their shard.
--
-- Ids are readable on purpose: ...0002 is the original, ...0021-...0027 are its siblings.
INSERT INTO accounts (id, owner_id, account_type, currency, balance_minor) VALUES
    ('00000000-0000-0000-0000-000000000021', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000022', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000023', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000024', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000025', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000026', 'system', 'CLEARING', 'INR', 0),
    ('00000000-0000-0000-0000-000000000027', 'system', 'CLEARING', 'INR', 0);


-- 3. Each hold records the clearing account its money is parked in.
--
-- Every existing hold was parked in the original, so that is what they get. Backfilled, then made
-- NOT NULL, rather than given a DEFAULT: a default would quietly supply the original for any future
-- insert that forgot the column, which is exactly the recompute-instead-of-record bug above.
ALTER TABLE holds ADD COLUMN clearing_account_id UUID;
UPDATE holds SET clearing_account_id = '00000000-0000-0000-0000-000000000002';
ALTER TABLE holds ALTER COLUMN clearing_account_id SET NOT NULL;

-- ...and the database, not the service, guarantees it IS a clearing account. A plain foreign key
-- would accept any account - a hold "parked" in a customer's account would pass it, and that
-- customer's balance would then include somebody else's money in flight. So the key is composite:
-- (clearing_account_id, 'CLEARING') must exist as (id, account_type). The second column is pinned to
-- the one value by its CHECK; it exists only so the foreign key can say "and it is of this type".
-- The entity does not map it - the DEFAULT fills it, and the CHECK stops anything else being written.
ALTER TABLE accounts ADD CONSTRAINT accounts_id_type_unique UNIQUE (id, account_type);

ALTER TABLE holds ADD COLUMN clearing_account_type VARCHAR(8) NOT NULL DEFAULT 'CLEARING';
ALTER TABLE holds ADD CONSTRAINT holds_clearing_account_type_is_clearing
    CHECK (clearing_account_type = 'CLEARING');
ALTER TABLE holds ADD CONSTRAINT holds_parked_in_a_clearing_account
    FOREIGN KEY (clearing_account_id, clearing_account_type)
    REFERENCES accounts (id, account_type);

COMMENT ON COLUMN holds.clearing_account_id IS
    'The CLEARING shard this hold''s money was parked in at reserve. Commit and release debit THIS '
    'account; they never recompute it, so adding shards cannot split a transfer''s legs across two.';
