-- M5: the data authorization needs.
--
-- Two additions, and they answer two different questions:
--
--   account_owners        who is allowed to spend from this account?   (a projection)
--   transfers.initiated_by  who asked for this transfer?               (a fact we own)
--
-- The first is somebody else's data, copied here. The second is ours, and was previously
-- unrecorded - which is worth pausing on, because it means every transfer created before this
-- migration is one whose originator this service cannot name. An audit trail that starts today
-- is the honest outcome; back-filling a guess would be worse than the gap.


-- =====================================================================================
-- account_owners - a read model of who owns what
-- =====================================================================================
--
-- THE PROBLEM THIS SOLVES
--
-- POST /api/v1/transfers arrives with a bearer token and a from_account_id. Answering "may this
-- subject spend from that account" means comparing the token's `sub` with accounts.owner_id -
-- and that column lives in accounts_db, which belongs to account-service and which this service
-- must never connect to. Database-per-service is not a style preference: a second writer to
-- someone else's tables makes their migrations a cross-team negotiation and their invariants
-- unenforceable.
--
-- WHY NOT JUST ASK OVER HTTP
--
-- Because it would make accepting a transfer depend on account-service being up. The saga is
-- built so a participant can be down without the front door closing; a synchronous authorization
-- lookup on the write path hands that property straight back. The projection makes the check a
-- local read that no other service's outage can fail.
--
-- WHAT THE STALENESS CAN DO
--
-- This table is fed by AccountOpened events over Kafka and is therefore always some milliseconds
-- behind. That is safe here because of one domain fact: OWNERSHIP NEVER CHANGES. An account is
-- opened once by one owner and this system has no way to transfer ownership. So this table can
-- only ever be MISSING a row, never holding a wrong one, and a missing row denies. Stale fails
-- closed.
--
-- If ownership ever becomes mutable, that argument dies and this check becomes advisory only -
-- which is one of the reasons account-service checks again, under the lock, before it moves
-- anything.

CREATE TABLE account_owners (
    -- The account, as account-service knows it. Not generated here: this row is a copy of a fact
    -- that was decided elsewhere, and inventing an id would break the join to the only system
    -- that can confirm it.
    account_id   UUID         PRIMARY KEY,

    -- The JWT subject that owns the account. VARCHAR(64) to match accounts.owner_id and
    -- idempotency_records.client_id - all three are the same value now that the subject has
    -- replaced the X-Client-Id header, and a width mismatch between them would truncate at
    -- whichever one is narrowest.
    owner_id     VARCHAR(64)  NOT NULL,

    -- CUSTOMER / SYSTEM / CLEARING. Carried so that the internal accounts can be refused as a
    -- transfer source outright, whatever owner id they happen to hold. Without it, anybody who
    -- learned the owner string on the SYSTEM row could mint money through the ordinary API.
    account_type VARCHAR(16)  NOT NULL,

    currency     VARCHAR(3)   NOT NULL,

    -- Diagnostics: how far behind this projection was when the row landed. lag = projected_at
    -- minus the event's occurred_at, and the question it answers - "could the 403 the user just
    -- saw have been this table being late?" - is otherwise unanswerable after the fact.
    projected_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- "Which accounts does this subject own" - the query behind a future GET /accounts listing, and
-- cheap enough to add now while the table is empty.
CREATE INDEX idx_account_owners_owner ON account_owners (owner_id);

COMMENT ON TABLE account_owners IS
    'Projection of accounts.owner_id from account-service, fed by AccountOpened events. Read only '
    'by the authorization check at the API edge. Never used to decide anything about money: '
    'balances are not projected here, and this table has no opinion about whether a transfer can '
    'be afforded.';


-- =====================================================================================
-- transfers.initiated_by - who asked
-- =====================================================================================
--
-- NULLABLE, and it is worth being explicit about why rather than leaving it looking like an
-- oversight. Every transfer created from now on has an authenticated subject, so the column is
-- always populated going forward. Rows written before M5 have no such value and no way to
-- recover one - the requests that created them carried no identity at all. A NOT NULL constraint
-- would therefore mean either refusing to run this migration on any database with history, or
-- back-filling a fiction into an audit column. Both are worse than a nullable column and a
-- comment saying when it started.

ALTER TABLE transfers ADD COLUMN initiated_by VARCHAR(64);

COMMENT ON COLUMN transfers.initiated_by IS
    'The authenticated subject that created this transfer (JWT sub). NULL only for rows written '
    'before M5, when the API was unauthenticated. Also the value GET /transfers/{id} checks '
    'before answering, so one caller cannot read another caller''s transfer by guessing its id.';

-- Supports "my transfers, newest first" - the listing the M6.5 console needs, and the query the
-- read-side authorization check runs on every poll.
CREATE INDEX idx_transfers_initiated_by ON transfers (initiated_by, created_at DESC);
