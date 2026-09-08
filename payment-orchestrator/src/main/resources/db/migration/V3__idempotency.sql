-- M4: the idempotency gate.
--
-- THE PROBLEM
--
-- Alice taps "send 300". The response is lost on the way back - a timeout, a dropped connection,
-- a killed app. Her client retries. Two identical POSTs arrive and Bob receives 600.
--
-- The client cannot fix this. From its side a timeout is INDISTINGUISHABLE from success: the
-- request may have been lost on the way out, or the response lost on the way back, and there is
-- no way to tell which. So the client's only safe move is to retry, and the server's job is to
-- make the retry harmless.
--
-- THE MECHANISM
--
-- The client sends a key it generated once per logical intent and reuses on every retry:
--
--     POST /api/v1/transfers
--     Idempotency-Key: 3f7c...              <- same value on the retry
--     X-Client-Id: acme                     <- replaced by the JWT subject in M5
--
-- The server INSERTs that key first and asks questions afterwards. If the insert succeeds this
-- is the first time the intent has been seen, so do the work. If it violates the unique
-- constraint, someone else already has this intent, so return what they returned.
--
-- WHY THE PRIMARY KEY IS THE WHOLE DESIGN
--
-- The tempting implementation is SELECT-then-INSERT: look the key up, and if it is absent do the
-- work. That has a window between the two statements, and under a retry storm - which is exactly
-- when this table is used - two requests land in that window and both charge Alice. The check
-- must be performed by the same statement that makes the claim, holding the index lock, which is
-- what a PRIMARY KEY does and no amount of application code can.
--
-- The second thing the index buys is subtler and it is the reason this design does not need a
-- distributed lock. When a duplicate arrives while the FIRST request is still in flight, its
-- INSERT does not fail and does not proceed: Postgres BLOCKS it on the uncommitted index tuple
-- until the first transaction ends. If the first commits, the duplicate then sees the conflict
-- and returns the stored response. If the first rolls back, the duplicate takes over and does
-- the work itself. That is mutual exclusion between concurrent duplicates, with correct handoff
-- on failure, from an index - no lease, no TTL, no clock. See docs/adr/0002-idempotency.md.
--
-- SCOPE
--
-- This table gates the API edge - one HTTP request from one client. It is a different mechanism
-- from `inbox`, which gates one MESSAGE from one broker, and the two are not interchangeable:
-- the inbox dedupes on an id the SYSTEM minted and the client never sees, so it can do nothing
-- about a human pressing send twice.

CREATE TABLE idempotency_records (

    -- Who is asking. Part of the key so one tenant can neither collide with nor probe another's
    -- keys: without it, a client could send someone else's key and be handed their response
    -- body, which here would be another customer's transfer.
    --
    -- Until M5 this comes from an X-Client-Id header, which is a trust-the-caller placeholder.
    -- M5 replaces it with the JWT subject and the column stops being forgeable. The column does
    -- not change; only where the value comes from does.
    client_id            VARCHAR(64)  NOT NULL,

    -- The client's value, opaque to us. VARCHAR(255) rather than UUID because it is the CLIENT's
    -- identifier for its own intent and it is not our place to dictate its shape - Stripe allows
    -- any string up to 255 characters and so does this.
    idempotency_key      VARCHAR(255) NOT NULL,

    -- SHA-256 of the canonical request body, hex.
    --
    -- Without it, a client that reuses a key with a DIFFERENT body gets the first request's
    -- response for the second request's money - a 300 rupee transfer answered with the receipt
    -- for a 5000 rupee one. That is worse than no idempotency at all, because it is silent and
    -- the caller has been told it succeeded.
    --
    -- Stored rather than compared in memory because the two requests are usually in different
    -- processes. Hashed rather than stored whole because the body can be large and this column
    -- is only ever used for equality.
    request_fingerprint  VARCHAR(64)  NOT NULL,

    -- What the first request answered, replayed verbatim to every retry.
    --
    -- NULLABLE, and the reason is worth stating because it looks like a missing constraint. The
    -- row is inserted and then completed inside ONE transaction - the same transaction that
    -- writes the transfer, the saga and the outbox row - so a committed row always has a
    -- response, and a row without one is only ever visible to the transaction still writing it.
    --
    -- A design where the work happens OUTSIDE this transaction (a synchronous call to an
    -- external PSP, say) cannot make that claim, and needs an explicit state column -
    -- IN_PROGRESS -> COMPLETED - plus a decision about what to answer a duplicate that arrives
    -- while the original is still running. We do not need one because the work is a local
    -- commit, and adding the state anyway would be machinery that is always in the same state.
    response_status      INT,

    -- TEXT, and deliberately NOT jsonb. This is the response we promise to hand back verbatim to
    -- every retry, and jsonb cannot keep that promise: Postgres parses it into a binary tree on
    -- the way in and re-renders it on the way out with its own key order and its own ": "
    -- spacing. The bytes that come back are Postgres's serialization, never Jackson's. The first
    -- caller would get Jackson's rendering and every retry a different-but-equivalent one, which
    -- is precisely the "two different answers to one request" that idempotency exists to remove.
    --
    -- Nothing is lost by storing text. This column is only ever returned, never queried into -
    -- that is what the transfer_id column below is for.
    response_body        TEXT,

    -- Denormalized out of response_body so "which transfer did this key produce" is an indexable
    -- question rather than a JSON scan. It is also the join back into `transfers` when someone
    -- is reconstructing what a client did.
    transfer_id          UUID REFERENCES transfers (id),

    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Idempotency is a promise with an expiry date, and stating it is part of the API contract.
    -- Keys are not kept forever: this table would grow without bound, and a client retrying a
    -- payment it made a year ago is not retrying, it is making a new one. 24 hours is the
    -- industry norm and comfortably exceeds any client's retry budget.
    --
    -- Written by the service from configuration rather than by a DEFAULT here, for the same
    -- reason as saga deadline_at: retention is policy, and policy that lives in a migration can
    -- only be changed by another migration.
    expires_at           TIMESTAMPTZ  NOT NULL,

    -- THE GUARANTEE. Not a UNIQUE index next to a surrogate id - the pair IS the identity of a
    -- row here, and giving it a synthetic key would allow a second row for the same pair to be
    -- inserted by any code path that forgot the constraint.
    PRIMARY KEY (client_id, idempotency_key),

    CONSTRAINT idempotency_expires_after_creation CHECK (expires_at > created_at)
);

-- The sweeper's query in M4 part 2: expired keys, oldest first. Cheap to keep, and without it
-- the cleanup job degrades into a sequential scan of every key ever issued.
CREATE INDEX idx_idempotency_expires ON idempotency_records (expires_at);

COMMENT ON TABLE idempotency_records IS
    'API-edge deduplication. The PRIMARY KEY (client_id, idempotency_key) is the guarantee: it '
    'is claimed by the same transaction that creates the transfer, so the transfer and the '
    'record of having created it commit together. Redis caches this table and never replaces it '
    '- deleting Redis makes retries slower, deleting this table makes double transfers possible.';

COMMENT ON COLUMN idempotency_records.request_fingerprint IS
    'SHA-256 hex of the canonical request. A retry that reuses a key with a different body is a '
    'client bug and must be answered 409, never with the first request''s response.';
