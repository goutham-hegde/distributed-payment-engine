-- =====================================================================================
-- M6 part 3 - ledger history as a paged read
-- =====================================================================================
--
-- One index. GET /accounts/{id}/ledger is the only new query shape in this database:
--
--     WHERE account_id = :id AND id < :cursor
--     ORDER BY id DESC
--     LIMIT :n
--
-- Keyset, like the transfer list in payments_db, and simpler than it: ledger_entries.id is a
-- BIGSERIAL, so it is already a total order over the rows of an account and needs no tiebreak
-- column. Newest first, because that is the question - "what just happened to this account" -
-- and because it means the common page is the leading edge of the index.
--
-- ledger_entries is APPEND-ONLY (V1__ledger_core.sql), which is what makes keyset pagination
-- perfectly stable here rather than merely better: rows are never updated and never deleted, so
-- a cursor can never point at a row that has moved or vanished. On a mutable table keyset
-- pagination still beats OFFSET, but a page can miss a row that was updated out from under it;
-- here it cannot.


-- V1 indexed (account_id) alone, for "read this account's entries", and that is a prefix of this
-- one - so the new index answers everything the old one did. Two B-trees on the hottest table in
-- the ledger to answer one question is not a trade worth making; the old one goes.
--
-- The DESC matters. Postgres CAN scan a B-tree backwards, so (account_id, id) would work for this
-- query too, but only while the sort direction of every column agrees. Writing the index the way
-- the ORDER BY is written keeps that true the day someone adds a second sort column.
DROP INDEX idx_ledger_entries_account_id;

CREATE INDEX idx_ledger_entries_account
    ON ledger_entries (account_id, id DESC);

COMMENT ON INDEX idx_ledger_entries_account IS
    'Serves the keyset page of one account''s ledger history, newest first, and every lookup the '
    'account_id-only index it replaced used to serve.';
