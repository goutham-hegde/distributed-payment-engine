-- =====================================================================================
-- M6 part 3 - the read paths the demo console needs
-- =====================================================================================
--
-- No new tables. Two indexes, and both exist for the same reason the M6 gauges were shaped the
-- way they were: a read endpoint queries the tables that are on the write path of every payment,
-- so "it returns the right answer" is only half of correct. The other half is that it cannot
-- turn into a sequential scan the day the table is large.
--
-- What is deliberately NOT here: a denormalised "transfer view" table. It would make these
-- queries trivial and it would be a fourth thing to keep in step with `transfers`,
-- `saga_instances` and `saga_steps` - a read model maintained by hand, which is the failure mode
-- the outbox exists to avoid one level up. Two indexes over the authoritative rows is the
-- cheaper trade at this size.


-- --------------------------------------------------------------------- keyset pagination
--
-- GET /api/v1/transfers is paged by KEYSET, not by OFFSET:
--
--     WHERE initiated_by = :subject AND (created_at, id) < (:cursorCreatedAt, :cursorId)
--     ORDER BY created_at DESC, id DESC
--     LIMIT :n
--
-- The index has to carry all three columns, in that order, with the two sort columns descending,
-- or the plan is an index scan followed by a sort of everything the subject has ever sent.
--
-- V5 created this index as (initiated_by, created_at DESC) - correct for the ownership lookup it
-- was added for, and one column short for this. The tiebreak column is not decoration: without
-- `id` in the ORDER BY, two transfers created in the same microsecond have no defined order, and
-- a cursor that lands between them either repeats one or skips one. Postgres will happily return
-- them in a different order on the two queries either side of the page boundary.
--
-- Dropped and recreated rather than added alongside: the new index serves every query the old one
-- did (a leading-column prefix match), so keeping both would be paying for two B-trees to answer
-- one question, on a table that is written by every payment.
DROP INDEX idx_transfers_initiated_by;

CREATE INDEX idx_transfers_initiated_by
    ON transfers (initiated_by, created_at DESC, id DESC);

COMMENT ON INDEX idx_transfers_initiated_by IS
    'Serves both the M5 ownership lookup and the M6 keyset page. The trailing id is the tiebreak '
    'that makes the page boundary deterministic when two transfers share a created_at.';


-- --------------------------------------------------------------------- the message trail
--
-- GET /api/v1/transfers/{id}/timeline stitches saga_steps to the messages that caused them, and
-- reaches the outbox by aggregate_id - the transfer id.
--
-- Until now nothing queried this table by aggregate: the relay reads it by (published_at IS NULL,
-- created_at) and nothing else ever looked. So the only index on it is the partial one, which
-- covers UNPUBLISHED rows exclusively - and the timeline is interested in precisely the rows that
-- partial index excludes, the ones already sent. Without this, a timeline lookup is a sequential
-- scan of every message this service has ever produced.
CREATE INDEX idx_outbox_aggregate
    ON outbox (aggregate_id, created_at, id);

COMMENT ON INDEX idx_outbox_aggregate IS
    'Reads the message trail for one aggregate. Distinct from idx_outbox_unpublished, which is '
    'partial and therefore holds only rows the relay has not sent yet - the complement of what a '
    'timeline wants.';
