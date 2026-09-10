-- M6 part 2: the trace context, stored as a column on the message it belongs to.
--
-- WHY A COLUMN AND NOT NOTHING
--
-- Distributed tracing normally needs no schema at all. The instrumentation reads the current
-- span from a thread local at the moment of the outbound call, writes a W3C `traceparent` header
-- onto the request, and the receiver picks it up. That works because the call happens on the
-- thread that is inside the span.
--
-- The transactional outbox breaks precisely that assumption, and breaks it on purpose. Nothing is
-- sent on the request thread. A row is written inside the business transaction, and OutboxRelay
-- turns it into a Kafka record later, on a scheduled thread, in a different transaction. By then
-- the request span is closed and the relay thread carries no context - or worse, carries its OWN,
-- in which case every message drained in one batch becomes a child of one "drain" span and a
-- single payment shatters into several unrelated traces.
--
-- So the context has to stop being ambient and become DATA: captured on the request thread where
-- it still exists, committed atomically with the message it describes, and re-established by the
-- relay as the parent of its publish span. That is the outbox pattern applied one level up -
-- anything that must survive the gap between the transaction and the send has to be in the row.
--
-- WHY TWO NAMED COLUMNS RATHER THAN A JSONB BAG
--
-- These column names commit to the W3C Trace Context format. A propagator change (to B3, say)
-- is then a migration, which is the point: a jsonb map would accept whatever keys the propagator
-- of the day emits, and the day the format changes, old rows and new rows would silently disagree
-- with nobody having to notice. `management.tracing.propagation.type: w3c` is pinned in each
-- service's application.yml for the same reason.
--
-- trace_parent  "00-<32 hex trace id>-<16 hex span id>-<2 hex flags>" - 55 characters, fixed.
--               NULL is normal and must stay legal: a message written by a scheduler, by a test,
--               or by any service booted without a tracing bridge has no span to capture.
-- trace_state   Vendor-specific key/value list. Almost always empty in a single-vendor system,
--               and carried anyway because dropping it discards other vendors' sampling decisions
--               at this hop - a bug that only ever appears once the mesh has a second vendor in
--               it. TEXT rather than a guessed length: the spec's own limit is 512 characters
--               across 32 entries, and a VARCHAR that truncates a header is worse than one that
--               does not.
--
-- Nullable, no default, no index. Nothing ever queries BY trace context - it is read only for the
-- row the relay has already claimed by primary key - and an index on a high-cardinality column
-- that no predicate mentions is pure write amplification on the busiest table in the service.

ALTER TABLE outbox
    ADD COLUMN trace_parent VARCHAR(55),
    ADD COLUMN trace_state  TEXT;

COMMENT ON COLUMN outbox.trace_parent IS
    'W3C traceparent captured by OutboxWriter on the producing thread, inside the business '
    'transaction. OutboxRelay restores it as the parent of its publish span so that one payment '
    'is one trace across the asynchronous hop. NULL means the producer had no active span.';

COMMENT ON COLUMN outbox.trace_state IS
    'W3C tracestate, carried verbatim. Usually empty; dropping it would discard other vendors '
    'sampling decisions at this hop.';
