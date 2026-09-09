-- M4 part 2: the dead letter store.
--
-- THE PROBLEM
--
-- A consumer that cannot process a message has exactly two moves, and both are wrong on their
-- own.
--
-- It can RETRY FOREVER. The message never leaves the head of the partition, so every message
-- behind it on that partition also never gets processed. One malformed record - one field a
-- newer producer added, one truncated payload - stops a third of the traffic on a 3-partition
-- topic, indefinitely, and the symptom presents as "the service is slow".
--
-- Or it can GIVE UP AND SKIP. That is Spring Kafka's DEFAULT: DefaultErrorHandler retries ten
-- times with no delay and then logs the exception and commits the offset. The partition keeps
-- moving, and a ReserveFunds command is gone - not failed, not compensated, GONE, with nothing
-- but a stack trace in a log that rotates in three days. In a payment system that is the worse
-- of the two.
--
-- THE ANSWER
--
-- Retry a bounded number of times with backoff, then move the message OFF the partition and
-- INTO durable storage where a human can look at it, fix the cause, and replay it. The partition
-- is unblocked and nothing is lost. That storage is this table.
--
-- WHY A TABLE AND NOT JUST THE .dlt TOPIC
--
-- Both. The recoverer publishes to `<topic>.dlt` because that is what makes the failure durable
-- at the moment of failure, in the same infrastructure, without a database being available. This
-- table is a CONSUMER of that topic, and it exists because a Kafka topic answers almost none of
-- the questions an operator has: how many are pending right now, which ones have I already
-- replayed, show me every failure for transfer X. Those are queries, and a log is not a query
-- engine. Depth becomes a COUNT, replay becomes a row update, and the M6.5 console can render
-- both without a Kafka client in the browser.
--
-- WHAT THIS TABLE CANNOT SAVE
--
-- Not every poison message is reachable by a DLQ, and it is worth writing down where the
-- boundary is. A record whose failure happens INSIDE consumer.poll() - a deserializer that
-- throws, a compression codec whose native library will not load - fails below the listener,
-- with no record attached. DefaultErrorHandler refuses it ("no record information is
-- available"), no recoverer runs, and the partition spins forever. See the compression.type
-- note in application.yml: that is not hypothetical, M2 hit it. The defence there is a
-- deserializer that cannot throw (ErrorHandlingDeserializer, which turns a failure into a
-- poison-pill VALUE the listener can see) - a different mechanism at a different layer.

CREATE TABLE dead_letters (

    -- Assigned by the recorder, not by the database, for the same reason as outbox.id: the
    -- replay endpoint has to name a row in a URL.
    id                  UUID         NOT NULL PRIMARY KEY,

    -- The BUSINESS message id, copied from the header the relay set. Nullable on purpose: a
    -- record with no message id is undedupable and gets dropped by the consumer before it ever
    -- reaches a handler, which makes it precisely the kind of record that ends up here. A
    -- schema that could not represent it would lose it.
    message_id          UUID,

    -- Where it came from and where a replay must send it back to.
    original_topic      VARCHAR(128) NOT NULL,
    original_partition  INTEGER      NOT NULL,
    original_offset     BIGINT       NOT NULL,

    -- The original Kafka key - the aggregate id. A replay MUST reuse it: publishing with a
    -- different key (or none) lands the message on a different partition, where it can overtake
    -- or be overtaken by the other messages of the same saga. Replay would then fix one failure
    -- by introducing a reordering bug.
    message_key         VARCHAR(255),

    event_type          VARCHAR(64),

    -- TEXT, not jsonb, and this is not the usual "money needs verbatim bytes" argument - it is
    -- simpler than that. A message can arrive here BECAUSE it is not valid JSON. A jsonb column
    -- would reject exactly the payloads this table exists to hold, so the INSERT that records
    -- the failure would itself fail. Nullable for the same reason: a tombstone has a null value.
    payload             TEXT,

    -- Triage. The class name is what you group by; the message is what you read.
    exception_type      VARCHAR(255),
    exception_message   TEXT,

    -- How many times the container delivered it before giving up. Distinguishes "poison from
    -- the first attempt" from "retried five times and the downstream never came back", which
    -- are different incidents with different fixes.
    attempts            INTEGER      NOT NULL DEFAULT 0,

    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- NULL means pending. This column IS the queue depth.
    replayed_at         TIMESTAMPTZ,
    replay_count        INTEGER      NOT NULL DEFAULT 0,

    -- IDENTITY IS THE SOURCE COORDINATE, NOT THE MESSAGE ID.
    --
    -- The DLT consumer is at-least-once like every other consumer here, so it needs a dedup key,
    -- and message_id is the wrong one. A replayed message that fails again is genuinely a SECOND
    -- failure of the same business message and must produce a second row - keying on message_id
    -- would silently discard it and the operator would believe their fix had worked. A Kafka
    -- record's (topic, partition, offset) is unique forever and never reused, so it identifies
    -- the FAILURE rather than the message. That is the thing being recorded.
    --
    -- This is the same lesson as the inbox's "a dedup key must be unique across everything that
    -- shares the table", applied to a table whose rows are events about messages rather than
    -- the messages themselves.
    CONSTRAINT dead_letters_source_unique
        UNIQUE (original_topic, original_partition, original_offset)
);

-- The operator's query and the M6.5 depth gauge: pending letters, oldest first. Partial, so the
-- index holds only the backlog and stays small however much history accumulates - the same
-- shape as idx_outbox_unpublished and idx_saga_instances_in_flight.
CREATE INDEX idx_dead_letters_pending
    ON dead_letters (created_at)
    WHERE replayed_at IS NULL;

-- "Show me everything that went wrong for this transfer." message_key is the aggregate id.
CREATE INDEX idx_dead_letters_key ON dead_letters (message_key);
