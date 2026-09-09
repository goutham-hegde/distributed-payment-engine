package com.dpe.orchestrator.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;


/**
 * Deletes idempotency keys that have outlived their retention window.
 *
 * <h2>Why this exists at all</h2>
 *
 * <p>Every write request to this service inserts a row into {@code idempotency_records} and
 * nothing has ever deleted one. At a thousand transfers a minute that is half a billion rows a
 * year, in a table consulted by a primary key lookup on the hot path of every payment. The index
 * still works - B-trees are patient - but the table is in the buffer cache, the backups, and
 * every {@code VACUUM}, and none of that is buying anything: a key past
 * {@link IdempotencyProperties#retention} is already dead to the gate, which compares
 * {@code expires_at} before honouring a row.
 *
 * <p>This is the unglamorous half of every stateful pattern in the system, and the half most
 * often skipped: the outbox keeps published rows forever, the inbox keeps consumed message ids
 * forever, {@code dead_letters} keeps replayed letters forever. Each needs an answer eventually,
 * and each answer is different, which is the interesting part - see below.
 *
 * <h2>Why deleting a key is safe, and deleting an inbox row is not</h2>
 *
 * <p>They look like the same table with different column names, and their retention rules are
 * opposites.
 *
 * <p>An idempotency key has a <b>published contract</b>: retention is part of the API. A client
 * retrying after 24 hours is told, by documentation, that it is making a new payment. So deleting
 * an expired key changes nothing that was promised - the row was already being ignored.
 *
 * <p>An inbox row has no such contract, because the thing that might redeliver the message is
 * Kafka, and Kafka's redelivery window is the broker's retention plus however long a consumer
 * group can lag plus however long a partition can be stalled. Delete an inbox row while the
 * broker can still redeliver that message and the dedup gate opens: the message is processed a
 * second time and money moves twice. An inbox is prunable only against the broker's retention,
 * with margin, and that number belongs to the broker rather than to us.
 *
 * <p>Worth being able to say in an interview: retention policy is not a housekeeping detail, it
 * is a statement about what can still happen.
 *
 * <h2>Where the transaction goes, which is the whole design of this class</h2>
 *
 * <p>Three shapes were on the table and two of them are wrong.
 *
 * <p><b>No transaction at all</b> does not run. A Spring Data {@code @Modifying} query needs an
 * active transaction, and without one the repository proxy throws
 * {@code TransactionRequiredException}, surfacing as
 * {@code InvalidDataAccessApiUsageException: No active transaction for update or delete query}.
 * Worth knowing on sight, because "the annotation is missing" is not what that message says.
 *
 * <p><b>{@code @Transactional} on this method</b> runs, and quietly undoes the batching. Every
 * {@code deleteExpired} would then share one transaction, so every row lock taken by the first
 * batch is still held while the thousandth runs - a single lock-holding statement against a table
 * on the path of every write request, which is precisely what
 * {@link IdempotencyRepository#deleteExpired} bounds its own {@code LIMIT} to avoid. It also makes
 * the whole sweep all-or-nothing: one failure at the end discards every deletion before it, for no
 * benefit, since these rows are independent garbage and there is nothing to be consistent about.
 *
 * <p><b>One transaction per batch</b> is what it does. Each batch commits and releases its locks
 * before the next begins, a failure costs one batch instead of all of them, and progress already
 * made is kept. That is the right granularity because the unit of work here really is a batch:
 * nothing relates one batch to another.
 *
 * <p>An injected {@link TransactionTemplate} rather than a second {@code @Transactional} method,
 * for the reason {@code IdempotencyConfig} spells out at length - a {@code @Transactional} method
 * called from another method of the same bean goes down {@code this}, never touches the proxy, and
 * gets no transaction. It would fail exactly as case one above, having looked correct.
 *
 * <h2>Why the loop is bounded</h2>
 *
 * <p>The loop stops when a batch comes back short, because that means the backlog is drained. It
 * also stops at {@link #MAX_BATCHES_PER_SWEEP} regardless, so a sweep cannot run forever under an
 * arrival rate that outpaces it - the next interval picks up where this one left off, and a
 * garbage collector that never returns is worse than one that is behind.
 */
@Component
public class IdempotencySweeper {

    private static final Logger log = LoggerFactory.getLogger(IdempotencySweeper.class);

    /**
     * Ceiling on batches per sweep. With the default batch size of 500 that is half a million
     * rows, which is only survivable because each batch is its own short transaction - the same
     * number under one transaction would be the lock-holding statement this class exists to avoid.
     */
    private static final int MAX_BATCHES_PER_SWEEP = 1000;

    private final IdempotencyRepository records;
    private final IdempotencyProperties properties;
    private final TransactionTemplate tx;

    public IdempotencySweeper(IdempotencyRepository records, IdempotencyProperties properties,
                              TransactionTemplate tx) {
        this.records = records;
        this.properties = properties;
        this.tx = tx;
    }

    /**
     * Deletes expired keys, a batch at a time, until the backlog is drained or the batch cap is
     * reached.
     *
     * @return how many rows were removed
     */
    public int sweep() {
        int limit = properties.sweepBatchSize();
        int totalDeleted = 0;
        int batches = 0;
        int deletedThisBatch;

        do {
            // One transaction per batch. The template's boundary opens and closes inside this
            // loop body, which is the entire point - see the class javadoc.
            deletedThisBatch = tx.execute(status -> records.deleteExpired(limit));
            totalDeleted += deletedThisBatch;
            batches++;
        } while (deletedThisBatch == limit && batches < MAX_BATCHES_PER_SWEEP);

        if (deletedThisBatch == limit) {
            log.info("idempotency sweep hit its {}-batch cap after removing {} rows; the "
                            + "remaining backlog is picked up on the next interval",
                    batches, totalDeleted);
        }

        return totalDeleted;
    }
}
