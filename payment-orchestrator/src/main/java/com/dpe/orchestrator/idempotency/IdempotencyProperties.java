package com.dpe.orchestrator.idempotency;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tuning for the idempotency gate. Every value here is policy, which is why none of it is a
 * database DEFAULT or a constant inside the gate.
 *
 * @param retention how long a key is honoured. Past this, a client retry is treated as a new
 *                  intent, so it must comfortably exceed any sane client retry budget. This is
 *                  part of the published API contract, not an implementation detail.
 * @param cacheTtl  how long Redis keeps a copy of a stored response. Kept at or below
 *                  {@code retention}: an entry that outlived the row it copies would answer a
 *                  request the database has already forgotten, which is Redis quietly becoming
 *                  the source of truth - the one thing it must never be.
 * @param cache     master switch for the Redis fast path. Turning it off must change nothing but
 *                  latency, and a test asserts exactly that. A system that becomes incorrect
 *                  without its cache had a load-bearing cache all along.
 * @param lockTtl   how long the anti-stampede lock is held before Redis expires it unilaterally.
 *                  Short, because it exists to keep one retry storm off the database for the
 *                  duration of one request, and every millisecond beyond that is a millisecond a
 *                  crashed holder blocks its own client. This lease is precisely what Kleppmann
 *                  critiques - which is why nothing in the design trusts it.
 * @param lockWait  how long a caller that failed to take the lock waits for the winner answer to
 *                  appear in the cache before going to Postgres anyway. Never waits forever, and
 *                  never refuses the request: the database is always allowed to arbitrate.
 * @param sweepInterval  how often expired keys are deleted. Nothing is CORRECT about this number
 *                  - a key past its retention is already dead to the gate, whether or not the
 *                  row still exists - so it is purely a bound on how much garbage the table
 *                  carries. Frequent and small beats rare and enormous: this table is on the path
 *                  of every write request, and a nightly delete of a day of keys is a long
 *                  lock-holding statement against exactly the wrong table.
 * @param sweepBatchSize  rows deleted per statement. See {@code IdempotencyRepository.deleteExpired}
 *                  for why an unbounded DELETE is the wrong shape here.
 * @param scheduled  master switch for the sweep timer. Off in tests, which call {@code sweep()}
 *                  directly so a background thread cannot race their assertions. Boxed, so that
 *                  an absent key defaults to ON rather than binding to false - a primitive
 *                  boolean would ship the sweeper silently disabled wherever the key is unset.
 */
@ConfigurationProperties(prefix = "dpe.idempotency")
public record IdempotencyProperties(
        Duration retention,
        Duration cacheTtl,
        Boolean cache,
        Duration lockTtl,
        Duration lockWait,
        Duration sweepInterval,
        int sweepBatchSize,
        Boolean scheduled) {

    public IdempotencyProperties {
        retention = retention == null ? Duration.ofHours(24) : retention;
        cacheTtl = cacheTtl == null ? Duration.ofHours(24) : cacheTtl;
        // Boxed purely so "absent" is distinguishable from "false". A primitive boolean binds to
        // false when the key is missing, which would ship the fast path silently disabled.
        cache = cache == null || cache;
        lockTtl = lockTtl == null ? Duration.ofSeconds(3) : lockTtl;
        lockWait = lockWait == null ? Duration.ofMillis(500) : lockWait;
        sweepInterval = sweepInterval == null ? Duration.ofMinutes(5) : sweepInterval;
        sweepBatchSize = sweepBatchSize <= 0 ? 500 : sweepBatchSize;
        scheduled = scheduled == null || scheduled;
    }
}
