package com.dpe.orchestrator.idempotency;

/**
 * A copy of one {@code idempotency_records} row, small enough to live in Redis.
 *
 * <p>It carries the fingerprint as well as the response, and it has to. A cache hit is not
 * permission to replay: the key may be a hit while the BODY differs, which is the 409 case, and a
 * fast path that skipped that check would turn the cache into a way of bypassing the very
 * validation the slow path performs. The fast path must reach every conclusion the database
 * would, only sooner.
 *
 * <p>Everything here is derived. Nothing in this record is knowledge Redis holds and Postgres
 * does not - which is the test of whether a cache is a cache.
 */
public record CachedResponse(String fingerprint, int status, String bodyJson) {
}
