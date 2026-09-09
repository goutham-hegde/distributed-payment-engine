package com.dpe.orchestrator.idempotency;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Optional;
import com.dpe.orchestrator.web.dto.TransferResponse;
import com.dpe.orchestrator.transfer.TransferService;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * THE IDEMPOTENCY GATE. Every write request enters through here.
 *
 * <h2>YOU WRITE THIS ONE (M4)</h2>
 *
 * <p>The whole method is below in prose. Nothing about it is subtle to describe and all of it is
 * subtle to get right, which is why it is the milestone.
 *
 * <h2>The shape</h2>
 *
 * <pre>
 *   execute(clientId, key, request)                       NOT transactional
 *     |
 *     +-- fingerprint = RequestFingerprint.of(request)
 *     |
 *     +-- FAST PATH: cache.lookup(clientId, key)
 *     |     hit + same fingerprint  -> replay it, done, database untouched
 *     |     hit + different one     -> 409, database untouched
 *     |     miss / Redis is down    -> carry on
 *     |
 *     +-- ANTI-STAMPEDE: cache.acquireLock(clientId, key)
 *     |     not acquired -> wait up to lockWait for the winner to publish its answer,
 *     |                     then go to the database ANYWAY. Never refuse, never wait forever.
 *     |
 *     +-- tx.execute:                                     ONE transaction
 *     |     records.claim(...)  == 0  -> duplicate: read the row, compare fingerprints,
 *     |                                  replay or 409
 *     |     records.claim(...)  == 1  -> ours: transfers.createTransfer(request)
 *     |                                        records.complete(... the serialized response ...)
 *     |
 *     +-- after commit: cache.store(...), then releaseLock(...)
 * </pre>
 *
 * <h2>The five things that make it correct</h2>
 *
 * <p><b>1. The claim and the transfer commit together.</b> {@code records.claim} and everything
 * {@code TransferService.createTransfer} writes - the transfer, the saga, the ReserveFunds outbox
 * row - are one transaction. If the key were claimed in its own transaction first, a crash in
 * between would leave a key that says "this was done" pointing at a transfer that does not exist,
 * and the client could never retry: every attempt would be answered "already handled" forever.
 * The reverse split is worse - the transfer commits, the claim does not, and the retry makes a
 * second one.
 *
 * <p><b>2. A duplicate is read AFTER the claim comes back 0, in the same transaction.</b> Not
 * before. A read-then-claim is the {@code SELECT}-then-{@code INSERT} race this table exists to
 * eliminate.
 *
 * <p><b>3. The fingerprint is checked on every replay path</b> - the Redis one and the Postgres
 * one. Two paths to the same decision means two places to forget the check, and the fast one is
 * the one under load.
 *
 * <p><b>4. The cache is written after the commit, never before.</b> Before, a rollback leaves
 * Redis asserting a transfer that does not exist, and the fast path then serves that assertion to
 * every retry - a lie the database cannot correct because nothing goes to the database any more.
 * The lock is released after the cache is written, so the loser wakes to a populated cache.
 *
 * <p><b>5. Every Redis failure is survivable.</b> Lookup fails: treat it as a miss. Lock fails:
 * proceed without it. Store fails: log and move on - the row is committed and the next retry
 * reads it from Postgres. If any Redis failure can make this method return the wrong answer or
 * throw, the cache has become load-bearing and the design has been lost.
 *
 * <h2>Two things worth being wrong about first</h2>
 *
 * <p><i>"The lock is what stops concurrent duplicates."</i> It is not, and proving that to
 * yourself is the point of the milestone. Postgres blocks the second inserter on the uncommitted
 * index tuple until the first transaction ends - see {@link IdempotencyRepository#claim}. Delete
 * the lock and 100 concurrent identical requests still produce exactly one transfer; they just
 * queue on the index instead of on Redis. The lock keeps 99 of them from opening a database
 * transaction at all, which is a throughput argument, not a correctness one.
 *
 * <p><i>"A failed request should still burn the key."</i> Tempting, and wrong for this API. A
 * validation failure rolls the transaction back, so the claim disappears with it and the key is
 * free again - which is what a client that fixes its request and retries with the same key
 * expects. Caching failures is a real design (Stripe does it) and it needs the key to be claimed
 * in a SEPARATE transaction that survives the rollback. That is a bigger machine than this
 * milestone needs; know that you chose the small one.
 *
 * <p>The design argument, written out properly: {@code docs/adr/0002-idempotency.md}.
 *
 * @see IdempotencyRepository#claim for why the database, not Redis, is the guarantee
 */
@Service
public class IdempotencyGate {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGate.class);
    private static final Duration LOCK_POLL_INTERVAL = Duration.ofMillis(25);
    private final IdempotencyRepository records;
    private final IdempotencyCache cache;
    private final TransferService transfers;
    private final TransactionTemplate tx;
    private final ObjectMapper objectMapper;
    private final IdempotencyProperties properties;

    /**
     * M6. What the gate decided, per request.
     *
     * <p>Four outcomes, each of which means something different to whoever is reading the graph.
     * {@code new} is a first request. {@code replay} is the mechanism working - a client retried
     * and got the first answer back instead of a second payment. {@code in_flight} is a duplicate
     * that arrived while the first was still running and waited for the winner. {@code conflict}
     * is a client reusing one key for a DIFFERENT body, which is a bug in the CALLER and is the
     * one an operator should chase, because nothing else in the system will report it.
     *
     * <p>The client id is not a tag. It is unbounded - one series per client, forever - and
     * "which client is misusing keys" is a question for a log line, which is where it is.
     */
    private final MeterRegistry registry;

    public IdempotencyGate(IdempotencyRepository records, IdempotencyCache cache,
                           TransferService transfers, TransactionTemplate tx,
                           ObjectMapper objectMapper, IdempotencyProperties properties,
                           MeterRegistry registry) {
        this.records = records;
        this.cache = cache;
        this.transfers = transfers;
        this.tx = tx;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.registry = registry;

        // Every outcome gets a series at zero before the first request. Registered here rather
        // than on first use because `conflict` in particular may legitimately never happen - and
        // a panel that reads "No data" for a healthy system teaches an operator to ignore it.
        for (String outcome : new String[] {"new", "replay", "in_flight", "conflict"}) {
            outcomeCounter(outcome);
        }
    }

    private void recordOutcome(String outcome) {
        outcomeCounter(outcome).increment();
    }

    private Counter outcomeCounter(String outcome) {
        return Counter.builder("dpe.idempotency.request")
                .tag("outcome", outcome)
                .description("Idempotency gate decisions, by what the gate concluded")
                .register(registry);
    }

    /**
     * Runs this intent exactly once, however many times it is asked for.
     *
     * @param clientId who is asking. Part of the key, so one client cannot read another response.
     * @param key      the client value from the {@code Idempotency-Key} header
     * @param request  the transfer being requested
     * @return the response to send, and whether it is a replay of an earlier one
     * @throws IdempotencyConflictException if the key was used before with a different request
     */
    public IdempotentOutcome execute(String clientId, String key, CreateTransferRequest request) {

        String fingerprint = RequestFingerprint.of(request);

        Optional<CachedResponse> cached = cache.lookup(clientId, key);
        if (cached.isPresent()) {
            return replayOrConflict(cached.get(), fingerprint, clientId, key, "replay");
        }
        IdempotencyCache.LockOutcome lockOutcome = cache.acquireLock(clientId, key);

if (lockOutcome instanceof IdempotencyCache.LockOutcome.HeldByAnother) {
    IdempotentOutcome fromWinner = waitForWinner(clientId, key, fingerprint);
    if (fromWinner != null) {
        return fromWinner;
    }
}

        try {
            IdempotentOutcome outcome = tx.execute(status -> {
                OffsetDateTime expiresAt = OffsetDateTime.now().plus(properties.retention());
                int claimed = records.claim(clientId, key, fingerprint, expiresAt);

                if (claimed == 0) {
                    IdempotencyRecord existing = records.findById(new IdempotencyRecord.Key(clientId, key)).orElseThrow(() -> new IllegalStateException("claim reported a duplicate but no record exists for " + clientId + "/" + key));
                    if (!existing.getRequestFingerprint().equals(fingerprint)) {
                        throw new IdempotencyConflictException("Idempotency key " + key + " for client " + clientId + " was reused with a different request");
                    }
                    return new IdempotentOutcome(existing.getResponseStatus(), existing.getResponseBody(), true);
                }

                // M5: clientId IS the JWT subject now, so it is also the value recorded as the
                // transfer's originator. One value, asserted by the issuer, doing both jobs -
                // where M4 had a caller-supplied header doing the first and nothing doing the
                // second.
                TransferResponse response = transfers.createTransfer(request, clientId);
                String body = objectMapper.writeValueAsString(response);
                records.complete(clientId, key, 202, body, response.transferId());
                return new IdempotentOutcome(202, body, false);
            });

            // Counted here rather than inside the lambda, and the placement is the point: at
            // this line the transaction has COMMITTED. An increment inside tx.execute would
            // count work that a rollback then undid - and a rollback here is not exotic, it is
            // what a duplicate losing the race on the unique index does by design. The metric
            // would read permanently high, which is worse than not having it.
            recordOutcome(outcome.replayed() ? "replay" : "new");

            cache.store(clientId, key, new CachedResponse(fingerprint, outcome.status(), outcome.bodyJson()));
            return outcome;
        } catch (IdempotencyConflictException e) {
            // Thrown out of the transaction by the fingerprint check, so it never reaches the
            // line above.
            recordOutcome("conflict");
            throw e;
        } finally {
    if (lockOutcome instanceof IdempotencyCache.LockOutcome.Acquired acquired) {
        cache.releaseLock(clientId, key, acquired.token());
    }
}
}
    
private IdempotentOutcome waitForWinner(String clientId, String key, String fingerprint) {
    long deadline = System.currentTimeMillis() + properties.lockWait().toMillis();
    while (System.currentTimeMillis() < deadline) {
        Optional<CachedResponse> cached = cache.lookup(clientId, key);
        if (cached.isPresent()) {
            return replayOrConflict(cached.get(), fingerprint, clientId, key, "in_flight");
        }
        try {
            Thread.sleep(LOCK_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
        }
    }
    return null;
}

private IdempotentOutcome replayOrConflict(
        CachedResponse cached, String fingerprint, String clientId, String key, String outcome) {
    if (!cached.fingerprint().equals(fingerprint)) {
        recordOutcome("conflict");
        throw new IdempotencyConflictException(
        "Idempotency key " + key + " for client " + clientId + " was reused with a different request");
    }
    recordOutcome(outcome);
    return new IdempotentOutcome(cached.status(), cached.bodyJson(), true);
}}
