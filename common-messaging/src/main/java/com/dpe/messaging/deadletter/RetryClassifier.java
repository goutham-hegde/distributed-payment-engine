package com.dpe.messaging.deadletter;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import tools.jackson.core.JacksonException;
import org.springframework.stereotype.Component;

/**
 * Decides whether a failed delivery is worth trying again.
 *
 * <p>This is the single most consequential judgement in the milestone, and it is a judgement
 * about the FUTURE: retrying is only ever right when the same input might produce a different
 * outcome later. Everything else follows from that one sentence.
 *
 * <h2>The two kinds of failure</h2>
 *
 * <p><b>Transient.</b> The database was failing over. The connection pool was exhausted. Two
 * transactions deadlocked and Postgres shot one. A downstream service was mid-restart. Nothing
 * about the message is wrong; it arrived at a bad moment. Retrying is not just acceptable, it is
 * the only thing that will fix it - and backing off first is what gives the dependency room to
 * recover instead of piling on.
 *
 * <p><b>Poison.</b> The payload is not valid JSON. A UUID field contains "null". A required
 * field was renamed by a producer deployed two hours ago. There is no handler for this command
 * type. The message is deterministically unprocessable, so every retry is a guaranteed failure
 * that costs a partition stall and a stack trace. Retrying poison is not caution, it is an
 * outage with extra steps: the message stays at the head of its partition and everything behind
 * it waits.
 *
 * <p>Note what is NOT on either list. A BUSINESS rejection - insufficient funds, an account that
 * does not exist - should never reach this class at all, because a business failure commits a
 * rejection to the outbox and tells the saga. If one shows up here, the bug is upstream: someone
 * threw where they should have committed, and no retry policy can rescue it, because the answer
 * will be the same forever. See the project rule "a business failure must COMMIT, a technical
 * failure must THROW".
 *
 * <h2>The trap: the exception you are handed is not the exception that happened</h2>
 *
 * <p>Spring wraps whatever the listener threw. What arrives here is typically a
 * {@code ListenerExecutionFailedException}, and inside it may be a
 * {@code TransactionSystemException}, and inside THAT the {@code DataIntegrityViolationException}
 * you actually care about. A classifier that looks only at the top of the chain sees the same
 * wrapper class for every failure in the system and therefore cannot classify anything - and it
 * fails in the safe-looking direction, because the wrapper matches no rule and falls to the
 * default. Walk the chain.
 *
 * <p>Watch for a self-referential cause while you walk it. {@code getCause()} returning
 * {@code this} is legal and does happen; a naive loop over it never terminates, inside a Kafka
 * listener, holding a partition.
 *
 * <h2>Which way should the default go?</h2>
 *
 * <p>An unrecognised exception is either retried a few times and then dead-lettered, or
 * dead-lettered immediately. Both are defensible and you should be able to argue yours. Consider
 * which mistake you would rather make on an exception type you have never seen before, and
 * remember that the retry budget is bounded either way - a wrong "retryable" costs
 * {@code maxAttempts} deliveries and some backoff; a wrong "poison" costs an operator a replay.
 *
 * <h2>Your job</h2>
 *
 * <p>Implement {@link #isRetryable(Throwable)}. {@code KafkaErrorHandlingConfig} calls it to
 * choose between the exponential backoff and no retry at all, so returning {@code false} sends
 * the record to the dead letter topic on its first failure.
 *
 * <p>Useful Spring types, all under {@code org.springframework.dao}:
 * {@code TransientDataAccessException} (the parent of deadlock, lock-acquisition and query-timeout
 * failures), {@code RecoverableDataAccessException}, {@code DataAccessResourceFailureException},
 * {@code QueryTimeoutException} - against {@code DataIntegrityViolationException}, which is a
 * constraint the data violates and will violate again forever. Outside that package,
 * {@code org.apache.kafka.common.errors.RetriableException} marks the broker errors Kafka itself
 * considers worth another go, and {@code tools.jackson.core.JacksonException} is the deserializer
 * telling you the bytes will never parse.
 *
 * <p>{@code RetryClassifierTest} is the specification. Read it before you write anything.
 */
@Component
public class RetryClassifier {

    /**
     * @param failure whatever the listener container caught, wrappers and all
     * @return {@code true} to retry with backoff, {@code false} to dead-letter immediately
     */
        public boolean isRetryable(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = failure;

        while (current != null && visited.add(current)) {
            if (isPoison(current)) {
                return false;
            }
            if (isTransient(current)) {
                return true;
            }
            current = current.getCause();
        }

        return false;
    }

    private boolean isTransient(Throwable t) {
        return t instanceof TransientDataAccessException
                || t instanceof RecoverableDataAccessException
                || t instanceof DataAccessResourceFailureException
                || t instanceof QueryTimeoutException
                || t instanceof RetriableException;
    }

    private boolean isPoison(Throwable t) {
        return t instanceof DataIntegrityViolationException
                || t instanceof JacksonException;
    }
}
