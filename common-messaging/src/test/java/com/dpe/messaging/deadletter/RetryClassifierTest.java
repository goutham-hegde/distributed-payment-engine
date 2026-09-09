package com.dpe.messaging.deadletter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.TransactionSystemException;

/**
 * The specification for {@link RetryClassifier#isRetryable}.
 *
 * <p>One question, asked of a dozen exceptions: could this same message succeed later? The tests
 * fall into four groups, and each group is a different way of getting the answer wrong.
 */
class RetryClassifierTest {

    private final RetryClassifier classifier = new RetryClassifier();

    // ---- transient: the message is fine, the moment was not -----------------------------

    @Test
    @DisplayName("a deadlock victim is retried - the same statement wins next time")
    void deadlockIsRetryable() {
        assertThat(classifier.isRetryable(
                new CannotAcquireLockException("deadlock detected"))).isTrue();
    }

    @Test
    @DisplayName("an unreachable database is retried - it is coming back")
    void connectionFailureIsRetryable() {
        assertThat(classifier.isRetryable(
                new DataAccessResourceFailureException("connection refused"))).isTrue();
    }

    @Test
    @DisplayName("a query timeout is retried")
    void queryTimeoutIsRetryable() {
        assertThat(classifier.isRetryable(new QueryTimeoutException("statement timeout"))).isTrue();
    }

    // ---- poison: no number of retries changes the answer --------------------------------

    @Test
    @DisplayName("unparseable JSON is never retried - the bytes will not improve")
    void malformedPayloadIsPoison() {
        assertThat(classifier.isRetryable(new tools.jackson.core.JacksonException("boom") {
        })).isFalse();
    }

    @Test
    @DisplayName("a malformed UUID is never retried")
    void malformedIdentifierIsPoison() {
        assertThat(classifier.isRetryable(
                new IllegalArgumentException("Invalid UUID string: not-a-uuid"))).isFalse();
    }

    @Test
    @DisplayName("a command type with no handler is never retried - deploying is the fix, not waiting")
    void unknownCommandTypeIsPoison() {
        assertThat(classifier.isRetryable(
                new IllegalStateException("no handler for command type Foo"))).isFalse();
    }

    @Test
    @DisplayName("a constraint violation is never retried - the data violates it permanently")
    void constraintViolationIsPoison() {
        assertThat(classifier.isRetryable(
                new DataIntegrityViolationException("duplicate key value"))).isFalse();
    }

    // ---- the wrapper trap ---------------------------------------------------------------
    //
    // These four are the ones that catch a classifier written against the top of the chain.
    // Spring hands the error handler a ListenerExecutionFailedException every time, so a
    // classifier that inspects only what it was given sees ONE exception type for the entire
    // system and cannot distinguish anything - and it fails in whichever direction the default
    // happens to point, which looks like a working policy until the day it matters.

    @Test
    @DisplayName("a transient cause is found through the listener wrapper")
    void unwrapsListenerExecutionFailure() {
        assertThat(classifier.isRetryable(new ListenerExecutionFailedException("listener threw",
                new CannotAcquireLockException("deadlock detected")))).isTrue();
    }

    @Test
    @DisplayName("a poison cause is found through the listener wrapper")
    void unwrapsListenerExecutionFailureForPoison() {
        assertThat(classifier.isRetryable(new ListenerExecutionFailedException("listener threw",
                new IllegalArgumentException("Invalid UUID string: x")))).isFalse();
    }

    @Test
    @DisplayName("the cause is found however deep it is buried")
    void unwrapsMoreThanOneLevel() {
        Throwable buried = new ListenerExecutionFailedException("listener threw",
                new TransactionSystemException("could not commit",
                        new DataAccessResourceFailureException("connection reset")));
        assertThat(classifier.isRetryable(buried)).isTrue();
    }

    @Test
    @DisplayName("a self-referential cause chain terminates instead of hanging the partition")
    void selfReferentialCauseDoesNotLoopForever() {
        // Legal, and it happens. A naive while(cause != null) cause = cause.getCause() loop
        // spins here forever, inside a Kafka listener, holding a partition - a hang rather than
        // a failure, which is the hardest kind of bug to find from the outside.
        SelfCausingException self = new SelfCausingException();
        assertThat(classifier.isRetryable(self)).isIn(true, false);
    }

    // ---- the edges ----------------------------------------------------------------------

    @Test
    @DisplayName("null is not retryable and does not throw")
    void nullIsHandled() {
        assertThat(classifier.isRetryable(null)).isFalse();
    }

    @Test
    @DisplayName("an unrecognised exception gets a deliberate answer, not an accidental one")
    void unknownExceptionHasADocumentedDefault() {
        // This test does not tell you which way to go - both are defensible. It exists so that
        // the choice is made on purpose and written down: change the expectation to match the
        // default you chose, and say why in the javadoc of isRetryable.
        boolean expected = false;
        assertThat(classifier.isRetryable(new RuntimeException("something new")))
                .as("decide the default for an exception type you have never seen, and document it")
                .isEqualTo(expected);
    }

    private static final class SelfCausingException extends RuntimeException {

        SelfCausingException() {
            super("I am my own cause");
        }

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }
}
