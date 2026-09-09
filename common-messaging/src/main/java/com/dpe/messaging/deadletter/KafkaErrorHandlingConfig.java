package com.dpe.messaging.deadletter;

import com.dpe.events.Topics;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * What happens when a listener throws.
 *
 * <h2>The default this replaces</h2>
 *
 * <p>With no {@code CommonErrorHandler} bean, Spring Boot installs a {@link DefaultErrorHandler}
 * configured with {@code FixedBackOff(0, 9)} and a recoverer that <b>logs the exception and
 * commits the offset</b>. Read that again with money in mind: a {@code ReserveFunds} command that
 * fails ten times in a row - which, with a zero interval, is ten attempts inside a few
 * milliseconds, so a two-second database blip loses all ten - is silently dropped. The saga is
 * never told, so it sits in {@code STARTED} until the timeout sweeper compensates it, and the
 * only trace is a stack trace in a log.
 *
 * <p>Nothing about that default is wrong for a system where a lost message is an inconvenience.
 * It is catastrophic for one where a lost message is money, and it is the single most common
 * production surprise in Spring Kafka, because it is invisible until you look for it.
 *
 * <h2>What this installs instead</h2>
 *
 * <ol>
 *   <li><b>Exponential backoff between attempts</b>, so a downstream that is struggling is given
 *       room instead of being hit again immediately. Zero-interval retries are a retry storm
 *       aimed at the thing that is already failing.</li>
 *   <li><b>A bounded budget</b> - {@link DeadLetterProperties#maxAttempts} deliveries - after
 *       which the record leaves the partition. Unbounded retry is not resilience: the message
 *       stays at the head of its partition and every message behind it waits with it.</li>
 *   <li><b>A classifier</b>, so that a message which cannot possibly succeed does not spend the
 *       budget proving it. See {@link RetryClassifier}.</li>
 *   <li><b>A dead letter topic</b> rather than a log line, so the message is still a message
 *       afterwards and can be replayed.</li>
 * </ol>
 *
 * <h2>The backoff is blocking, and that constrains it</h2>
 *
 * <p>{@link DefaultErrorHandler} sleeps <i>in the listener thread</i>, between polls. So the
 * total retry budget - roughly {@code maxAttempts x maxInterval} - is time this consumer is not
 * calling {@code poll()}. Exceed {@code max.poll.interval.ms} (five minutes by default) and the
 * broker declares the consumer dead and rebalances the group; the partition is handed to another
 * instance, which begins the retry budget again from zero, and the group rebalances once more
 * when the original consumer comes back. An over-generous backoff therefore does not produce
 * patient retries, it produces a rebalance loop. Non-blocking retry (Spring Kafka's
 * {@code @RetryableTopic}, which parks the record on a delay topic instead of sleeping) is the
 * answer when the wait must be long, and it costs message ordering - which this system is not
 * willing to give up, since a saga's messages must stay ordered within their partition.
 *
 * <h2>Why the dead letter listener needs a different factory</h2>
 *
 * <p>See {@link #deadLetterListenerContainerFactory}. In short: this error handler must not be
 * applied to the listener that consumes the dead letter topic, or a failure there publishes to
 * {@code <topic>.dlt.dlt} and the chain has no end.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /**
     * Backoff for a record classified as poison: no wait, no retry, straight to the recoverer.
     *
     * <p>{@code FixedBackOff(0, 0)} means zero retries, not zero delay before infinite ones -
     * the second argument is the retry count. Off-by-one here is the difference between "never
     * retried" and "retried once".
     */
    private static final BackOff NO_RETRY = new FixedBackOff(0L, 0L);

    /**
     * The error handler Boot applies to every {@code @KafkaListener} that uses the default
     * container factory. A single {@code CommonErrorHandler} bean is enough; Boot's
     * {@code KafkaAnnotationDrivenConfiguration} finds it and calls
     * {@code setCommonErrorHandler} on the factory it builds.
     */
    @Bean
    CommonErrorHandler deadLetterErrorHandler(KafkaTemplate<String, String> kafka,
                                              RetryClassifier classifier,
                                              DeadLetterProperties properties) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> {
                    // Partition -1 hands the choice to the producer's partitioner, which keys on
                    // the record key - the aggregate id, which the recoverer preserves. Pinning
                    // the original partition number instead would require the dead letter topic
                    // to be at least as wide as the source, and would buy nothing: ordering is a
                    // per-key guarantee and the key is unchanged. The original coordinates are
                    // not lost either way; the recoverer writes them into headers.
                    String dlt = record.topic() + Topics.DLT_SUFFIX;
                    log.error("dead-lettering {}-{}@{} to {}: {}", record.topic(),
                            record.partition(), record.offset(), dlt, exception.toString());
                    return new TopicPartition(dlt, -1);
                });

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, retryBackOff(properties));

        // The classifier decides which of two backoffs applies, per failure. Expressed as a
        // function rather than as addNotRetryableExceptions(...) because the interesting cases
        // are not top-level exception types - what arrives is a wrapper, and the exception that
        // matters is somewhere down its cause chain. A predicate can walk that; a class list
        // cannot.
        handler.setBackOffFunction((record, exception) ->
                classifier.isRetryable(exception) ? retryBackOff(properties) : NO_RETRY);

        // Log every failed attempt, not just the last. Without this, a message that is retried
        // four times and then dead-lettered produces exactly one log line, and the question
        // "was it failing the same way each time?" - which is how you tell a flapping dependency
        // from a genuinely bad payload - has no answer.
        handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);

        return handler;
    }

    /**
     * A fresh {@link ExponentialBackOff} per call, because a {@link BackOff} is a factory for
     * executions and {@link DefaultErrorHandler} holds one per record - sharing a single mutable
     * instance across records is how a retry budget ends up shared between unrelated messages.
     *
     * <p>Note {@code setMaxAttempts(n)} yields {@code n} intervals and then stops, so total
     * deliveries are {@code n + 1}: the first attempt was not a retry.
     *
     * <p><b>Version note.</b> {@code ExponentialBackOffWithMaxRetries}, which every Spring Kafka
     * tutorial written before 2025 uses here, does not exist in Spring Framework 7 - it was
     * folded into {@link ExponentialBackOff#setMaxAttempts(int)}.
     */
    private static BackOff retryBackOff(DeadLetterProperties properties) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(properties.initialInterval().toMillis());
        backOff.setMultiplier(properties.multiplier());
        backOff.setMaxInterval(properties.maxInterval().toMillis());
        backOff.setMaxAttempts(Math.max(0, properties.maxAttempts() - 1));
        // Spreads the retries of records that all failed at the same instant. Without jitter,
        // fifty records that failed on one dead connection retry in lockstep forever, which is
        // the thundering herd the backoff was supposed to prevent.
        //
        // Note the unit: Spring's setJitter takes MILLISECONDS of random spread, not a fraction
        // of the interval. Passing 0.2 expecting "20%" does not compile, which is the good
        // outcome - passing 1 expecting the same would have compiled and done almost nothing.
        backOff.setJitter(Math.max(1L, properties.initialInterval().toMillis() / 5));
        return backOff;
    }

    /**
     * Makes the container stamp each delivery with a {@code kafka_deliveryAttempt} header, so the
     * dead letter row can record how many attempts it took rather than guessing.
     *
     * <p>A {@link ContainerCustomizer} bean is applied by Boot to the container factory it
     * auto-configures, which is how this reaches the listeners without this module having to
     * build a factory of its own and re-derive every property from {@code application.yml}.
     */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>
            deliveryAttemptCustomizer() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    /**
     * The container factory used by {@link DeadLetterConsumer}, and the only reason it exists is
     * that the dead letter listener must NOT inherit the handler above.
     *
     * <p>Two things would go wrong if it did.
     *
     * <p><b>The chain would not terminate.</b> A failure while recording a dead letter would
     * publish to {@code dpe.account.commands.v1.dlt.dlt}, which nothing consumes, and the next
     * failure to {@code .dlt.dlt.dlt}. The message is not lost, exactly - it is somewhere nobody
     * will ever look, which is worse than lost because it looks handled.
     *
     * <p><b>It would drop the copy of last resort.</b> The only reason to fail here is that
     * Postgres is unreachable, which is transient by nature and will resolve. So this factory
     * gets an error handler that retries forever with a fixed interval and never recovers,
     * deliberately blocking the dead letter partition until the database comes back. Blocking is
     * the right failure mode for this listener and the wrong one for the others, and the
     * difference is that here there is nowhere further to fall.
     *
     * <p><b>What an unbounded blocking backoff costs, stated honestly.</b> Spring Kafka's default
     * {@code BackOffHandler} sleeps in the listener thread, so an outage longer than
     * {@code max.poll.interval.ms} - five minutes, or sixty of these five-second waits - gets this
     * consumer evicted from the group. The partition is reassigned, the record is redelivered, and
     * the retrying resumes: nothing is lost, but the group rebalances every five minutes for as
     * long as the database is down, which is noisy exactly when the logs are already busy.
     * {@code ContainerPausingBackOffHandler} is the fix if that ever matters - it pauses the
     * container and keeps polling, so the consumer stays in the group across an arbitrarily long
     * wait. It is not wired here because it needs a {@code ListenerContainerPauseService} and a
     * scheduler, and a rebalance every five minutes during a total database outage is not the
     * problem anyone will be trying to solve at that moment.
     *
     * <p>{@link ConcurrentKafkaListenerContainerFactoryConfigurer} applies everything Boot would
     * have applied - the consumer factory, ack mode, {@code auto-startup}, concurrency - so this
     * factory differs from the default in exactly one respect. Building it by hand instead would
     * silently ignore {@code spring.kafka.listener.auto-startup=false}, and the dead letter
     * listener would then be running during every test that thought it had disabled listeners.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> deadLetterListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        // Long.MAX_VALUE retries, five seconds apart, no recoverer. See above.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(5_000L,
                Long.MAX_VALUE)));
        return factory;
    }
}
