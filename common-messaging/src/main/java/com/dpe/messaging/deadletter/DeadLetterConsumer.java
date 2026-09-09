package com.dpe.messaging.deadletter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Drains the dead letter topics into the {@code dead_letters} table.
 *
 * <p>Transport only, exactly like every other consumer here: hand the record to a transactional
 * handler, and acknowledge only once that transaction has committed. If the handler throws, the
 * offset is not committed and the record is redelivered - which for this listener means it
 * retries forever, on purpose. See {@code KafkaErrorHandlingConfig#deadLetterListenerContainerFactory}.
 *
 * <h2>The topic list comes from a bean, not from a property</h2>
 *
 * <p>{@code "#{@dltTopics}"} is a SpEL bean reference: Spring resolves it at listener-registration
 * time against a bean named {@code dltTopics}, which each service declares in its own
 * {@code KafkaTopicsConfig} next to its {@code NewTopic} declarations.
 *
 * <p>The alternative - a comma-separated list in {@code application.yml} - was rejected because
 * it duplicates the topic names in a second place that can drift from {@code Topics}, and a
 * mistyped name there is not an error: the listener subscribes to a topic nobody publishes to
 * and simply never fires. Deriving the list from the same constants the {@code NewTopic} beans
 * use means the names cannot disagree with themselves.
 *
 * <h2>Same consumer group as the service's other listeners</h2>
 *
 * <p>Which is safe, and worth spelling out, because the project rule says "one consumer group per
 * service, and therefore one inbound listener per service". That rule is about two groups reading
 * the SAME topic and both writing the same inbox: whichever committed first would make the other
 * skip its message as a duplicate. Neither half applies here. This listener reads different
 * topics, and it writes to {@code dead_letters}, keyed by the source Kafka coordinates rather
 * than by the message id, so it shares no dedup key with anything.
 */
@Component
public class DeadLetterConsumer {

    private final DeadLetterRecorder recorder;

    public DeadLetterConsumer(DeadLetterRecorder recorder) {
        this.recorder = recorder;
    }

    @KafkaListener(topics = "#{@dltTopics}",
            containerFactory = "deadLetterListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        recorder.record(record);
        ack.acknowledge();
    }
}
