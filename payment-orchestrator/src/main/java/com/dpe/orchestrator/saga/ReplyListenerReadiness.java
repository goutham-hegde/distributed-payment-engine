package com.dpe.orchestrator.saga;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Answers one question for the timeout sweeper: <b>can this service hear replies right now?</b>
 *
 * <p>A timeout is a statement about the other side - "they did not answer in time" - and it is
 * only true while you are able to hear them. M7's chaos scenario 3 showed what happens otherwise.
 * After a SIGKILL the orchestrator restarted, served HTTP, and ran its relay and its sweeper for
 * about twenty-three seconds before its consumer rejoined the group (consistent with the dead
 * member being held until its session expired - not proven). The replies were already sitting in
 * the topic, unread, and the sweeper failed the sagas they belonged to.
 *
 * <p>So the sweeper is gated on two facts:
 *
 * <ol>
 *   <li>The reply listener has partitions assigned. Before the group join completes it has none,
 *       and whatever it is "waiting for" may already have arrived.</li>
 *   <li>It has had them for at least {@code dpe.saga.listen-grace}. Being assigned is not the same
 *       as having read the backlog; the grace is time to drain what queued up while it was deaf.
 *       It is measured from the first sweep that SAW the assignment, so its resolution is one sweep
 *       interval - coarse, and deliberately simple.</li>
 * </ol>
 *
 * <h2>What it does not cover, stated rather than implied</h2>
 *
 * <ul>
 *   <li><b>A consumer that is assigned but not fetching</b> - the silent stall chaos scenario 1
 *       found after a broker restart. It looks exactly like a healthy idle consumer from here. Only
 *       lag that does not fall can show it, and that belongs to an alert, not to this gate.</li>
 *   <li><b>A broker outage.</b> The client keeps its assignment while the coordinator is
 *       unreachable, so the gate stays open. That is survivable since M7 because every
 *       compensation now commutes with the step it undoes - a timeout during an outage produces a
 *       spurious failure, not stranded money.</li>
 *   <li><b>Several instances.</b> The gate is per instance, and the sweeper claims any expired saga,
 *       including one whose replies land on another instance's partitions.</li>
 * </ul>
 *
 * <p>Static group membership ({@code group.instance.id}) was the other candidate and was not
 * chosen: it shortens the rejoin after a crash, but a static member does not send LeaveGroup on a
 * CLEAN shutdown either, so every ordinary deploy of a scaled-out service would leave its
 * partitions unowned for a full session timeout. It also does nothing when the broker is what
 * restarted. The gate is needed whichever way the rejoin is made faster.
 */
@Component
public class ReplyListenerReadiness {

    private final KafkaListenerEndpointRegistry registry;
    private final Duration grace;

    /** When this instance first saw partitions assigned, or null while it has none. */
    private volatile Instant hearingSince;

    public ReplyListenerReadiness(KafkaListenerEndpointRegistry registry,
                                  SagaProperties properties) {
        this.registry = registry;
        this.grace = properties.listenGrace();
    }

    public boolean canHear() {
        return canHear(Instant.now());
    }

    boolean canHear(Instant now) {
        MessageListenerContainer container =
                registry.getListenerContainer(SagaReplyConsumer.LISTENER_ID);
        Collection<TopicPartition> assigned =
                container == null || !container.isRunning() ? null : container.getAssignedPartitions();
        if (assigned == null || assigned.isEmpty()) {
            hearingSince = null;
            return false;
        }
        if (hearingSince == null) {
            hearingSince = now;
        }
        return !now.isBefore(hearingSince.plus(grace));
    }
}
