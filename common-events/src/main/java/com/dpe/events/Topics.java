package com.dpe.events;

/**
 * Kafka topic names, in one place so producers and consumers cannot drift.
 *
 * <p>Naming convention: {@code dpe.<bounded-context>.<kind>.v<version>}. The explicit version
 * suffix is what lets us evolve a message shape without breaking live consumers - a breaking
 * change publishes to {@code .v2} while {@code .v1} consumers keep working until they migrate.
 *
 * <p>Commands are directed at exactly one service ("do this"). Events are broadcast facts
 * ("this happened"). Keeping them on separate topics means a new subscriber can consume events
 * without accidentally receiving commands it must not act on.
 */
public final class Topics {

    private Topics() {
    }

    // Commands: orchestrator -> service
    public static final String ACCOUNT_COMMANDS = "dpe.account.commands.v1";
    public static final String GATEWAY_COMMANDS = "dpe.gateway.commands.v1";

    // Events: service -> orchestrator (and any other interested subscriber)
    public static final String ACCOUNT_EVENTS = "dpe.account.events.v1";
    public static final String GATEWAY_EVENTS = "dpe.gateway.events.v1";

    /**
     * Dead letter topic suffix. Spring Kafka's {@code DeadLetterPublishingRecoverer} appends
     * this to the original topic name once the retry budget is exhausted, so a poison message
     * stops blocking the partition but is never silently dropped.
     */
    public static final String DLT_SUFFIX = ".dlt";
}
