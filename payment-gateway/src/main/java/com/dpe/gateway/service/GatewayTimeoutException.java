package com.dpe.gateway.service;

/**
 * The PSP did not answer.
 *
 * <p>Thrown rather than turned into a {@code FAILED} charge row on purpose. Throwing rolls back
 * the whole handler transaction - the inbox row included - so the command is redelivered and
 * tried again, which is the right response to a failure that might not repeat. Writing a FAILED
 * row instead would commit "we gave up" as a durable fact after a single attempt.
 *
 * <p>It is a {@link RuntimeException} so that Spring's transaction manager rolls back on it
 * without any {@code rollbackFor} configuration; a checked exception would commit by default,
 * which is precisely the wrong behaviour and a genuinely easy mistake to make.
 */
public class GatewayTimeoutException extends RuntimeException {

    public GatewayTimeoutException(String message) {
        super(message);
    }
}
