package com.dpe.orchestrator.transfer;

/**
 * The caller-facing outcome of a transfer. Three words, deliberately.
 *
 * <p>FAILED covers both "the sender did not have the money" and "the gateway declined and we
 * compensated". A caller cannot act differently on those - their money is where it started
 * either way - so splitting them in the public status would expose an internal distinction and
 * buy nothing. The distinction is kept in {@code saga_instances.status} and in the transfer's
 * failure reason, for whoever is debugging.
 */
public enum TransferStatus {
    PENDING,
    COMPLETED,
    FAILED
}
