package com.dpe.orchestrator.transfer;

/** A request that is malformed on its face, before any participant is asked anything. */
public class InvalidTransferException extends RuntimeException {

    public InvalidTransferException(String message) {
        super(message);
    }
}
