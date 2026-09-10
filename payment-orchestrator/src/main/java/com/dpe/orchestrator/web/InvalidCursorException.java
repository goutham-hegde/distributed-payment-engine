package com.dpe.orchestrator.web;

/**
 * The {@code cursor} query parameter was not something this service issued.
 *
 * <p>A 400, not a 500 and not a silent fall back to the first page. Silently restarting from the
 * beginning is the tempting behaviour and it is wrong twice over: a client paging through a long
 * list would loop forever without ever seeing an error, and a corrupted cursor - the interesting
 * case - would be indistinguishable from a fresh request.
 */
public class InvalidCursorException extends RuntimeException {

    public InvalidCursorException(String message) {
        super(message);
    }
}
