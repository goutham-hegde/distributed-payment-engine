package com.dpe.account.web;

/**
 * The {@code cursor} query parameter was not something this service issued. A 400.
 *
 * <p>Not a silent fall back to the first page: a client paging a long history with a corrupted
 * cursor would loop over page one forever and never see an error.
 */
public class InvalidCursorException extends RuntimeException {

    public InvalidCursorException(String message) {
        super(message);
    }
}
