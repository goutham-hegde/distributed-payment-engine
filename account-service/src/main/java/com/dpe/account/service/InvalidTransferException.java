package com.dpe.account.service;

/**
 * The command is malformed in a way that no amount of retrying will fix: a non-positive amount,
 * a self-transfer, or a currency that does not match the accounts involved.
 */
public class InvalidTransferException extends RuntimeException {

    public InvalidTransferException(String message) {
        super(message);
    }
}
