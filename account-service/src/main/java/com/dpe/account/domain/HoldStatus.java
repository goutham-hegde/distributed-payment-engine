package com.dpe.account.domain;

/**
 * The life of a hold. {@link #ACTIVE} is the only state that counts toward invariant I3.
 */
public enum HoldStatus {

    /** Money has left the sender and is sitting in the CLEARING account. */
    ACTIVE,

    /** Settled: the recipient was credited. Terminal. */
    COMMITTED,

    /** Compensated: the money went back to the sender. Terminal. */
    RELEASED;

    public boolean isTerminal() {
        return this != ACTIVE;
    }
}
