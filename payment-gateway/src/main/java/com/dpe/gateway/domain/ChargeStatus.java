package com.dpe.gateway.domain;

/**
 * How a charge attempt ended.
 *
 * <p>The distinction between {@link #DECLINED} and {@link #FAILED} is the one that matters, and
 * it is not a shade of the same thing. A decline is an <b>answer</b> - the PSP considered the
 * charge and refused it, nothing was taken, and the outcome is known. A failure is the
 * <b>absence</b> of an answer - a timeout or a reset - and the charge may or may not have gone
 * through on the other side.
 *
 * <p>Only the first is safe to compensate on immediately. The second is why
 * {@code gateway_charges.transfer_id} is UNIQUE: a retry after an unknown outcome finds the
 * original row instead of charging a second time, which converts "we do not know" into "ask
 * again and find out".
 */
public enum ChargeStatus {
    APPROVED,
    DECLINED,
    FAILED
}
