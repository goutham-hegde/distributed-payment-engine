package com.dpe.account.domain;

/**
 * The two halves of a double-entry posting.
 *
 * <p>The sign of {@code amount_minor} is redundant with this value on purpose, and a CHECK
 * constraint keeps them in lockstep: a {@code DEBIT} row is always negative, a {@code CREDIT}
 * row always positive. That redundancy is what lets invariant I1 be a plain {@code SUM()}
 * rather than a CASE expression that could itself be written wrongly.
 */
public enum EntryType {
    DEBIT,
    CREDIT
}
