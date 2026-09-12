package com.dpe.account.domain;

import java.util.UUID;

/**
 * Whether an account holds money or issues it.
 *
 * <p>{@link #CUSTOMER} accounts may never go negative - that is invariant I5, enforced by a
 * CHECK constraint. {@link #SYSTEM} is the single issuance account: money enters the ledger by
 * being debited from it, so its balance is negative and equals the total issued. Without it,
 * crediting a new account at opening time would be a credit with no matching debit, which is
 * money created from nothing and a broken I1.
 *
 * <p>{@link #CLEARING} arrived with the saga in M3 and holds money that has left the sender but
 * has not reached the recipient. See {@code V3__holds_and_inbox.sql} for why reserve-then-commit
 * cannot satisfy I1 and I3 at the same time without it.
 */
public enum AccountType {
    CUSTOMER,
    SYSTEM,

    /**
     * A funds-in-transit account. Credited by a reserve, debited by the commit or the release that
     * ends it. Since M8 there are several - shards, so concurrent transfers do not all queue on one
     * row lock - and each hold records which one its money is parked in (V8).
     *
     * <p>Not a CUSTOMER account, and that is load-bearing rather than cosmetic: invariant I3 sums
     * CUSTOMER balances plus ACTIVE holds, so money parked here is deliberately outside the sum
     * and is counted by the hold instead. If this were typed CUSTOMER, every reserve would count
     * the same money twice and I3 would fail immediately.
     *
     * <p>Each shard's balance at rest is zero, and at any instant it equals the total value of the
     * ACTIVE holds that name it. That identity is a free reconciliation check - a divergence means
     * some code path wrote a hold without its ledger pair, a pair without its hold, or settled a
     * hold against a different shard than it was parked in.
     */
    CLEARING;

    /** Seeded by {@code V1__ledger_core.sql}. Well-known so scripts need no lookup. */
    public static final UUID SYSTEM_ACCOUNT_ID =
            UUID.fromString("00000000-0000-0000-0000-000000000001");

    /**
     * The ORIGINAL clearing account, seeded by {@code V3__holds_and_inbox.sql} - since V8, shard 0
     * of eight. Every hold written before V8 names it. Not "the" clearing account any more: code
     * that needs a transfer's clearing account reads it from the hold, and a total reads every
     * account of type CLEARING.
     */
    public static final UUID CLEARING_ACCOUNT_ID =
            UUID.fromString("00000000-0000-0000-0000-000000000002");
}
