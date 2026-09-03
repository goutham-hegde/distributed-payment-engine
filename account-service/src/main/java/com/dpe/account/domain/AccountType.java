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
 */
public enum AccountType {
    CUSTOMER,
    SYSTEM;

    /** Seeded by {@code V1__ledger_core.sql}. Well-known so scripts need no lookup. */
    public static final UUID SYSTEM_ACCOUNT_ID =
            UUID.fromString("00000000-0000-0000-0000-000000000001");
}
