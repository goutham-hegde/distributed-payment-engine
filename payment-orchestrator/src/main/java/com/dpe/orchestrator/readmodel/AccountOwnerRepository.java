package com.dpe.orchestrator.readmodel;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountOwnerRepository extends JpaRepository<AccountOwner, UUID> {

    /**
     * Records who owns an account.
     *
     * <p>{@code DO NOTHING}, not {@code DO UPDATE}, and the choice is a statement about the
     * domain: <b>ownership is decided once, when the account is opened, and never changes.</b> A
     * second AccountOpened for the same account id can only be a redelivery or a producer bug,
     * and in either case the row already here is the right one. An upsert that overwrote would
     * turn a replayed message into a silent change of who can spend somebody's money.
     *
     * <p>The inbox gate in {@link AccountOwnerHandler} already stops duplicate <i>messages</i>
     * from reaching this statement. This is the second line: the inbox dedupes on message id, so
     * it cannot recognise two DIFFERENT messages that claim the same account - which is exactly
     * what a buggy or malicious producer would send.
     */
    @Modifying
    @Query(value = """
            INSERT INTO account_owners (account_id, owner_id, account_type, currency)
            VALUES (:accountId, :ownerId, :accountType, :currency)
            ON CONFLICT (account_id) DO NOTHING
            """, nativeQuery = true)
    int recordOwner(@Param("accountId") UUID accountId,
                    @Param("ownerId") String ownerId,
                    @Param("accountType") String accountType,
                    @Param("currency") String currency);
}
