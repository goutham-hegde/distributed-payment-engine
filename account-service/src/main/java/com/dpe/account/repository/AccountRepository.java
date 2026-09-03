package com.dpe.account.repository;

import com.dpe.account.domain.Account;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, UUID> {

    /**
     * Reads an account and holds a row-level write lock on it until the surrounding transaction
     * ends. Emits {@code SELECT ... FOR UPDATE}.
     *
     * <p>This is what makes the lost update impossible. Postgres defaults to READ COMMITTED,
     * which prevents dirty reads but happily lets two transactions both read a balance of 1000,
     * both approve a debit of 600, and both write 400. A second caller of this method blocks
     * until the first commits, and then reads the committed value.
     *
     * <p><strong>Call this once per account, in a deterministic order.</strong> There is
     * deliberately no batch {@code WHERE id IN (...) FOR UPDATE} variant here: the order in
     * which Postgres locks the rows matched by an {@code IN} list is not part of the contract,
     * even with an {@code ORDER BY}, so a batch fetch silently reintroduces the deadlock this
     * milestone is about. Two sorted single-row calls are explicit and provably ordered.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") UUID id);
}
