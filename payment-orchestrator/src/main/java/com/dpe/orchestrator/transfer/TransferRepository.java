package com.dpe.orchestrator.transfer;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TransferRepository extends JpaRepository<Transfer, UUID> {

    Optional<Transfer> findByIdAndFromAccountId(UUID id, UUID fromAccountId);

    /**
     * The first page of one subject's transfers, newest first.
     *
     * <h2>Why native, and why two methods instead of one with a nullable cursor</h2>
     *
     * <p>Native because the next method needs the row-value comparison {@code (a, b) < (:x, :y)},
     * which HQL cannot express and which is the entire reason the keyset page is an index scan
     * rather than a scan-and-filter. Once one of the pair is native, both are, so the two queries
     * that must produce the same ordering are written in the same language and can be read
     * against each other.
     *
     * <p>Two methods because the alternative is {@code (:cursor IS NULL OR (created_at, id) <
     * (:cursor, :cursorId))} - one query with a branch inside it, which Postgres must evaluate per
     * row and which makes the plan depend on a bound value. Splitting the branch out gives two
     * statements that each have exactly one plan.
     *
     * <p>The subject is {@code transfers.initiated_by}, which is the token's {@code sub}. Scoping
     * is in the WHERE clause, not applied to the result afterwards: a filter applied after the
     * LIMIT would return short pages full of other people's rows removed, and a page size that
     * varies with how many of your neighbour's transfers happened to sort nearby is a data leak
     * wearing a pagination bug's clothes.
     *
     * @param limit ask for one MORE than the page size; the extra row is how the service knows
     *              whether a next page exists without a second query
     */
    @Query(value = """
            SELECT * FROM transfers
            WHERE initiated_by = :subject
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<Transfer> findFirstPageFor(@Param("subject") String subject,
                                    @Param("limit") int limit);

    /**
     * The page after {@code (cursorCreatedAt, cursorId)}.
     *
     * <p>{@code (created_at, id) < (:cursorCreatedAt, :cursorId)} is a <b>row-value comparison</b>,
     * not two comparisons and-ed together. It means "strictly earlier in the composite ordering",
     * which is the same ordering as the {@code ORDER BY} and the same as the index - so Postgres
     * descends the B-tree straight to the cursor position and reads forward.
     *
     * <p>The hand-expanded form people write instead - {@code created_at < :c OR (created_at = :c
     * AND id < :id)} - is logically identical and is planned as a filter over a wider range,
     * because the planner will not reassemble an OR into a range scan. Same answer, different
     * cost, and the difference only appears once the table is big enough that nobody is looking.
     */
    @Query(value = """
            SELECT * FROM transfers
            WHERE initiated_by = :subject
              AND (created_at, id) < (:cursorCreatedAt, :cursorId)
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<Transfer> findPageAfter(@Param("subject") String subject,
                                 @Param("cursorCreatedAt") OffsetDateTime cursorCreatedAt,
                                 @Param("cursorId") UUID cursorId,
                                 @Param("limit") int limit);
}
