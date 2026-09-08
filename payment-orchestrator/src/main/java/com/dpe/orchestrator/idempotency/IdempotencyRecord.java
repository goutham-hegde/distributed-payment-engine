package com.dpe.orchestrator.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * One client intent, and what we answered the first time we saw it.
 *
 * <p>Read-only from Java's point of view: the row is created and completed by the native
 * statements on {@link IdempotencyRepository}, and this entity exists so a duplicate can be
 * <i>read back</i> with a typed accessor rather than a map of columns. There are no setters, and
 * that is deliberate - a JPA dirty-check writing to this table behind the gate's back is exactly
 * the kind of second write path the constraint cannot see.
 *
 * <p>The identity is the pair, so the key class below is the {@code @IdClass}. It is not a
 * surrogate id with a UNIQUE index beside it: a surrogate would let a second row for the same
 * pair be inserted by any code path that forgot to check, which is the failure this table exists
 * to make impossible.
 */
@Entity
@Table(name = "idempotency_records")
@IdClass(IdempotencyRecord.Key.class)
public class IdempotencyRecord {

    @Id
    @Column(name = "client_id", nullable = false, updatable = false, length = 64)
    private String clientId;

    @Id
    @Column(name = "idempotency_key", nullable = false, updatable = false, length = 255)
    private String idempotencyKey;

    @Column(name = "request_fingerprint", nullable = false, updatable = false, length = 64)
    private String requestFingerprint;

    @Column(name = "response_status")
    private Integer responseStatus;

    // Plain TEXT, unlike OutboxMessage.payload - no @JdbcTypeCode here on purpose. A jsonb column
    // would round-trip through Postgres's own serializer and hand back different bytes than were
    // stored, and this value has to come back verbatim. See V3__idempotency.sql.
    @Column(name = "response_body")
    private String responseBody;

    @Column(name = "transfer_id")
    private UUID transferId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "expires_at", nullable = false, updatable = false)
    private OffsetDateTime expiresAt;

    protected IdempotencyRecord() {
        // for JPA
    }

    public String getClientId() {
        return clientId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    /**
     * The SHA-256 of the request that created this record. Compare it against the current
     * request's fingerprint before replaying anything: equal means a genuine retry, different
     * means the client reused a key for a different intent and must be told 409.
     */
    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public Integer getResponseStatus() {
        return responseStatus;
    }

    /** The original response, verbatim, as JSON text. */
    public String getResponseBody() {
        return responseBody;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getExpiresAt() {
        return expiresAt;
    }

    /**
     * The composite primary key: whose key, and which key.
     *
     * <p>A plain class rather than a record, and it is not a style choice. An {@code @IdClass}
     * must be instantiable by the persistence provider through a public no-argument constructor,
     * which a record cannot have - its canonical constructor is the only one and every component
     * is final. Hibernate builds an instance of this class whenever it materializes an id, so a
     * record here fails at bootstrap rather than at the first query.
     */
    public static final class Key implements Serializable {

        private String clientId;
        private String idempotencyKey;

        public Key() {
            // for the persistence provider
        }

        public Key(String clientId, String idempotencyKey) {
            this.clientId = Objects.requireNonNull(clientId, "clientId");
            this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        }

        public String getClientId() {
            return clientId;
        }

        public String getIdempotencyKey() {
            return idempotencyKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Key other
                    && Objects.equals(clientId, other.clientId)
                    && Objects.equals(idempotencyKey, other.idempotencyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, idempotencyKey);
        }

        @Override
        public String toString() {
            return clientId + "/" + idempotencyKey;
        }
    }
}
