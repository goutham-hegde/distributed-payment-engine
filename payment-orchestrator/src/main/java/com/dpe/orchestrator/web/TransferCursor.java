package com.dpe.orchestrator.web;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.UUID;

/**
 * Where the last page stopped: {@code (created_at, id)} of the final row, as an opaque string.
 *
 * <h2>Why a cursor at all, rather than {@code ?page=3}</h2>
 *
 * <p>{@code OFFSET 5000} asks Postgres to produce five thousand rows and throw them away, so the
 * cost of a page grows with how deep into the list it is - the last page of a long history is the
 * most expensive one to serve, which is exactly backwards. That is the performance half.
 *
 * <p>The correctness half is worse and it is the reason this is not negotiable on a table that is
 * still being written to. {@code transfers} gains rows while a client pages through it, and the
 * list is ordered newest first. Insert one row between the client's page 1 and page 2 and every
 * row shifts down by one position: the row that was last on page 1 is now first on page 2 and is
 * returned twice, and one row is skipped for every page after that. An offset names <i>a count of
 * rows</i>; a keyset names <i>a place in the data</i>, and only the second one still means the
 * same thing after a concurrent insert.
 *
 * <h2>Why both columns</h2>
 *
 * <p>{@code created_at} alone is not unique. Two transfers committed in the same microsecond are
 * ordered arbitrarily, and a page boundary that falls between them will either return one twice or
 * lose one, depending on which way the planner happened to emit them. The id is the tiebreak that
 * makes the ordering total, and it is in {@code idx_transfers_initiated_by} for that reason.
 *
 * <h2>Why it is opaque</h2>
 *
 * <p>A client that can read {@code createdAt=...&lastId=...} will eventually construct one by
 * hand, and then the cursor's internals are a published API this service cannot change - it could
 * not add a third sort column without breaking callers. Base64 says "this value came from us,
 * give it back unmodified". It is not a security boundary and is not pretending to be one: it
 * encodes a timestamp and an id the caller already saw in the response body. What it buys is the
 * freedom to change the shape.
 */
public record TransferCursor(OffsetDateTime createdAt, UUID id) {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    /**
     * Encodes as {@code <epochSecond>:<nanoOfSecond>:<uuid>}.
     *
     * <p>Seconds and nanos separately, rather than an ISO-8601 string, because the value has to
     * survive the round trip <b>exactly</b>: it is compared with {@code <} against a
     * {@code timestamptz}, and a cursor that loses the last digits of the microsecond field
     * re-reads the row it was supposed to stop after. Text formats invite exactly that loss
     * through a truncating parser or a zone conversion.
     */
    public String encode() {
        Instant instant = createdAt.toInstant();
        String raw = instant.getEpochSecond() + ":" + instant.getNano() + ":" + id;
        return ENCODER.encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @throws InvalidCursorException if the value was not produced by {@link #encode()}
     */
    public static TransferCursor decode(String encoded) {
        String raw;
        try {
            raw = new String(DECODER.decode(encoded), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new InvalidCursorException("cursor is not valid base64url");
        }

        // limit 3 rather than a plain split: a UUID contains no colon today, but a decoder that
        // depends on that is one field-format change away from a confusing failure.
        String[] parts = raw.split(":", 3);
        if (parts.length != 3) {
            throw new InvalidCursorException("cursor does not have three fields");
        }

        try {
            Instant instant = Instant.ofEpochSecond(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
            return new TransferCursor(instant.atOffset(ZoneOffset.UTC), UUID.fromString(parts[2]));
        } catch (RuntimeException e) {
            throw new InvalidCursorException("cursor fields are not a timestamp and an id");
        }
    }
}
