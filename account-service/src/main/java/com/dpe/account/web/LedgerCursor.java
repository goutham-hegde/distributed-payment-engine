package com.dpe.account.web;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Where the last page of ledger history stopped: the id of its final entry, opaquely encoded.
 *
 * <h2>Why this is not shared with the orchestrator's TransferCursor</h2>
 *
 * <p>Because they encode different things. A transfer page is ordered by
 * {@code (created_at, id)} and its cursor carries both; a ledger page is ordered by a BIGSERIAL
 * that is already a total order, so its cursor is one number. A shared "cursor" abstraction would
 * have to be the union of the two, which means every caller carries fields it does not use and the
 * two services' pagination contracts become one thing that cannot change independently - in
 * exchange for saving about fifteen lines, in two services that share no database.
 *
 * <p>Same reasoning as {@code common-messaging}'s boundary: the shared modules in this repo hold
 * MECHANISM, not shapes that happen to rhyme.
 *
 * <h2>Why encode a number at all</h2>
 *
 * <p>Not secrecy - the id is in the response body the client just read. It is to stop clients
 * doing arithmetic on it. A raw {@code ?afterId=41} invites {@code ?afterId=1} and a client that
 * has decided ids are dense and sequential; the day this table is partitioned, or ids come from a
 * sequence with a cache, that client silently skips rows. Base64 says "give this back to us
 * unmodified" and keeps the ordering key an implementation detail.
 */
public record LedgerCursor(long entryId) {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public String encode() {
        return ENCODER.encodeToString(Long.toString(entryId).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @throws InvalidCursorException if the value was not produced by {@link #encode()}
     */
    public static LedgerCursor decode(String encoded) {
        try {
            String raw = new String(DECODER.decode(encoded), StandardCharsets.UTF_8);
            return new LedgerCursor(Long.parseLong(raw));
        } catch (IllegalArgumentException e) {
            // Covers both halves - IllegalArgumentException is the supertype of
            // NumberFormatException, and base64 decoding throws it directly.
            throw new InvalidCursorException("cursor is not one this API issued");
        }
    }
}
