package com.dpe.orchestrator.idempotency;

import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Reduces a request to the 64 hex characters stored in
 * {@code idempotency_records.request_fingerprint}.
 *
 * <h2>Why the fields and not the raw body</h2>
 *
 * <p>Hashing the bytes as they arrived is the obvious implementation and it is wrong in a way
 * that only shows up in production. Two JSON documents that mean the same thing are rarely byte
 * identical - a different key order, a trailing newline, {@code 30000} against {@code 30000.0},
 * a pretty-printer on the retry path - and each difference turns a legitimate retry into a 409
 * the client cannot act on. Canonicalizing the PARSED fields makes the fingerprint mean what it
 * should: the same intent, however it was spelled.
 *
 * <p>The separator matters more than it looks. Concatenating fields without one lets two
 * different requests produce identical input - account {@code ...ab} sending {@code 1} and
 * account {@code ...a} sending {@code b1} - so a genuinely different transfer would be treated
 * as a retry of the first and answered with its receipt. A delimiter that cannot occur inside a
 * UUID or a decimal number closes that.
 *
 * <h2>Why a hash at all</h2>
 *
 * <p>Only equality is ever asked of this value, so there is no reason to keep the request itself
 * - and a positive reason not to: the stored request would be a second copy of the payment
 * details sitting in a table with a long retention. SHA-256 rather than a fast non-cryptographic
 * hash because a collision here would replay the wrong response to a real transfer, and a
 * 32-byte digest makes that not worth reasoning about.
 */
final class RequestFingerprint {

    private static final char SEPARATOR = '|';

    private RequestFingerprint() {
    }

    /** The canonical fingerprint of a transfer request. */
    static String of(CreateTransferRequest request) {
        String canonical = new StringBuilder()
                .append(request.fromAccountId()).append(SEPARATOR)
                .append(request.toAccountId()).append(SEPARATOR)
                .append(request.amountMinor()).append(SEPARATOR)
                // Upper-cased for the same reason the transfer stores it upper-cased: "inr" and
                // "INR" are one currency, and a client whose retry differs only in case is
                // retrying, not asking for something new.
                .append(request.currency() == null ? "" : request.currency().toUpperCase())
                .toString();

        return sha256Hex(canonical);
    }

    private static String sha256Hex(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            // Every JVM is required to ship SHA-256, so this cannot happen; it is unchecked
            // rather than propagated so no caller has to pretend to handle it.
            throw new IllegalStateException("SHA-256 is missing from this JVM", e);
        }
    }
}
