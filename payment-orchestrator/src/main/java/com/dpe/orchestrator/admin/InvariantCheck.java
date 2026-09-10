package com.dpe.orchestrator.admin;

/**
 * One invariant, evaluated now.
 *
 * <p><b>This record's shape is a contract between two services that do not share a database.</b>
 * account-service publishes the same one from its own {@code /admin/invariants}, because the five
 * invariants are split across {@code accounts_db} and {@code payments_db} and no single service
 * can answer all of them. The console asks both and renders one row per check.
 *
 * <p>It is deliberately duplicated in both services rather than lifted into a shared module. The
 * shared modules in this repo exist for MECHANISM - {@code common-messaging} holds the outbox and
 * inbox machinery, {@code common-security} holds token validation - and a five-field response
 * record is not a mechanism. Putting it in one of them would make every service redeploy when the
 * console wanted an extra field, which is the coupling those module boundaries were drawn to
 * avoid. See {@code docs/adr/0004-shared-messaging-library.md}.
 *
 * @param id                 the invariant's name in the project's own vocabulary: I1 to I5
 * @param holds              whether it held at the instant it was evaluated
 * @param detail             the actual number, always - including when the check passes. "I2 holds"
 *                           and "I2 holds and 412 accounts were compared" are different amounts of
 *                           evidence, and a check that reports nothing when it passes is a check
 *                           nobody can tell from one that did not run
 * @param requiresQuiescence true for an invariant that is only meaningful once nothing is in
 *                           flight. I4 is the one: a saga in {@code RESERVED} is a saga working
 *                           correctly at this instant and a violation ten minutes later, and a
 *                           console that paints it red under load teaches its operators to ignore
 *                           it. The flag is how the UI knows to say "in flight" rather than
 *                           "violated"
 */
public record InvariantCheck(
        String id,
        String title,
        boolean holds,
        String detail,
        boolean requiresQuiescence) {

    static InvariantCheck of(String id, String title, boolean holds, String detail) {
        return new InvariantCheck(id, title, holds, detail, false);
    }

    static InvariantCheck atQuiescence(String id, String title, boolean holds, String detail) {
        return new InvariantCheck(id, title, holds, detail, true);
    }
}
