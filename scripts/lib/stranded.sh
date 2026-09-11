#!/usr/bin/env bash
#
# S1-S4: the stranded-money checks. Sourced, never run.
#
# One definition, three readers: scripts/verify-invariants.sh (the absolute check), chaos/lib.sh
# (the same queries, judged on violations NEW to a scenario), and scripts/reconcile.sh (which turns
# S1 and S2 into work). They used to live only in the chaos harness, which is how a check drifts:
# the day the k6 run needed them it would have grown its own copy.
#
# Each function prints the VIOLATING transfer ids, one per line, sorted - never a count - so a
# caller can diff two snapshots, or act on the ids.
#
# These are statements about THREE databases at once, which is why they are shell and not an
# endpoint: no service may hold credentials to more than its own (see InvariantsController). This
# file runs as an operator's tool through the postgres container's superuser.
#
# All four assume QUIESCENCE, like I4. Mid-run, an ACTIVE hold is money correctly in flight.

STRANDED_PG="${PG_CONTAINER:-dpe-postgres}"

_stranded_sql() {
    docker exec -i "$STRANDED_PG" psql -U postgres -d "$1" -tAq -v ON_ERROR_STOP=1 -c "$2" \
        | tr -d '\r' | sed '/^$/d'
}

# S1. No ACTIVE hold. I3 adds held money back into the total - right mid-run, and precisely why a
# hold stranded forever is invisible to it.
stranded_s1() {
    _stranded_sql accounts_db "SELECT transfer_id FROM holds WHERE status = 'ACTIVE'" | sort
}

# S2. Every APPROVED charge belongs to a COMPLETED transfer. Otherwise the PSP kept money for a
# transfer we told the customer did not happen. A VOIDED charge is not approved: the PSP holds
# nothing for it.
stranded_s2() {
    comm -23 \
        <(_stranded_sql gateway_db  "SELECT transfer_id FROM gateway_charges WHERE status = 'APPROVED'" | sort) \
        <(_stranded_sql payments_db "SELECT id FROM transfers WHERE status = 'COMPLETED'" | sort)
}

# S3. Every COMPLETED transfer has an APPROVED charge - money never moves without the PSP agreeing.
stranded_s3() {
    comm -13 \
        <(_stranded_sql gateway_db  "SELECT transfer_id FROM gateway_charges WHERE status = 'APPROVED'" | sort) \
        <(_stranded_sql payments_db "SELECT id FROM transfers WHERE status = 'COMPLETED'" | sort)
}

# S4. No transfer PENDING under a terminal saga - a customer told "processing" about something
# that has finished.
stranded_s4() {
    _stranded_sql payments_db "SELECT t.id FROM transfers t JOIN saga_instances s ON s.transfer_id = t.id
                                WHERE s.status IN ('COMPLETED','COMPENSATED','FAILED') AND t.status = 'PENDING'" | sort
}
