#!/usr/bin/env bash
#
# Finds money stranded by a saga that has already finished, and asks the orchestrator to finish the
# compensation. An operator's tool: it may read three databases because no service can.
#
#   ./scripts/reconcile.sh            dry run - list what would be reconciled, change nothing
#   ./scripts/reconcile.sh --apply    POST /admin/transfers/{id}/reconcile for each, then re-check
#
# What it acts on, and what it refuses to:
#
#   S1  an ACTIVE hold under a FAILED/COMPENSATED saga   -> reconcile: release, then void
#   S2  an APPROVED charge under a non-COMPLETED transfer -> reconcile: release answer, then void
#   S3  a COMPLETED transfer with no approved charge      -> REPORTED ONLY. Money moved without the
#                                                           PSP agreeing; no compensation is the
#                                                           right answer to that, a person is
#   S4  a PENDING transfer under a terminal saga         -> REPORTED ONLY
#
# The orchestrator makes the actual decision per transfer and refuses a COMPLETED or live saga
# with a 409, so a stale list here cannot do damage - see SagaOrchestrator#reconcile for why that
# endpoint is not "an operator moving money", and why it asks account-service before it voids.
#
# Idempotent. Running it twice sends every command twice, and every participant absorbs the second.

set -uo pipefail
export MSYS_NO_PATHCONV=1
command -v docker >/dev/null 2>&1 || export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"

SCRIPTS="$(cd "$(dirname "$0")" && pwd)"
ORCH="${ORCHESTRATOR_URL:-http://localhost:8081}"
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

# shellcheck source=lib/stranded.sh
source "$SCRIPTS/lib/stranded.sh"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/dpe-reconcile.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

summarise() {
    stranded_s1 > "$WORK/s1"; stranded_s2 > "$WORK/s2"
    stranded_s3 > "$WORK/s3"; stranded_s4 > "$WORK/s4"
    printf '  S1 ACTIVE holds under a finished saga     %s\n' "$(wc -l < "$WORK/s1" | tr -d ' ')"
    printf '  S2 APPROVED charges, transfer not COMPLETED %s\n' "$(wc -l < "$WORK/s2" | tr -d ' ')"
    printf '  S3 COMPLETED, no approved charge (manual)  %s\n' "$(wc -l < "$WORK/s3" | tr -d ' ')"
    printf '  S4 PENDING under a terminal saga (manual)  %s\n' "$(wc -l < "$WORK/s4" | tr -d ' ')"
}

echo "Stranded money now:"
summarise
sort -u "$WORK/s1" "$WORK/s2" | sed '/^$/d' > "$WORK/targets"
n="$(wc -l < "$WORK/targets" | tr -d ' ')"

if [ -s "$WORK/s1" ]; then
    echo
    echo "S1 by saga state (only FAILED/COMPENSATED can be reconciled; anything else is still live):"
    _stranded_sql payments_db "SELECT status, COUNT(*) FROM saga_instances
                               WHERE transfer_id IN ($(sed "s/.*/'&'/" "$WORK/s1" | paste -sd, -))
                               GROUP BY status" | sed 's/^/  /'
fi

if [ "$n" = "0" ]; then
    echo; echo "Nothing to reconcile."
    exit 0
fi
if [ "$APPLY" = "0" ]; then
    echo; echo "$n transfer(s) would be reconciled. Re-run with --apply to send the requests."
    exit 0
fi

op="$("$SCRIPTS/token.sh" operator)" || { echo "error: could not get an operator token" >&2; exit 2; }

echo; echo "Requesting reconciliation for $n transfer(s)"
declare -A tally=()
while read -r id; do
    # Body and status in one capture, never `curl -o $WORK/...`: under MSYS_NO_PATHCONV a Windows
    # curl is handed /tmp/... untranslated and fails every write with "(23) Failure writing output".
    out="$(curl -sS -m 10 -w '\n%{http_code}' -X POST \
        -H "Authorization: Bearer $op" "$ORCH/admin/transfers/$id/reconcile" 2>&1)"
    code="$(printf '%s' "$out" | tail -n1)"
    tally[$code]=$(( ${tally[$code]:-0} + 1 ))
    [ "$code" = "202" ] || printf '  %s  %s  %s\n' "$code" "$id" "$(printf '%s' "$out" | sed '$d')"
done < "$WORK/targets"
for c in "${!tally[@]}"; do printf '  HTTP %s  x%s\n' "$c" "${tally[$c]}"; done

# Each request is an outbox row; the answer is two hops away (account-service, then the gateway).
echo; echo "Waiting for the participants to answer"
deadline=$((SECONDS + ${RECONCILE_TIMEOUT:-60}))
while :; do
    left=$(( $(stranded_s1 | wc -l) + $(stranded_s2 | wc -l) ))
    [ "$left" -eq 0 ] && break
    [ $SECONDS -lt $deadline ] || break
    sleep 2
done

echo; echo "Stranded money after:"
summarise
remaining=$(( $(wc -l < "$WORK/s1") + $(wc -l < "$WORK/s2") ))
if [ "$remaining" -ne 0 ]; then
    echo
    echo "$remaining still stranded. Their Reconcile steps:"
    sort -u "$WORK/s1" "$WORK/s2" | sed '/^$/d' | head -20 > "$WORK/left"
    _stranded_sql payments_db "SELECT s.transfer_id, st.outcome, COALESCE(st.detail, '')
                                 FROM saga_steps st JOIN saga_instances s ON s.id = st.saga_id
                                WHERE st.step_name = 'Reconcile'
                                  AND s.transfer_id IN ($(sed "s/.*/'&'/" "$WORK/left" | paste -sd, -))
                                ORDER BY st.created_at" | sed 's/^/  /'
    echo "A Reconcile step that FAILED means the hold was COMMITTED - the recipient has the money -"
    echo "and that transfer needs a person, not a compensation. One with only STARTED has not been"
    echo "answered yet: check consumer lag, then run this again."
    exit 1
fi
exit 0
