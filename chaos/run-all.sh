#!/usr/bin/env bash
#
# Runs every scenario in order and prints one line per result. Exits non-zero if any hypothesis
# was refuted.
#
# Sequential on purpose. Each scenario refuses to start until the system is quiescent, so running
# two at once would make each one's verdict a statement about both.
#
# A refuted hypothesis does not stop the run - but a scenario that leaves the system NON-quiescent
# (a saga stuck for good) makes every scenario after it refuse to start, and the summary says so.
# That is the correct behaviour: the next result would be about two faults at once.

cd "$(dirname "$0")" || exit 2

runs=(
    "01-broker-dies.sh"
    "02-account-service-dies.sh"
    "WHEN=before-reserve 02-account-service-dies.sh"
    "03-orchestrator-dies-before-relay.sh"
    "04-gateway-declines-everything.sh"
    "05-gateway-timeouts-and-duplicates.sh"
    "06-idempotency-key-stampede.sh"
    "REDIS=off 06-idempotency-key-stampede.sh"
    "07-hot-account.sh"
    "08-network-partition.sh"
    "MODE=crash 08-network-partition.sh"
)

declare -a summary
refuted=0
for run in "${runs[@]}"; do
    script="${run##* }"
    envs="${run% *}"; [ "$envs" = "$run" ] && envs=""
    env $envs bash "./$script"
    case $? in
        0) summary+=("HELD      $run") ;;
        1) summary+=("REFUTED   $run"); refuted=$((refuted + 1)) ;;
        *) summary+=("NOT RUN   $run   (refused to start - see above)"); refuted=$((refuted + 1)) ;;
    esac
done

echo
echo "=================== chaos suite ==================="
printf '%s\n' "${summary[@]}"
exit $(( refuted > 0 ))
