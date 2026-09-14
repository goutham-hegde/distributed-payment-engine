#!/usr/bin/env bash
#
# Obtain a bearer token from the running stack.
#
#   ./scripts/token.sh                        # alice, the default demo customer
#   ./scripts/token.sh operator               # an operator token
#   ./scripts/token.sh alice alice-password   # explicit credentials
#
# Prints the raw token to stdout and nothing else, so it composes:
#
#   TOKEN=$(./scripts/token.sh)
#   curl -H "Authorization: Bearer $TOKEN" ...
#
# It exists because M7's chaos scenarios and M8's k6 run both have to authenticate now, and a
# demo whose first step is "paste a JWT from somewhere" is a demo nobody runs twice.
#
# The credentials below are the development directory from application.yml. They are fake, they
# are in the repository on purpose, and they authenticate against a token endpoint that stands in
# for a real identity provider - see docs/adr/0005-jwt-authentication.md.

set -euo pipefail

USER_NAME="${1:-alice}"
PASSWORD="${2:-}"

if [[ -z "$PASSWORD" ]]; then
  case "$USER_NAME" in
    alice)    PASSWORD="alice-password" ;;
    bob)      PASSWORD="bob-password" ;;
    operator) PASSWORD="operator-password" ;;
    *)
      echo "no default password for '$USER_NAME' - pass one as the second argument" >&2
      exit 2
      ;;
  esac
fi

ORCHESTRATOR="${ORCHESTRATOR_URL:-http://localhost:8081}"

RESPONSE=$(curl -sS -w '\n%{http_code}' \
  -X POST "$ORCHESTRATOR/auth/token" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$USER_NAME\",\"password\":\"$PASSWORD\"}")

STATUS=$(printf '%s' "$RESPONSE" | tail -n1)
BODY=$(printf '%s' "$RESPONSE" | sed '$d')

if [[ "$STATUS" != "200" ]]; then
  # The endpoint answers the same 401 for a wrong password and an unknown user, deliberately -
  # a login that distinguishes them tells an attacker which accounts exist. So this message
  # cannot be more specific than the server was.
  echo "token request failed (HTTP $STATUS) for user '$USER_NAME'" >&2
  exit 1
fi

# python rather than jq: jq is not installed everywhere, and the JVM toolchain this repo already
# needs does not include it either. Falls back to sed if python is missing too.
if command -v python3 >/dev/null 2>&1; then
  printf '%s' "$BODY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])'
else
  printf '%s' "$BODY" | sed -n 's/.*"accessToken":"\([^"]*\)".*/\1/p'
fi
