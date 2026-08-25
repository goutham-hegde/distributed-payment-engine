#!/bin/bash
#
# Runs once, on first initialisation of the Postgres data volume.
#
# Database-per-service: each service gets its own database and its own role, and
# no role can read another's data. That enforces the service boundary at the
# database level - a service physically cannot join across it, which is the
# discipline that makes the boundary real rather than a naming convention.
#
# In production these would be separate clusters. One container with three
# databases is a deliberate concession to running the whole stack on a laptop;
# the isolation property that matters here is preserved.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER payments WITH PASSWORD 'payments';
    CREATE DATABASE payments_db OWNER payments;

    CREATE USER accounts WITH PASSWORD 'accounts';
    CREATE DATABASE accounts_db OWNER accounts;

    CREATE USER gateway  WITH PASSWORD 'gateway';
    CREATE DATABASE gateway_db  OWNER gateway;
EOSQL

for db_user in "payments_db:payments" "accounts_db:accounts" "gateway_db:gateway"; do
    db="${db_user%%:*}"
    usr="${db_user##*:}"

    # Postgres grants CONNECT on every database to PUBLIC by default, so owning a
    # database does NOT keep other roles out - any role could connect and read.
    # Revoking PUBLIC and granting back only the owner is what actually enforces
    # the service boundary.
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
        REVOKE CONNECT ON DATABASE ${db} FROM PUBLIC;
        GRANT  CONNECT ON DATABASE ${db} TO ${usr};
EOSQL

    # The owner needs rights on its own public schema (Postgres 15+ revoked the
    # implicit CREATE grant on public from non-owners).
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-EOSQL
        GRANT ALL ON SCHEMA public TO ${usr};
        ALTER SCHEMA public OWNER TO ${usr};
EOSQL
done

echo "initialised: payments_db, accounts_db, gateway_db"
