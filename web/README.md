# The project page, and a second implementation of the engine

A Next.js app that serves the project page **and** runs a working implementation of the same
payment engine against a real PostgreSQL database. Visitors send real payments, which write real
ledger rows, and the invariants shown on the page are SQL queries over those rows rather than a
verdict decided in advance.

It exists because a static page can assert that a compensation returns the sender exactly whole,
and only a running system can show it.

## What is the same as the Java system

| | |
|---|---|
| The saga | `STARTED → RESERVED → CHARGED → COMPLETED`, with `COMPENSATING → COMPENSATED` on a decline or a pre-pivot timeout, and `FAILED` directly on a rejected reserve |
| The ledger | Double-entry, signed amounts, money as a whole number of paise, every constraint from `V1__ledger_core.sql` |
| Clearing | Sharded across eight accounts; the hold records its shard and commit/release read it back rather than recomputing it |
| The outbox | Messages written in the transaction that caused them, published separately |
| The inbox | Claimed in the same transaction as the work, so a duplicate delivery is absorbed |
| Idempotency | A unique index, claimed in the same transaction as the payment it guards |
| Lock ordering | Accounts locked in one global id order, so deadlock is structurally impossible |
| The invariants | I1–I5 and S1–S4, as queries, with I4 and S1 reported as "not yet" until the system is at rest |

## What is different, and why

Three services became one process and Kafka became a table, because a serverless platform has
nowhere to put a long-running broker or a background relay thread. The page being watched turns
the crank that a relay thread turns in the deployed system, by polling `POST /api/tick`.

That changes *who invokes the loop*, not what the loop guarantees. The message is still a
committed row published separately from the transaction that wrote it, delivery is still
at-least-once, and the consumer still claims its inbox row inside the transaction that does the
work — which is why every fault switch on the page can still reach a real failure mode.

One constraint was deliberately dropped: `saga_compensation_needs_a_hold`. A timeout in `STARTED`
sends a transfer-addressed compensation before any hold exists, and here that path is reachable
from the UI.

Every visitor gets an isolated world keyed by a session cookie, so one person setting the gateway
to decline everything cannot break somebody else's payment, and the invariants are a statement
about one visitor's ledger rather than a sum over strangers. Worlds untouched for a day are swept.

## API

| | |
|---|---|
| `GET /api/state` | Everything the console renders |
| `POST /api/transfers` | Accept a payment. Honours `Idempotency-Key`; answers 202 |
| `POST /api/tick` | One round of the relay and its consumers, plus the timeout sweeper |
| `GET /api/invariants` | I1–I5 and S1–S4; 200 when they all hold, 409 when one does not |
| `POST /api/faults` | The runtime fault switches |
| `POST /api/reset` | Wipe and reopen this visitor's world |

## Running it locally

Needs any PostgreSQL. A throwaway one in Docker will do:

```bash
docker run -d --name dpe-web-testdb -e POSTGRES_PASSWORD=test -e POSTGRES_DB=dpe \
  -p 55432:5432 postgres:16-alpine

cp .env.example .env.local
# DATABASE_URL=postgresql://postgres:test@127.0.0.1:55432/dpe?sslmode=disable

npm install
npm run dev
```

The schema applies itself on first request; every statement is `IF NOT EXISTS`, so it is safe to
repeat and needs no migration step.

## Deploying

Any host that runs Next.js and can reach a Postgres. On Vercel, set the project's **Root
Directory** to `web` and provide `DATABASE_URL`. A pooled (pgbouncer) connection string is the
right one to use — a serverless platform opens many short-lived connections, and the pool here is
deliberately capped at three per instance for the same reason.
