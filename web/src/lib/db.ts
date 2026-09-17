import { Pool, type PoolClient } from "pg";
import { SCHEMA_SQL } from "./schema";

/**
 * One pool per warm serverless instance.
 *
 * `max` is deliberately small. A serverless platform will happily run many instances of this
 * function at once, and each one holding a fat pool is how a small Postgres runs out of
 * connections while every instance believes it is being modest. Point DATABASE_URL at a pooled
 * (pgbouncer) endpoint and this stays cheap.
 */
declare global {
  // eslint-disable-next-line no-var
  var __dpePool: Pool | undefined;
  // eslint-disable-next-line no-var
  var __dpeMigrated: Promise<void> | undefined;
}

export function pool(): Pool {
  if (!global.__dpePool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error(
        "DATABASE_URL is not set. Copy web/.env.example to web/.env.local and point it at a Postgres."
      );
    }
    global.__dpePool = new Pool({
      connectionString,
      max: 3,
      idleTimeoutMillis: 10_000,
      connectionTimeoutMillis: 10_000,
      // Neon, Supabase and most hosted Postgres require TLS; they present a chain Node does not
      // ship a root for, which is what `rejectUnauthorized: false` is about here.
      ssl: connectionString.includes("sslmode=disable")
        ? false
        : { rejectUnauthorized: false },
    });
  }
  return global.__dpePool;
}

/** Applied once per warm instance. Every statement is IF NOT EXISTS, so it is safe to repeat. */
export function migrate(): Promise<void> {
  if (!global.__dpeMigrated) {
    global.__dpeMigrated = pool()
      .query(SCHEMA_SQL)
      .then(() => undefined)
      .catch((e) => {
        // Never cache a failed migration - the next request should try again.
        global.__dpeMigrated = undefined;
        throw e;
      });
  }
  return global.__dpeMigrated;
}

export async function query<T = any>(sql: string, params: any[] = []): Promise<T[]> {
  const res = await pool().query(sql, params);
  return res.rows as T[];
}

/**
 * Run a function inside ONE database transaction.
 *
 * This is the whole point of the exercise. The business state, the inbox row and the outbox row
 * are written together or not at all; there is no path that writes to the database and publishes
 * a message as two separate operations.
 */
export async function tx<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool().connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {
      /* the connection is already gone; the transaction died with it */
    }
    throw e;
  } finally {
    client.release();
  }
}

/** Postgres's unique-violation SQLSTATE. A duplicate is an expected outcome here, not an error. */
export const UNIQUE_VIOLATION = "23505";
export function isUniqueViolation(e: unknown): boolean {
  return typeof e === "object" && e !== null && (e as { code?: string }).code === UNIQUE_VIOLATION;
}
