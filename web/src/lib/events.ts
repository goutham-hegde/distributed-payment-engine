import type { PoolClient } from "pg";
import { randomUUID } from "crypto";

/**
 * Topics are a schema. In the deployed system each is declared by every service that touches it,
 * with an explicit partition count, because a consumer that subscribes to an undeclared topic has
 * the broker auto-create it with one partition and then silently never sees most of the traffic.
 * Here they carry no routing weight, but they are kept because they are how the message log reads.
 */
export const TOPICS = {
  ACCOUNT_COMMANDS: "dpe.account.commands.v1",
  ACCOUNT_EVENTS: "dpe.account.events.v1",
  GATEWAY_COMMANDS: "dpe.gateway.commands.v1",
  GATEWAY_EVENTS: "dpe.gateway.events.v1",
} as const;

export type EventType =
  | "ReserveFunds"
  | "FundsReserved"
  | "ReserveRejected"
  | "ChargeGateway"
  | "GatewayApproved"
  | "GatewayDeclined"
  | "CommitFunds"
  | "FundsCommitted"
  | "ReleaseFunds"
  | "FundsReleased"
  | "VoidCharge"
  | "ChargeVoided";

/** Which side of the conversation an event is, for the console's message log. */
export const DIRECTION: Record<EventType, { from: string; to: string }> = {
  ReserveFunds: { from: "orchestrator", to: "account" },
  FundsReserved: { from: "account", to: "orchestrator" },
  ReserveRejected: { from: "account", to: "orchestrator" },
  CommitFunds: { from: "orchestrator", to: "account" },
  FundsCommitted: { from: "account", to: "orchestrator" },
  ReleaseFunds: { from: "orchestrator", to: "account" },
  FundsReleased: { from: "account", to: "orchestrator" },
  ChargeGateway: { from: "orchestrator", to: "gateway" },
  GatewayApproved: { from: "gateway", to: "orchestrator" },
  GatewayDeclined: { from: "gateway", to: "orchestrator" },
  VoidCharge: { from: "orchestrator", to: "gateway" },
  ChargeVoided: { from: "gateway", to: "orchestrator" },
};

const TOPIC_OF: Record<EventType, string> = {
  ReserveFunds: TOPICS.ACCOUNT_COMMANDS,
  CommitFunds: TOPICS.ACCOUNT_COMMANDS,
  ReleaseFunds: TOPICS.ACCOUNT_COMMANDS,
  FundsReserved: TOPICS.ACCOUNT_EVENTS,
  ReserveRejected: TOPICS.ACCOUNT_EVENTS,
  FundsCommitted: TOPICS.ACCOUNT_EVENTS,
  FundsReleased: TOPICS.ACCOUNT_EVENTS,
  ChargeGateway: TOPICS.GATEWAY_COMMANDS,
  VoidCharge: TOPICS.GATEWAY_COMMANDS,
  GatewayApproved: TOPICS.GATEWAY_EVENTS,
  GatewayDeclined: TOPICS.GATEWAY_EVENTS,
  ChargeVoided: TOPICS.GATEWAY_EVENTS,
};

/**
 * Writes a message to the outbox, in whatever transaction the caller is already in.
 *
 * This is the only way a message is ever produced. There is no code path that writes to the
 * database and publishes in two operations, which is the dual-write bug the pattern exists to
 * prevent: the row and the state change it reports commit together or neither does.
 */
export async function emit(
  c: PoolClient,
  sessionId: string,
  aggregateId: string,
  eventType: EventType,
  payload: Record<string, unknown>
): Promise<string> {
  const id = randomUUID();
  await c.query(
    `INSERT INTO outbox (id, session_id, aggregate_id, topic, event_type, payload)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [id, sessionId, aggregateId, TOPIC_OF[eventType], eventType, JSON.stringify(payload)]
  );
  return id;
}

/** One thing that happened to a saga. Append-only, like the ledger. */
export async function recordStep(
  c: PoolClient,
  sessionId: string,
  sagaId: string,
  stepName: string,
  outcome: "STARTED" | "SUCCEEDED" | "FAILED" | "SKIPPED",
  toStatus: string | null,
  messageId: string | null,
  detail: string | null
): Promise<void> {
  await c.query(
    `INSERT INTO saga_steps (session_id, saga_id, step_name, outcome, to_status, message_id, detail)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [sessionId, sagaId, stepName, outcome, toStatus, messageId, detail]
  );
}

/**
 * Which clearing shard a transfer uses.
 *
 * Picked once, at the reserve, and then RECORDED on the hold. Commit and release read it back
 * from the hold rather than recomputing it: a recomputed shard would differ the day a shard is
 * added, splitting one transfer's legs across two accounts and quietly ending the guarantee that
 * completing and compensating are mutually exclusive.
 */
export function shardFor(transferId: string, shards: number): number {
  let h = 0;
  for (let i = 0; i < transferId.length; i++) {
    h = (Math.imul(h, 31) + transferId.charCodeAt(i)) | 0;
  }
  return Math.abs(h) % shards;
}
