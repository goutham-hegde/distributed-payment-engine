/**
 * The wire shapes, transcribed from the Java records that produce them.
 *
 * These are hand-written rather than generated, which is a real trade-off and worth naming: there
 * is no compiler anywhere that checks this file against `TransferSummary.java`. A field renamed on
 * the server is a `undefined` here, not a build failure. The mitigation is that every one of these
 * types names the record it mirrors, so a change on either side has a findable counterpart - and
 * the reason not to generate them from an OpenAPI document is that the document would then need
 * generating too, and this repository does not have one yet. If M9 adds one, these go.
 *
 * Money is `number` because JSON has one number type, and that is safe HERE for a reason worth
 * knowing rather than assuming: amounts are BIGINT minor units on the server, and JavaScript
 * integers are exact to 2^53 - about 90 trillion rupees in paise. It is exact for every value this
 * system will hold, and it would NOT be if the column were nanoseconds or a hash. Formatting
 * happens at the edge (see format.ts); arithmetic on these values happens on the server.
 */

/** Mirrors AuthController.TokenResponse. */
export interface TokenResponse {
  accessToken: string;
  tokenType: string;
  expiresInSeconds: number;
  username: string;
  roles: string[];
}

/** Mirrors orchestrator TransferSummary. */
export interface TransferSummary {
  transferId: string;
  fromAccountId: string;
  toAccountId: string;
  amountMinor: number;
  currency: string;
  status: string;
  failureReason: string | null;
  createdAt: string;
  updatedAt: string;
}

/**
 * Mirrors the ORCHESTRATOR's TransferResponse - the body of a successful POST.
 *
 * Not to be confused with account-service's record of the same name, which is a different shape
 * for a different endpoint. This one carries `sagaStatus` alongside the transfer's own status,
 * because at 202 the transfer has been accepted and the saga has only just started: two states,
 * genuinely distinct, and collapsing them in the UI would be the console telling a story the
 * system is not yet in a position to tell.
 */
export interface CreatedTransfer {
  transferId: string;
  fromAccountId: string;
  toAccountId: string;
  amountMinor: number;
  currency: string;
  status: string;
  failureReason: string | null;
  sagaStatus: string | null;
  /**
   * Null on the POST response, and that is not a bug to work around.
   *
   * The body is serialized from the entity inside the transaction that created it, before the
   * database-generated `created_at` has been read back. The transfer has a creation time - it just
   * is not in this response. The list and the timeline both carry it, so nothing needs it here;
   * typing it as non-null would have been a lie the compiler enforced.
   */
  createdAt: string | null;
}

/**
 * A POST /transfers result, with the thing the body cannot say.
 *
 * `replayed` comes from the `Idempotency-Replayed` response header, not from the body - because
 * the body of a replay is the FIRST request's response, byte for byte, and therefore cannot
 * mention that it is a replay. Surfacing it is the cheapest demonstration in the console that the
 * idempotency gate is real: press the button twice with one key and the second press returns the
 * first answer with this flag set, having written nothing.
 */
export interface TransferAccepted {
  status: number;
  replayed: boolean;
  transfer: CreatedTransfer;
}

/** Mirrors orchestrator TransferPage. */
export interface TransferPage {
  items: TransferSummary[];
  nextCursor: string | null;
}

/** Mirrors TimelineResponse.Outbound - the outbox row behind a stage. */
export interface Outbound {
  messageId: string;
  topic: string;
  eventType: string;
  queuedAt: string;
  publishedAt: string | null;
  relayLagMs: number | null;
}

/** Mirrors TimelineResponse.Inbound - the inbox row in the receiving service. */
export interface Inbound {
  messageId: string;
  topic: string;
  eventType: string;
  receivedAt: string;
}

/** Mirrors TimelineResponse.Stage. */
export interface Stage {
  name: string;
  outcome: string;
  toStatus: string | null;
  startedAt: string;
  endedAt: string | null;
  latencyMs: number | null;
  detail: string | null;
  command: Outbound | null;
  reply: Inbound | null;
}

/** Mirrors TimelineResponse.SagaView. */
export interface SagaView {
  sagaId: string;
  status: string;
  holdId: string | null;
  gatewayChargeId: string | null;
  failureReason: string | null;
  deadlineAt: string | null;
  timedOutAt: string | null;
  completedAt: string | null;
  createdAt: string;
}

/** Mirrors TimelineResponse. */
export interface TimelineResponse {
  transfer: TransferSummary;
  saga: SagaView;
  traceId: string | null;
  stages: Stage[];
}

/** Mirrors account-service AccountResponse. */
export interface AccountResponse {
  id: string;
  ownerId: string;
  currency: string;
  balanceMinor: number;
}

/** Mirrors account-service LedgerEntryResponse. */
export interface LedgerEntryResponse {
  id: number;
  transferId: string | null;
  accountId: string;
  amountMinor: number;
  entryType: string;
  currency: string;
  createdAt: string;
}

/** Mirrors account-service LedgerPage. */
export interface LedgerPage {
  accountId: string;
  balanceMinor: number;
  currency: string;
  entries: LedgerEntryResponse[];
  nextCursor: string | null;
}

/**
 * Mirrors InvariantCheck, which both services return.
 *
 * `requiresQuiescence` is the field that must not be dropped when rendering. A check carrying it
 * is only meaningful once the system is at rest - I4 counts sagas in flight, and sagas in flight
 * are what a working system under load looks like. Painting that red would teach an operator to
 * ignore the one panel that claims to prove something.
 */
export interface InvariantCheck {
  id: string;
  title: string;
  holds: boolean;
  detail: string;
  requiresQuiescence: boolean;
}

/** Mirrors account-service InvariantsController.Conservation - the I3 total, not a verdict. */
export interface Conservation {
  customerBalanceMinor: number;
  activeHoldsMinor: number;
  totalMinor: number;
}

/**
 * Mirrors both InvariantsController.InvariantsResponse records. They differ in their last field -
 * the orchestrator returns `inFlight`, account-service returns `conservation` - which is why this
 * has both, optional. One endpoint per database is the whole design: no service holds credentials
 * to both, so the join happens here, in the console.
 */
export interface InvariantsResponse {
  service: string;
  database: string;
  checks: InvariantCheck[];
  inFlight?: Record<string, number>;
  conservation?: Conservation;
}

/** Mirrors SimulationController.SimulationView. */
export interface SimulationView {
  failureRate: number;
  latencyMs: number;
  timeoutRate: number;
  duplicateCallbackRate: number;
}

/** Mirrors SimulationController.SimulationUpdate - every field optional, null means "leave it". */
export type SimulationUpdate = Partial<SimulationView>;

/** Mirrors DeadLetterController.DepthResponse. */
export interface DepthResponse {
  pending: number;
}

/** One `[timestamp, value]` sample from Prometheus's instant query API. */
export interface PromSample {
  metric: Record<string, string>;
  value: [number, string];
}
