import type {
  AccountResponse,
  TransferAccepted,
  CreatedTransfer,
  DepthResponse,
  InvariantsResponse,
  LedgerPage,
  PromSample,
  SimulationUpdate,
  SimulationView,
  TimelineResponse,
  TokenResponse,
  TransferPage,
} from "./types";

/**
 * Every call the console makes, in one file.
 *
 * The prefixes are the nginx routes (see ui/nginx.conf), which the Vite dev server mirrors. No
 * component anywhere constructs a URL, so there is exactly one place that knows a service moved.
 */
const ORCHESTRATOR = "/api/orchestrator";
const ACCOUNTS = "/api/accounts";
const GATEWAY = "/api/gateway";
const PROM = "/api/prom";

/**
 * A non-2xx response, carrying the status so callers can branch on it.
 *
 * The status genuinely matters here in a way it does not in most UIs, because this API uses three
 * codes to mean three different things and the console has to respect all three:
 *
 *   401 - the token is missing, expired or invalid. Recoverable by logging in again.
 *   403 - the token is fine and this subject may not do this. Logging in again changes nothing.
 *   404 - on a read path, "no such transfer OR not yours". Deliberately indistinguishable, so the
 *         console must not render it as "not found" with any more confidence than the server had.
 *
 * Collapsing them into one "request failed" would erase a security decision the backend spent M5
 * making.
 */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly url: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }

  /** True when re-authenticating could plausibly help. 403 is not one of these. */
  get isAuthExpiry(): boolean {
    return this.status === 401;
  }
}

/** How the caller supplies credentials. `null` sends no Authorization header at all. */
export type TokenSource = () => string | null;

interface RequestOptions {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
  token: TokenSource;
}

async function request<T>(url: string, options: RequestOptions): Promise<T> {
  const token = options.token();

  const headers: Record<string, string> = { ...options.headers };
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const response = await fetch(url, {
    method: options.method ?? "GET",
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });

  if (!response.ok) {
    // The body is read as text and not parsed, because an error body is not guaranteed to be
    // JSON - a 403 from the Spring Security filter chain has an empty body, and nginx's own 502
    // is HTML. Trying to parse it would replace a useful status code with a parse error.
    const detail = await response.text().catch(() => "");
    throw new ApiError(
      response.status,
      url,
      detail.slice(0, 400) || `${response.status} ${response.statusText}`,
    );
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

// -----------------------------------------------------------------------------------------------
// Auth
// -----------------------------------------------------------------------------------------------

/**
 * Log in. The only call in this file that sends no token, because the endpoint that issues tokens
 * cannot require one.
 */
export function login(username: string, password: string): Promise<TokenResponse> {
  return request<TokenResponse>(`${ORCHESTRATOR}/auth/token`, {
    method: "POST",
    body: { username, password },
    token: () => null,
  });
}

// -----------------------------------------------------------------------------------------------
// Transfers (orchestrator, USER role)
// -----------------------------------------------------------------------------------------------

/**
 * Start a transfer.
 *
 * `Idempotency-Key` is required by the server and generated per ATTEMPT by the caller, not per
 * render - see the note in NewTransfer.tsx for why that distinction is the whole point of the
 * header. There is no `X-Client-Id`: M5 moved the idempotency namespace from a caller-supplied
 * header to the token's subject, precisely so one client could not claim another's key and be
 * handed their receipt.
 */
export async function createTransfer(
  token: TokenSource,
  idempotencyKey: string,
  body: { fromAccountId: string; toAccountId: string; amountMinor: number; currency: string },
): Promise<TransferAccepted> {
  const url = `${ORCHESTRATOR}/api/v1/transfers`;
  const bearer = token();

  // This one call does not go through `request`, because what it needs is in a HEADER. The body
  // of a replay is the first request's response byte for byte and therefore cannot say that it is
  // a replay; `Idempotency-Replayed` is the only place that fact exists. A generic JSON helper
  // that returns `response.json()` throws it away.
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new ApiError(response.status, url, detail.slice(0, 400) || `${response.status} ${response.statusText}`);
  }

  return {
    status: response.status,
    replayed: response.headers.get("Idempotency-Replayed") === "true",
    transfer: (await response.json()) as CreatedTransfer,
  };
}

export function listTransfers(
  token: TokenSource,
  cursor?: string | null,
  size?: number,
): Promise<TransferPage> {
  const params = new URLSearchParams();
  if (cursor) params.set("cursor", cursor);
  if (size) params.set("size", String(size));
  const query = params.toString();
  return request<TransferPage>(`${ORCHESTRATOR}/api/v1/transfers${query ? `?${query}` : ""}`, {
    token,
  });
}

export function getTimeline(token: TokenSource, transferId: string): Promise<TimelineResponse> {
  return request<TimelineResponse>(
    `${ORCHESTRATOR}/api/v1/transfers/${encodeURIComponent(transferId)}/timeline`,
    { token },
  );
}

// -----------------------------------------------------------------------------------------------
// Accounts (account-service)
// -----------------------------------------------------------------------------------------------

export function getAccount(token: TokenSource, accountId: string): Promise<AccountResponse> {
  return request<AccountResponse>(`${ACCOUNTS}/accounts/${encodeURIComponent(accountId)}`, {
    token,
  });
}

export function getLedger(
  token: TokenSource,
  accountId: string,
  cursor?: string | null,
  size?: number,
): Promise<LedgerPage> {
  const params = new URLSearchParams();
  if (cursor) params.set("cursor", cursor);
  if (size) params.set("size", String(size));
  const query = params.toString();
  return request<LedgerPage>(
    `${ACCOUNTS}/accounts/${encodeURIComponent(accountId)}/ledger${query ? `?${query}` : ""}`,
    { token },
  );
}

// -----------------------------------------------------------------------------------------------
// Operational surfaces (OPERATOR role)
// -----------------------------------------------------------------------------------------------

/**
 * The two invariants endpoints, fetched separately and joined here.
 *
 * This function is the console's whole reason for existing on the System view. No service can
 * answer all five invariants, because I1/I2/I3/I5 are statements about accounts_db and I4 is a
 * statement about payments_db - and a service holding credentials to both would be a shared
 * database reintroduced through the monitoring door. So each answers for its own database and the
 * join happens in a browser, which holds no credentials at all beyond the operator's own token.
 *
 * `Promise.allSettled`, not `Promise.all`: if account-service is down, the orchestrator's answer
 * is still worth showing, and an all-or-nothing fetch would blank the panel that would have told
 * an operator which half was broken.
 */
export async function getInvariants(
  token: TokenSource,
): Promise<{ orchestrator: SettledResult<InvariantsResponse>; accounts: SettledResult<InvariantsResponse> }> {
  const [orchestrator, accounts] = await Promise.allSettled([
    request<InvariantsResponse>(`${ORCHESTRATOR}/admin/invariants`, { token }),
    request<InvariantsResponse>(`${ACCOUNTS}/admin/invariants`, { token }),
  ]);
  return { orchestrator: settle(orchestrator), accounts: settle(accounts) };
}

export type SettledResult<T> = { ok: true; value: T } | { ok: false; error: Error };

function settle<T>(result: PromiseSettledResult<T>): SettledResult<T> {
  return result.status === "fulfilled"
    ? { ok: true, value: result.value }
    : { ok: false, error: result.reason instanceof Error ? result.reason : new Error(String(result.reason)) };
}

/** Dead letter depth for one service. Each service owns its own dead_letters table. */
export function getDeadLetterDepth(token: TokenSource, service: "orchestrator" | "accounts" | "gateway") {
  const base = service === "orchestrator" ? ORCHESTRATOR : service === "accounts" ? ACCOUNTS : GATEWAY;
  return request<DepthResponse>(`${base}/admin/dead-letters/depth`, { token });
}

// -----------------------------------------------------------------------------------------------
// Gateway simulation (OPERATOR role)
// -----------------------------------------------------------------------------------------------

export function getSimulation(token: TokenSource): Promise<SimulationView> {
  return request<SimulationView>(`${GATEWAY}/admin/simulation`, { token });
}

export function updateSimulation(
  token: TokenSource,
  update: SimulationUpdate,
): Promise<SimulationView> {
  return request<SimulationView>(`${GATEWAY}/admin/simulation`, {
    method: "POST",
    body: update,
    token,
  });
}

// -----------------------------------------------------------------------------------------------
// Prometheus
// -----------------------------------------------------------------------------------------------

interface PromQueryResponse {
  status: string;
  data: { resultType: string; result: PromSample[] };
}

/**
 * One instant query.
 *
 * No token: Prometheus has no authentication and never has - see the note on this route in
 * nginx.conf for why that is an accepted, bounded decision rather than an oversight.
 *
 * A failed query returns an empty array rather than throwing. This is the one place in the file
 * where swallowing an error is right: the metrics panels are decoration over the invariants
 * panels, and the console must stay usable when the observability stack is the thing that is
 * down. That is the same rule the backend holds itself to - tracing and Redis are not allowed to
 * be load-bearing, and neither is this.
 */
export async function promQuery(query: string): Promise<PromSample[]> {
  try {
    const url = `${PROM}/api/v1/query?query=${encodeURIComponent(query)}`;
    const response = await fetch(url);
    if (!response.ok) return [];
    const body = (await response.json()) as PromQueryResponse;
    return body.status === "success" ? body.data.result : [];
  } catch {
    return [];
  }
}

/** Convenience for a query that yields at most one number. */
export async function promScalar(query: string): Promise<number | null> {
  const samples = await promQuery(query);
  if (samples.length === 0) return null;
  const parsed = Number(samples[0].value[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Alerts currently firing, from the synthetic ALERTS series the rules in alerts.yml publish. */
export async function promFiringAlerts(): Promise<PromSample[]> {
  return promQuery('ALERTS{alertstate="firing"}');
}
