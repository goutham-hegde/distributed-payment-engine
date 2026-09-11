// The M8 load test: customers paying each other, measured two ways.
//
// Run through ./loadtest/run.sh, never directly - the runner opens the accounts this script spends
// from, records the I3 baseline AFTER opening them, and judges the run on the invariants once the
// system is at rest again. A load test that only reads its own HTTP statuses measures the API
// edge, and in this system the API edge is the part that cannot fail: it answers 202 as soon as
// the transfer, the saga and the ReserveFunds outbox row are committed, before any money has moved.
//
// So every iteration is what a customer does, not what a benchmark does:
//
//   POST the transfer          -> http_req_duration{op:create}   the EDGE: accept latency
//   poll until it settles      -> transfer_settle_ms             the SYSTEM: what the customer waits
//   think                      -> only in the `users` profile
//
// The two numbers diverge exactly when the asynchronous half falls behind, and that divergence is
// the thing this milestone exists to find. The runner reads the precise version of the second one
// out of saga_instances afterwards (polling measures it to the nearest POLL_INTERVAL); the k6 one
// is the customer's view, including the polling itself.
//
// PROFILES (PROFILE=...):
//
//   smoke   5 users, 30 s. Does the harness work at all.
//
//   knee    OPEN model: a stepped ARRIVAL RATE (STAGES="rate:seconds,..."). New customers arrive on a
//           schedule whether or not the last ones have been served, which is how a payment API is
//           actually loaded - and the only model that measures latency honestly, because a closed
//           model slows its own arrivals down when the system slows down (coordinated omission).
//           Its job is to find the throughput at which settle time stops being flat.
//
//   users   CLOSED model: VUS concurrent customers (default 1,000), each paying, waiting for the
//           payment to settle, then thinking THINK_MIN..THINK_MAX seconds. This is the milestone's
//           "1,000 concurrent" and it is a statement about SESSIONS, not throughput: Little's law
//           turns it into an offered rate of about VUS / (think + settle) per second. With the
//           default 30-90 s think time that is ~16/s; with THINK_MIN=5 THINK_MAX=15 it is ~100/s.
//           Same thousand users, a different system - which is why the think time is printed.
//
// Metric names carry `name` tags rather than raw URLs: a transfer id in a URL is one time series
// per transfer, which is the same cardinality mistake in k6 that it is in Prometheus.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE = __ENV.ORCHESTRATOR_URL || 'http://payment-orchestrator:8081';
const PROFILE = __ENV.PROFILE || 'smoke';

const num = (name, dflt) => (__ENV[name] === undefined || __ENV[name] === '' ? dflt : Number(__ENV[name]));

const VUS = num('VUS', 1000);
const MAX_VUS = num('MAX_VUS', 1000);
const RAMP = __ENV.RAMP || '3m';
const HOLD = __ENV.HOLD || '5m';
const THINK_MIN = num('THINK_MIN', PROFILE === 'smoke' ? 1 : 30);
const THINK_MAX = num('THINK_MAX', PROFILE === 'smoke' ? 2 : 90);
// Rate steps for `knee`, "rate:seconds" pairs. Brackets the ceiling predicted from the code before
// the first run - see learning.md, Session 20 - rather than starting from a number chosen for
// looking good.
const STAGES = __ENV.STAGES || '5:60,10:60,15:60,20:60,30:60,40:60';
// Fraction of accepted transfers re-sent with the SAME Idempotency-Key, as a client retrying a
// request whose response it never saw. Each must be answered with the original transfer id.
const RETRY_RATE = num('RETRY_RATE', 0.02);
// Must outlast the saga's step-timeout (30 s) plus a sweep (5 s): a transfer the sweeper
// compensates still settles, and giving up before it does would count a correct FAILED as a hang.
const POLL_TIMEOUT_S = num('POLL_TIMEOUT', 90);
const POLL_INTERVAL_S = num('POLL_INTERVAL', 0.5);
// Tokens live 15 minutes (dpe.auth.token-ttl). Renewed well inside that, per VU.
const TOKEN_RENEW_MS = 10 * 60 * 1000;

const PASSWORDS = {
  alice: __ENV.ALICE_PASSWORD || 'alice-password',
  bob: __ENV.BOB_PASSWORD || 'bob-password',
};

// Written by run.sh: [{ owner, id }, ...]. SharedArray so a thousand VUs hold one copy, not a
// thousand.
const accounts = new SharedArray('accounts', () => JSON.parse(open('/work/accounts.json')));

// ------------------------------------------------------------------------------------ metrics

const settleMs = new Trend('transfer_settle_ms', true);
const outcomes = new Counter('transfer_outcome');
const completed = new Rate('transfer_completed');
const replayConsistent = new Rate('replay_consistent');
const refused = new Counter('transfer_refused');
// M8 admission control: a 503 AT_CAPACITY is the system declining new work, not failing it. Counted
// per refusal, and a payment still refused after MAX_ATTEMPTS is one the customer gave up on.
const atCapacity = new Counter('admission_refused');
const abandoned = new Rate('payment_abandoned');

// How many times a customer asks before giving up on a payment the system keeps declining.
const MAX_ATTEMPTS = num('MAX_ATTEMPTS', 5);

// 503 is an ANSWER here, not an error: admission control (POST) and the bulkhead (any request) both
// use it to shed load, and a load test that files shedding under http_req_failed cannot tell a
// system protecting itself from one falling over.
const CREATE_OK = http.expectedStatuses(202, 503);
const POLL_OK = http.expectedStatuses(200, 503);

// ------------------------------------------------------------------------------------ options

function parseStages(spec) {
  // Each step ramps to its rate over 10 s and then holds it, so a step's numbers are not smeared
  // by the climb into it.
  const out = [];
  for (const pair of spec.split(',')) {
    const [rate, secs] = pair.split(':').map(Number);
    out.push({ target: rate, duration: '10s' }, { target: rate, duration: `${secs}s` });
  }
  return out;
}

const scenarios = {
  smoke: {
    executor: 'constant-vus',
    vus: 5,
    duration: '30s',
  },
  knee: {
    executor: 'ramping-arrival-rate',
    startRate: 1,
    timeUnit: '1s',
    preAllocatedVUs: 50,
    // The ceiling on customers in flight at once. When settle time grows, rate x settle outgrows
    // it and k6 reports dropped_iterations - arrivals that found no VU free. That is a finding, not
    // a harness fault: it is the queue the system failed to drain.
    maxVUs: MAX_VUS,
    stages: parseStages(STAGES),
    gracefulStop: `${POLL_TIMEOUT_S + 10}s`,
  },
  users: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { duration: RAMP, target: VUS },
      { duration: HOLD, target: VUS },
      { duration: '1m', target: 0 },
    ],
    // A VU told to stop mid-iteration is a customer abandoning a payment they are waiting on.
    // Let them finish polling instead, so every transfer started is also observed settling.
    gracefulRampDown: `${POLL_TIMEOUT_S + 10}s`,
    gracefulStop: `${POLL_TIMEOUT_S + 10}s`,
  },
};

if (!scenarios[PROFILE]) {
  throw new Error(`unknown PROFILE '${PROFILE}' - one of: ${Object.keys(scenarios).join(', ')}`);
}

// Correctness thresholds apply to every profile. The latency ones are SLOs and are judged only
// where the offered load is meant to be served: `knee` exists to exceed them, so there they are
// reported and not failed.
const thresholds = {
  // A retried request answered with a different transfer is a double payment. Never acceptable.
  replay_consistent: ['rate==1'],
  // The edge refusing work. A 5xx here is a real failure; a 403 is a harness bug (the projection
  // did not have the account), and both land here.
  'http_req_failed{op:create}': ['rate<0.01'],
};
if (PROFILE !== 'knee') {
  Object.assign(thresholds, {
    'http_req_duration{op:create}': ['p(95)<250', 'p(99)<500'],
    // Below the 30 s step-timeout, with room: a settle time near it means the sweeper is racing
    // healthy sagas, and its compensations are load of their own.
    transfer_settle_ms: ['p(95)<10000', 'p(99)<20000'],
    transfer_completed: ['rate>0.99'],
    // Load the system is sized for must not be turned away. Refusals are correct under overload
    // (the knee exists to cause them); in a profile meant to be served they are a capacity miss.
    payment_abandoned: ['rate<0.01'],
  });
}

export const options = {
  scenarios: { [PROFILE]: scenarios[PROFILE] },
  thresholds,
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  // No response body is kept unless a request asks for it - at a thousand VUs the bodies are most
  // of the memory k6 uses.
  discardResponseBodies: true,
  // Resolve payment-orchestrator once, not on every request of every VU.
  dns: { ttl: '5m' },
};

// ------------------------------------------------------------------------------------ helpers

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const between = (lo, hi) => lo + Math.random() * (hi - lo);

function uuid() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') {
    return globalThis.crypto.randomUUID();
  }
  // Fallback only. An idempotency key must be unique per logical request, and Math.random is
  // enough for uniqueness at this volume; it is not enough for anything secret.
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

// Alternate VUs between the two customers in the dev directory. Each spends only from accounts it
// owns - anything else is a 403 from the ownership check, and that would be a harness bug.
const ME = () => (exec.vu.idInTest % 2 === 0 ? 'alice' : 'bob');

let mine = null;     // this VU's own accounts, computed once
let session = null;  // { token, at }

function ownAccounts(me) {
  if (mine === null) {
    mine = [];
    for (let i = 0; i < accounts.length; i++) {
      if (accounts[i].owner === me) mine.push(accounts[i].id);
    }
    if (mine.length === 0) throw new Error(`no accounts seeded for ${me}`);
  }
  return mine;
}

function tokenFor(me) {
  if (session !== null && Date.now() - session.at < TOKEN_RENEW_MS) return session.token;
  const res = http.post(`${BASE}/auth/token`, JSON.stringify({ username: me, password: PASSWORDS[me] }), {
    headers: { 'Content-Type': 'application/json' },
    responseType: 'text',
    tags: { name: 'POST /auth/token', op: 'login' },
  });
  if (res.status !== 200) {
    // Thrown, not aborted: under saturation a login can time out like anything else, and the next
    // iteration retries it. A misconfiguration shows up as every iteration failing, which is loud.
    throw new Error(`login as ${me} failed: HTTP ${res.status}`);
  }
  session = { token: res.json('accessToken'), at: Date.now() };
  return session.token;
}

// ------------------------------------------------------------------------------------ the customer

export default function () {
  const me = ME();
  const token = tokenFor(me);
  const from = pick(ownAccounts(me));
  let to = pick(accounts).id;
  while (to === from) to = pick(accounts).id;

  const key = uuid();
  const body = JSON.stringify({
    fromAccountId: from,
    toAccountId: to,
    // Small against the opening balances run.sh funds, so insufficient funds is rare and a
    // non-COMPLETED outcome means the system, not the customer's balance.
    amountMinor: 100 + Math.floor(Math.random() * 4901),
    currency: 'INR',
  });
  const headers = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    'Idempotency-Key': key,
  };

  const started = Date.now();
  let res;
  for (let attempt = 1; ; attempt++) {
    res = http.post(`${BASE}/api/v1/transfers`, body, {
      headers,
      responseType: 'text',
      responseCallback: CREATE_OK,
      tags: { name: 'POST /api/v1/transfers', op: 'create' },
    });
    if (res.status !== 503) break;
    atCapacity.add(1);
    if (attempt >= MAX_ATTEMPTS) {
      abandoned.add(true);
      think();
      return;
    }
    // What a correct client does with a 503: wait what Retry-After says PLUS jitter - without it,
    // every client refused in the same second comes back in the same second, and the refusal
    // becomes a wave - then retry with the SAME key. The refusal did not consume it, and a fresh
    // key after a lost 202 is the one way to pay twice.
    const retryAfter = Number(res.headers['Retry-After']) || 1;
    sleep(retryAfter + Math.random() * retryAfter);
  }
  abandoned.add(false);

  const accepted = check(res, { 'create: 202 Accepted': (r) => r.status === 202 });
  if (!accepted) {
    refused.add(1, { status: String(res.status) });
    think();
    return;
  }
  const transferId = res.json('transferId');

  if (Math.random() < RETRY_RATE) {
    // The retry a client makes when the first response was lost in transit. The gate must answer
    // with the ORIGINAL response - same status, same transfer - and say it is a replay.
    const again = http.post(`${BASE}/api/v1/transfers`, body, {
      headers,
      responseType: 'text',
      responseCallback: CREATE_OK,
      tags: { name: 'POST /api/v1/transfers (retry)', op: 'retry' },
    });
    // A 503 here is the BULKHEAD (no permit), which answers before the gate runs and says nothing
    // about this payment - not a verdict on idempotency either way. Admission control cannot be
    // the cause: a retry of an accepted payment is replayed at any load, and that is asserted by
    // AdmissionControlTest rather than hoped for here.
    if (again.status !== 503) {
      const ok = again.status === 202
        && again.headers['Idempotency-Replayed'] === 'true'
        && again.json('transferId') === transferId;
      replayConsistent.add(ok);
      check(again, { 'retry: same transfer, marked replayed': () => ok });
    }
  }

  settle(transferId, token, started);
  think();
}

function settle(transferId, token, started) {
  const deadline = started + POLL_TIMEOUT_S * 1000;
  while (Date.now() < deadline) {
    sleep(POLL_INTERVAL_S);
    const res = http.get(`${BASE}/api/v1/transfers/${transferId}`, {
      headers: { Authorization: `Bearer ${token}` },
      responseType: 'text',
      responseCallback: POLL_OK,
      tags: { name: 'GET /api/v1/transfers/{id}', op: 'poll' },
    });
    if (res.status !== 200) continue;
    const status = res.json('status');
    if (status === 'PENDING') continue;
    settleMs.add(Date.now() - started);
    outcomes.add(1, { outcome: status });
    completed.add(status === 'COMPLETED');
    return;
  }
  // Not a verdict on its own - the runner waits for quiescence and reads where it actually ended.
  outcomes.add(1, { outcome: 'UNSETTLED' });
  completed.add(false);
}

function think() {
  if (PROFILE === 'knee') return; // the arrival schedule is the pacing
  sleep(between(THINK_MIN, THINK_MAX));
}
