/**
 * The engine's chaos suite, in miniature.
 *
 * Deliberately built the same way as chaos/ in the Java system: it drives the real HTTP API
 * against a real PostgreSQL, injects one fault per scenario, waits for the system to go quiet,
 * and then judges it by what is in the ledger rather than by the status codes it returned. There
 * are no mocks and no stubbed database, because every guarantee being tested here lives in a
 * constraint, and a fake would be a test of the fake.
 *
 * Plain JavaScript and node:test, so it needs no build step and no dependency.
 *
 *   npm run build && npm start        # in one terminal, with DATABASE_URL set
 *   npm test                          # in another
 *
 * BASE_URL overrides the target (default http://127.0.0.1:3000).
 */

import { test, describe, before } from "node:test";
import assert from "node:assert/strict";

const BASE = process.env.BASE_URL ?? "http://127.0.0.1:3000";

const ALICE_OPENING = 100_000;
const BOB_OPENING = 50_000;
const AMOUNT = 30_000;

/** A client that keeps its session cookie, so each test gets its own isolated world. */
function client() {
  let cookie = null;
  return async function call(path, init = {}) {
    const headers = { ...(init.headers ?? {}) };
    if (cookie) headers.cookie = cookie;
    if (init.body) headers["content-type"] = "application/json";

    const res = await fetch(BASE + path, { ...init, headers });
    for (const c of res.headers.getSetCookie?.() ?? []) {
      if (c.startsWith("dpe_session=")) cookie = c.split(";")[0];
    }
    const text = await res.text();
    return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : null };
  };
}

const balances = (s) =>
  Object.fromEntries(
    s.accounts.filter((a) => a.type === "CUSTOMER").map((a) => [a.owner, a.balanceMinor])
  );
const checks = (s) => Object.fromEntries(s.invariants.checks.map((c) => [c.id, c.holds]));
const ledgerSum = (s) => s.ledger.reduce((a, e) => a + e.amountMinor, 0);

/** Turn the crank until the system is at rest, or give up and say so. */
async function settle(call, { rounds = 60, ms = 350 } = {}) {
  let state = null;
  for (let i = 0; i < rounds; i++) {
    state = (await call("/api/tick", { method: "POST" })).body;
    if (state.invariants.quiescent) return state;
    await new Promise((r) => setTimeout(r, ms));
  }
  return state;
}

async function send(call, amountMinor = AMOUNT, key = null) {
  return call("/api/transfers", {
    method: "POST",
    headers: { "Idempotency-Key": key ?? `t-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({ fromOwner: "alice", toOwner: "bob", amountMinor }),
  });
}

const DEFAULTS = {
  gateway_mode: "APPROVE",
  broker_down: false,
  duplicate_replies: false,
  drop_first_commit: false,
  forward_recovery: true,
};

/** A fresh world with known faults. Every test starts from here. */
async function world(faults = {}) {
  const call = client();
  await call("/api/state");
  await call("/api/reset", { method: "POST" });
  await call("/api/faults", {
    method: "POST",
    body: JSON.stringify({ ...DEFAULTS, ...faults }),
  });
  return call;
}

/** The checks that must hold no matter what was done to the system. */
function assertMoneyIsSafe(state, label) {
  const c = checks(state);
  assert.equal(ledgerSum(state), 0, `${label}: the ledger must sum to zero`);
  for (const id of ["I1", "I2", "I3", "I5"]) {
    assert.notEqual(c[id], false, `${label}: ${id} must not be violated`);
  }
}

before(async () => {
  const res = await fetch(BASE + "/api/state").catch(() => null);
  if (!res || !res.ok) {
    throw new Error(
      `No engine at ${BASE}. Start one with DATABASE_URL set:\n` +
        `  npm run build && npm start\n` +
        `then re-run, optionally with BASE_URL=http://127.0.0.1:PORT`
    );
  }
});

describe("a world opens balanced", () => {
  test("funding issues money against an issuance account, so I1 holds before anything happens", async () => {
    const call = await world();
    const { body: s } = await call("/api/state");

    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING });
    assert.equal(ledgerSum(s), 0);
    const issuance = s.accounts.find((a) => a.type === "SYSTEM");
    assert.equal(issuance.balanceMinor, -(ALICE_OPENING + BOB_OPENING));
    assertMoneyIsSafe(s, "fresh world");
  });
});

describe("the happy path", () => {
  test("a payment settles and every check holds", async () => {
    const call = await world();
    const accepted = await send(call);

    assert.equal(accepted.status, 202, "acceptance is 202, not 200 - no money has moved yet");
    assert.equal(accepted.body.transfers[0].sagaStatus, "STARTED");

    const s = await settle(call);
    assert.equal(s.transfers[0].sagaStatus, "COMPLETED");
    assert.equal(s.transfers[0].status, "COMPLETED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING - AMOUNT, bob: BOB_OPENING + AMOUNT });
    assertMoneyIsSafe(s, "happy path");
    for (const [id, holds] of Object.entries(checks(s))) {
      assert.equal(holds, true, `happy path: ${id} must pass once at rest`);
    }
  });
});

describe("idempotency", () => {
  test("the same key returns the original payment and moves no further money", async () => {
    const call = await world();
    const first = await send(call, AMOUNT, "same-key");
    const settled = await settle(call);
    const after = balances(settled);
    const entries = settled.ledger.length;

    const second = await send(call, AMOUNT, "same-key");
    assert.equal(second.body.accepted.replayed, true);
    assert.equal(second.body.accepted.transferId, first.body.accepted.transferId);
    assert.equal(second.headers.get("idempotency-replayed"), "true");

    const s = await settle(call);
    assert.deepEqual(balances(s), after, "a replay must not move money");
    assert.equal(s.ledger.length, entries, "a replay must not write ledger entries");
    assertMoneyIsSafe(s, "replay");
  });
});

describe("the card is declined", () => {
  test("the sender ends exactly whole, not approximately", async () => {
    const call = await world({ gateway_mode: "DECLINE" });
    await send(call);
    const s = await settle(call);

    assert.equal(s.transfers[0].sagaStatus, "COMPENSATED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING });
    assertMoneyIsSafe(s, "decline");
    assert.equal(checks(s).S1, true, "no hold may be left active");
  });
});

describe("the broker dies", () => {
  test("payments are still accepted, nothing moves, and the backlog drains on restore", async () => {
    const call = await world({ broker_down: true });

    for (const key of ["b1", "b2"]) {
      const res = await send(call, AMOUNT, key);
      assert.equal(res.status, 202, "a broker outage is not a customer-visible outage");
    }

    let s = (await call("/api/tick", { method: "POST" })).body;
    assert.ok(s.outbox.unpublished >= 2, "commands wait in the outbox as committed rows");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING }, "nothing moved");
    assertMoneyIsSafe(s, "broker down");

    await call("/api/faults", { method: "POST", body: JSON.stringify({ broker_down: false }) });
    s = await settle(call);

    assert.equal(s.outbox.unpublished, 0, "the relay drains the backlog");
    assert.deepEqual(balances(s), {
      alice: ALICE_OPENING - 2 * AMOUNT,
      bob: BOB_OPENING + 2 * AMOUNT,
    });
    assertMoneyIsSafe(s, "broker restored");
  });
});

describe("at-least-once delivery", () => {
  test("every message delivered twice still pays the recipient once", async () => {
    const call = await world({ duplicate_replies: true });
    await send(call);
    const s = await settle(call);

    assert.equal(s.transfers[0].sagaStatus, "COMPLETED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING - AMOUNT, bob: BOB_OPENING + AMOUNT });
    assert.equal(s.ledger.length, 8, "4 funding entries + 4 for one payment, not 12");
    assertMoneyIsSafe(s, "duplicate delivery");
  });
});

describe("insufficient funds", () => {
  test("is a business failure: terminal, with nothing to compensate", async () => {
    const call = await world();
    await send(call, ALICE_OPENING * 2);
    const s = await settle(call);

    assert.equal(s.transfers[0].sagaStatus, "FAILED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING });
    assert.equal(s.ledger.length, 4, "no ledger entry is written for a rejected reserve");
    assertMoneyIsSafe(s, "insufficient funds");
  });
});

describe("the provider never answers", () => {
  test("before the pivot, the deadline unwinds and the sender is made whole", async () => {
    const call = await world({ gateway_mode: "TIMEOUT" });
    await send(call);
    const s = await settle(call, { rounds: 80 });

    assert.equal(s.transfers[0].sagaStatus, "COMPENSATED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING });
    assert.equal(s.charges.approved, 0);
    assert.ok(s.charges.voided >= 1, "a tombstone stops a late charge landing");
    assertMoneyIsSafe(s, "gateway timeout");
  });
});

/**
 * The defect the chaos suite found, in both directions.
 *
 * This is the reason the whole project exists, so it is the test worth reading. The money is
 * conserved inside our own books in BOTH cases - which is exactly why I1 to I5 cannot tell them
 * apart, and why S2 had to be written.
 */
describe("a timeout after the charge", () => {
  test("unwinding refunds the sender while the card network keeps the charge - I1-I5 green, S2 red", async () => {
    const call = await world({ drop_first_commit: true, forward_recovery: false });
    await send(call);
    const s = await settle(call, { rounds: 80 });

    assert.equal(s.transfers[0].sagaStatus, "COMPENSATED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING, bob: BOB_OPENING }, "the sender was refunded");
    assert.equal(s.charges.approved, 1, "and the charge is still approved out in the world");

    // The point: our own books are immaculate.
    assertMoneyIsSafe(s, "backward recovery past the pivot");
    assert.equal(checks(s).I4, true);
    assert.equal(checks(s).S1, true);

    // And the payment is still wrong.
    assert.equal(checks(s).S2, false, "S2 is the only check that can see this");

    const report = await call("/api/invariants");
    assert.equal(report.status, 409, "the invariants endpoint refuses to report 200");
  });

  test("finishing it completes the payment and every check holds", async () => {
    const call = await world({ drop_first_commit: true, forward_recovery: true });
    await send(call);
    const s = await settle(call, { rounds: 80 });

    assert.equal(s.transfers[0].sagaStatus, "COMPLETED");
    assert.deepEqual(balances(s), { alice: ALICE_OPENING - AMOUNT, bob: BOB_OPENING + AMOUNT });
    assert.equal(s.charges.approved, 1);
    for (const [id, holds] of Object.entries(checks(s))) {
      assert.equal(holds, true, `forward recovery: ${id} must pass`);
    }

    const report = await call("/api/invariants");
    assert.equal(report.status, 200);
  });
});

describe("worlds are isolated", () => {
  test("one visitor's faults cannot reach another's ledger", async () => {
    const breaker = await world({ gateway_mode: "DECLINE" });
    const bystander = await world();

    await send(breaker);
    await send(bystander);

    const a = await settle(breaker);
    const b = await settle(bystander);

    assert.equal(a.transfers[0].sagaStatus, "COMPENSATED", "the one who declined gets a refund");
    assert.equal(b.transfers[0].sagaStatus, "COMPLETED", "the other is unaffected");
    assert.deepEqual(balances(b), { alice: ALICE_OPENING - AMOUNT, bob: BOB_OPENING + AMOUNT });
    assertMoneyIsSafe(a, "isolation/breaker");
    assertMoneyIsSafe(b, "isolation/bystander");
  });
});

describe("the API refuses what it should", () => {
  test("bad amounts and self-payment are rejected before anything is written", async () => {
    const call = await world();

    for (const amount of [0, -1, 1.5, 10_000_01]) {
      const res = await send(call, amount);
      assert.equal(res.status, 400, `amountMinor ${amount} must be refused`);
    }

    const self = await call("/api/transfers", {
      method: "POST",
      headers: { "Idempotency-Key": `self-${Date.now()}` },
      body: JSON.stringify({ fromOwner: "alice", toOwner: "alice", amountMinor: AMOUNT }),
    });
    assert.equal(self.status, 400);

    const { body: s } = await call("/api/state");
    assert.equal(s.ledger.length, 4, "a refused request writes nothing");
    assertMoneyIsSafe(s, "validation");
  });
});
