import { query } from "./db";
import { invariants, type InvariantReport } from "./invariants";
import { DIRECTION, type EventType } from "./events";
import type { Faults } from "./gateway";

export type AccountView = {
  id: string;
  owner: string;
  type: string;
  balanceMinor: number;
};

export type LedgerRow = {
  id: number;
  transferId: string;
  account: string;
  accountType: string;
  entryType: "DEBIT" | "CREDIT";
  amountMinor: number;
};

export type MessageRow = {
  id: string;
  eventType: string;
  from: string;
  to: string;
  topic: string;
  published: boolean;
  consumed: boolean;
};

export type TransferView = {
  id: string;
  amountMinor: number;
  status: string;
  sagaStatus: string | null;
  createdAt: string;
  steps: { name: string; outcome: string; toStatus: string | null; detail: string | null }[];
};

export type EngineState = {
  accounts: AccountView[];
  ledger: LedgerRow[];
  messages: MessageRow[];
  transfers: TransferView[];
  faults: Faults;
  invariants: InvariantReport;
  outbox: { unpublished: number; undelivered: number };
  holdsActive: number;
  charges: { approved: number; declined: number; voided: number };
};

export async function readState(sessionId: string): Promise<EngineState> {
  const [accounts, ledger, messages, transfers, steps, faultRows, counts, report] = await Promise.all([
    query<any>(
      `SELECT id, owner_id, account_type, balance_minor FROM accounts
        WHERE session_id = $1
        ORDER BY CASE account_type WHEN 'SYSTEM' THEN 0 WHEN 'CUSTOMER' THEN 1 ELSE 2 END, owner_id`,
      [sessionId]
    ),
    query<any>(
      `SELECT e.id, e.transfer_id, e.amount_minor, e.entry_type, a.owner_id, a.account_type
         FROM ledger_entries e JOIN accounts a ON a.id = e.account_id
        WHERE e.session_id = $1
        ORDER BY e.id
        LIMIT 200`,
      [sessionId]
    ),
    query<any>(
      `SELECT id, event_type, topic, published_at, consumed_at FROM outbox
        WHERE session_id = $1 ORDER BY created_at DESC LIMIT 25`,
      [sessionId]
    ),
    query<any>(
      `SELECT t.id, t.amount_minor, t.status, t.created_at, s.id AS saga_id, s.status AS saga_status
         FROM transfers t LEFT JOIN saga_instances s ON s.transfer_id = t.id
        WHERE t.session_id = $1
        ORDER BY t.created_at DESC LIMIT 10`,
      [sessionId]
    ),
    query<any>(
      `SELECT saga_id, step_name, outcome, to_status, detail FROM saga_steps
        WHERE session_id = $1 ORDER BY id LIMIT 300`,
      [sessionId]
    ),
    query<any>("SELECT * FROM faults WHERE session_id = $1", [sessionId]),
    query<any>(
      `SELECT
         (SELECT COUNT(*) FROM outbox WHERE session_id = $1 AND published_at IS NULL) AS unpublished,
         (SELECT COUNT(*) FROM outbox WHERE session_id = $1
            AND published_at IS NOT NULL AND consumed_at IS NULL)                     AS undelivered,
         (SELECT COUNT(*) FROM holds WHERE session_id = $1 AND status = 'ACTIVE')     AS active_holds,
         (SELECT COUNT(*) FROM gateway_charges WHERE session_id = $1 AND status = 'APPROVED') AS approved,
         (SELECT COUNT(*) FROM gateway_charges WHERE session_id = $1 AND status = 'DECLINED') AS declined,
         (SELECT COUNT(*) FROM gateway_charges WHERE session_id = $1 AND status = 'VOIDED')   AS voided`,
      [sessionId]
    ),
    invariants(sessionId),
  ]);

  const stepsBySaga = new Map<string, TransferView["steps"]>();
  for (const s of steps) {
    const list = stepsBySaga.get(s.saga_id) ?? [];
    list.push({ name: s.step_name, outcome: s.outcome, toStatus: s.to_status, detail: s.detail });
    stepsBySaga.set(s.saga_id, list);
  }

  return {
    accounts: accounts.map((a) => ({
      id: a.id,
      owner: a.owner_id,
      type: a.account_type,
      balanceMinor: Number(a.balance_minor),
    })),
    ledger: ledger.map((e) => ({
      id: Number(e.id),
      transferId: e.transfer_id,
      account: e.owner_id,
      accountType: e.account_type,
      entryType: e.entry_type,
      amountMinor: Number(e.amount_minor),
    })),
    messages: messages.map((m) => {
      const dir = DIRECTION[m.event_type as EventType] ?? { from: "?", to: "?" };
      return {
        id: m.id,
        eventType: m.event_type,
        from: dir.from,
        to: dir.to,
        topic: m.topic,
        published: m.published_at !== null,
        consumed: m.consumed_at !== null,
      };
    }),
    transfers: transfers.map((t) => ({
      id: t.id,
      amountMinor: Number(t.amount_minor),
      status: t.status,
      sagaStatus: t.saga_status,
      createdAt: t.created_at,
      steps: stepsBySaga.get(t.saga_id) ?? [],
    })),
    faults: faultRows[0] ?? {
      gateway_mode: "APPROVE",
      duplicate_replies: false,
      broker_down: false,
      drop_first_commit: false,
      forward_recovery: true,
    },
    invariants: report,
    outbox: {
      unpublished: Number(counts[0]?.unpublished ?? 0),
      undelivered: Number(counts[0]?.undelivered ?? 0),
    },
    holdsActive: Number(counts[0]?.active_holds ?? 0),
    charges: {
      approved: Number(counts[0]?.approved ?? 0),
      declined: Number(counts[0]?.declined ?? 0),
      voided: Number(counts[0]?.voided ?? 0),
    },
  };
}
