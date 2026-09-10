/**
 * Rendering helpers. Nothing here does arithmetic on money except the one divide that turns minor
 * units into a display string, and that divide happens once, here.
 */

/**
 * Minor units to a display amount.
 *
 * The server's whole money discipline is that an amount is a BIGINT count of paise and is never a
 * float. Dividing by 100 for display is the one place a fractional number appears in this codebase,
 * and it is safe only because the result is immediately turned into a string and never fed back
 * into anything. Never send a value derived from this to an API; send the minor units.
 */
export function money(amountMinor: number, currency = "INR"): string {
  const sign = amountMinor < 0 ? "-" : "";
  const abs = Math.abs(amountMinor);
  const major = Math.floor(abs / 100);
  const minor = String(abs % 100).padStart(2, "0");
  return `${sign}${currency === "INR" ? "₹" : ""}${major.toLocaleString("en-IN")}.${minor}`;
}

/** A short, sortable local time. Dates on their own line would push every table too wide. */
export function time(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? "—"
    : d.toLocaleTimeString([], { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

export function dateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString([], { hour12: false });
}

/** Milliseconds, rendered at a precision that does not imply more than was measured. */
export function millis(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/** First eight characters of a UUID, which is enough to tell two apart by eye and to click. */
export function shortId(id: string | null | undefined): string {
  return id ? id.slice(0, 8) : "—";
}

/**
 * A number for a stat tile. `null` renders as an em dash rather than as 0.
 *
 * That distinction is the whole reason this function exists. A metrics query that failed and a
 * metric that is genuinely zero are different facts, and showing both as "0" is the console
 * telling an operator the outbox is drained when what actually happened is that Prometheus is
 * unreachable. Absent is not zero.
 */
export function stat(value: number | null, suffix = ""): string {
  if (value === null) return "—";
  const rounded = Math.abs(value) < 10 ? Math.round(value * 100) / 100 : Math.round(value);
  return `${rounded.toLocaleString()}${suffix}`;
}
