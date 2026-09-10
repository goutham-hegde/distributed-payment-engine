import { useCallback, useEffect, useRef, useState } from "react";

export interface Async<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  /** Re-run now, out of band with the polling interval. */
  refresh: () => void;
}

/**
 * Fetch something, optionally on a timer.
 *
 * <h3>Polling, not websockets, and that is a decision rather than a shortcut</h3>
 *
 * Every number on this console is a query against a database or Prometheus. Pushing them would
 * mean each service holding an open connection per viewer and a publisher on the write path -
 * which is a second write next to the business transaction, i.e. the dual-write problem this
 * project exists to avoid, arriving through the UI door. Polling makes the console's load a
 * function of the poll interval and the number of viewers, both of which are visible and bounded.
 *
 * <h3>Two things this has to get right</h3>
 *
 * <b>No overlapping requests.</b> If a response takes longer than the interval - which is exactly
 * what happens when the system is struggling, i.e. when someone is watching this screen - a naive
 * `setInterval(fetch)` stacks requests on a service that is already slow. The timer is re-armed
 * AFTER each response instead, so a slow backend gets fewer requests rather than more.
 *
 * <b>No writes after unmount, and no out-of-order writes.</b> A response that arrives after the
 * component is gone, or after a newer request has already resolved, must not be applied. A
 * monotonically increasing request id handles both.
 */
export function useAsync<T>(
  fetcher: () => Promise<T>,
  deps: unknown[],
  intervalMs?: number,
): Async<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  const latest = useRef(0);
  const alive = useRef(true);
  const [nonce, setNonce] = useState(0);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  // `fetcher` is a new closure on every render, so it cannot be a dependency without re-running
  // this effect constantly. The caller passes `deps` describing what the fetch actually depends
  // on, and the ref keeps the closure current without participating in that decision.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    alive.current = true;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const run = async () => {
      const id = ++latest.current;
      try {
        const result = await fetcherRef.current();
        if (!alive.current || id !== latest.current) return;
        setData(result);
        setError(null);
      } catch (e) {
        if (!alive.current || id !== latest.current) return;
        setError(e instanceof Error ? e : new Error(String(e)));
      } finally {
        if (alive.current && id === latest.current) {
          setLoading(false);
          // Re-armed here, after the response, rather than on a fixed interval.
          if (intervalMs) timer = setTimeout(run, intervalMs);
        }
      }
    };

    setLoading(true);
    void run();

    return () => {
      alive.current = false;
      if (timer) clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, intervalMs, nonce]);

  return { data, error, loading, refresh };
}

/**
 * The transfer id in the URL hash, so a timeline survives a refresh and can be pasted to someone.
 *
 * Hash routing rather than the History API on purpose: a hash never reaches the server, so no
 * nginx rule is needed for deep links to work. (`try_files` is in nginx.conf anyway, because the
 * app should not break if someone later switches to path routing - but nothing depends on it.)
 */
export function useHashRoute(): [string | null, (id: string | null) => void] {
  const read = () => {
    const match = /^#\/transfer\/([0-9a-fA-F-]{36})$/.exec(window.location.hash);
    return match ? match[1] : null;
  };

  const [value, setValue] = useState<string | null>(read);

  useEffect(() => {
    const onChange = () => setValue(read());
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  const navigate = useCallback((id: string | null) => {
    // Assigning the hash fires `hashchange`, which is what updates the state - so there is one
    // path into `value` rather than two that can disagree.
    window.location.hash = id ? `/transfer/${id}` : "";
    if (!id) setValue(null);
  }, []);

  return [value, navigate];
}
