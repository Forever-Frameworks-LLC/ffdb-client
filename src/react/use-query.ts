import type { Compilable, Kysely } from "kysely";
import { useCallback, useContext, useEffect, useRef, useState } from "react";
import { inflightQueries, queryCache } from "../query-cache.js";
import type { Database as BasicDatabase } from "../types.ts";
import { FFDBContext } from "./context.tsx";

// Hook cache keys need to survive arbitrary dependency shapes. JSON is preferred
// because it keeps stable structural equality for plain data, with a string
// fallback so the hook never crashes on non-serializable values.
function serializeKey(value: unknown): string {
	try {
		return JSON.stringify(value);
	} catch {
		return String(value);
	}
}

// Query-builder hooks cache by the compiled SQL, not by object identity. That
// lets callers rebuild Kysely query objects every render without defeating the
// cache, while still separating logically different dependency sets.
function buildQueryCacheKey(query: unknown, deps: unknown[]): string | null {
	if (!query || typeof query !== "object") return null;
	const compilable = query as {
		compile?: () => { sql: string; parameters: readonly unknown[] };
	};
	if (typeof compilable.compile !== "function") return null;

	const compiled = compilable.compile();
	return JSON.stringify({
		sql: compiled.sql,
		params: compiled.parameters,
		deps,
	});
}

export type UseQueryOptions = {
	/** Refetch when the window regains focus (default: true) */
	refetchOnFocus?: boolean;
	/** Refetch when the browser comes back online (default: false) */
	refetchOnReconnect?: boolean;
	/** Refetch on an interval in milliseconds (default: off) */
	refetchInterval?: number;
	/** Whether the query should execute (default: true) */
	enabled?: boolean;
	/** Additional dependencies that trigger a refetch when changed */
	deps?: unknown[];
};

export type UseRawQueryInput<
	TValues extends readonly unknown[] = readonly unknown[],
> = {
	sql: string;
	values?: TValues;
	bypassCache?: boolean;
};

export type UseQueryResult<T> = {
	/** The query result data, or null if not yet loaded */
	data: T[] | null;
	/** Error if the query failed */
	error: Error | null;
	/** True during the initial load (no data yet) */
	isLoading: boolean;
	/** True whenever a fetch is in-flight (including refetches) */
	isFetching: boolean;
	/** Manually re-run the query */
	refetch: () => Promise<void>;
	/** Replace the cached data (for optimistic updates) */
	mutate: (data: T[] | null) => void;
};

export function useQuery<ExtraDatabase, T = unknown>(
	queryFn: (db: Kysely<BasicDatabase & ExtraDatabase>) => Compilable<T>,
	options: UseQueryOptions = {},
): UseQueryResult<T> {
	const {
		refetchOnFocus = true,
		refetchOnReconnect = false,
		refetchInterval,
		enabled = true,
		deps = [],
	} = options;

	const ctx = useContext(FFDBContext);
	if (!ctx) {
		throw new Error("useQuery must be used within an <FFDBProvider>");
	}

	const db = ctx.client?.db as Kysely<BasicDatabase & ExtraDatabase> | null;
	const subscribe = ctx.client?.subscribe;
	const clientVersion = ctx.clientVersion;
	const depsKey = serializeKey(deps);

	const [data, setData] = useState<T[] | null>(null);
	const [error, setError] = useState<Error | null>(null);
	const [isLoading, setIsLoading] = useState(true);
	const [isFetching, setIsFetching] = useState(false);

	// Consumers often inline query factories. Keeping the latest function in a ref
	// avoids rebuilding `execute` on every render while still running the newest
	// query definition when the hook actually fetches.
	const queryFnRef = useRef(queryFn);
	queryFnRef.current = queryFn;

	// Each execution gets a monotonically increasing id so slower earlier requests
	// cannot overwrite newer state after a refetch or dependency change.
	const fetchIdRef = useRef(0);

	const execute = useCallback(async () => {
		if (!enabled) return;
		if (!db) {
			if (!ctx.isLoading) {
				setIsLoading(false);
			}
			return;
		}

		const id = ++fetchIdRef.current;
		setIsFetching(true);

		try {
			const query = queryFnRef.current(db);
			const cacheKey = buildQueryCacheKey(query, deps);
			const cached = cacheKey ? queryCache.get(cacheKey) : null;

			// The React hook keeps a small in-memory cache on top of the offline driver
			// so repeated renders can reuse the last resolved rows immediately instead
			// of waiting for the database call to complete again.
			if (cached && id === fetchIdRef.current) {
				setData(cached.data as T[]);
				setIsLoading(false);
			}

			let result: T[];
			if (cacheKey && inflightQueries.has(cacheKey)) {
				// Share the same promise across components asking for the same query in the
				// same render window so one screen does not stampede the local driver.
				result = (await inflightQueries.get(cacheKey)) as T[];
			} else {
				const run = (query as any).execute() as Promise<T[]>;
				if (cacheKey) {
					inflightQueries.set(cacheKey, run as Promise<unknown[]>);
				}
				result = await run;
				if (cacheKey) {
					queryCache.set(cacheKey, {
						data: result as unknown[],
						updatedAt: Date.now(),
					});
					inflightQueries.delete(cacheKey);
				}
			}

			if (id === fetchIdRef.current) {
				setData(result);
				setError(null);
			}
		} catch (err) {
			if (id === fetchIdRef.current) {
				setError(err instanceof Error ? err : new Error(String(err)));
			}
		} finally {
			if (id === fetchIdRef.current) {
				setIsLoading(false);
				setIsFetching(false);
			}
		}
	}, [db, enabled, ctx.isLoading, depsKey]);

	// Initial fetch when db becomes available, and refetch when deps change
	// eslint-disable-next-line react-hooks/exhaustive-deps
	useEffect(() => {
		void execute();
	}, [execute, clientVersion, depsKey]);

	// Refetch on window focus
	useEffect(() => {
		if (!refetchOnFocus || typeof window === "undefined") return;

		const handler = () => {
			if (document.visibilityState === "visible") {
				execute();
			}
		};

		document.addEventListener("visibilitychange", handler);
		return () => document.removeEventListener("visibilitychange", handler);
	}, [refetchOnFocus, execute]);

	// Refetch on reconnect
	useEffect(() => {
		if (!refetchOnReconnect || typeof window === "undefined") return;

		const handler = () => execute();
		window.addEventListener("online", handler);
		return () => window.removeEventListener("online", handler);
	}, [refetchOnReconnect, execute]);

	// Re-run the query whenever the local cache changes.
	// This is what enables stale-while-revalidate behavior:
	// 1) query returns local rows immediately
	// 2) online read-through refresh updates local SQLite in the background
	// 3) subscribers are notified and this hook refetches from local cache
	useEffect(() => {
		if (!enabled || !subscribe) return;
		return subscribe(() => {
			void execute();
		});
	}, [enabled, subscribe, execute]);

	// Refetch on interval
	useEffect(() => {
		if (!refetchInterval || !enabled || !db) return;

		const id = setInterval(execute, refetchInterval);
		return () => clearInterval(id);
	}, [refetchInterval, enabled, db, execute]);

	const mutate = useCallback((newData: T[] | null) => {
		setData(newData);
	}, []);

	return {
		data,
		error,
		isLoading,
		isFetching,
		refetch: execute,
		mutate,
	};
}

export function useRawQuery<
	TRow extends Record<string, unknown> = Record<string, unknown>,
	TValues extends readonly unknown[] = readonly unknown[],
>(
	input: UseRawQueryInput<TValues>,
	options: UseQueryOptions = {},
): UseQueryResult<TRow> {
	const {
		refetchOnFocus = true,
		refetchOnReconnect = false,
		refetchInterval,
		enabled = true,
		deps = [],
	} = options;

	const ctx = useContext(FFDBContext);
	if (!ctx) {
		throw new Error("useRawQuery must be used within an <FFDBProvider>");
	}

	const sqlHelper = ctx.client?.sql;
	const subscribe = ctx.client?.subscribe;
	const clientVersion = ctx.clientVersion;
	const depsKey = serializeKey(deps);
	const inputKey = serializeKey({
		sql: input.sql,
		values: input.values ?? [],
		bypassCache: input.bypassCache === true,
		deps,
	});

	const [data, setData] = useState<TRow[] | null>(null);
	const [error, setError] = useState<Error | null>(null);
	const [isLoading, setIsLoading] = useState(true);
	const [isFetching, setIsFetching] = useState(false);

	// Raw query callers frequently rebuild the input object inline, so the hook
	// stores the latest version in a ref for the same reason `useQuery` stores the
	// latest query factory.
	const inputRef = useRef(input);
	inputRef.current = input;

	const fetchIdRef = useRef(0);

	const execute = useCallback(async () => {
		if (!enabled) return;
		if (!sqlHelper) {
			if (!ctx.isLoading) {
				setIsLoading(false);
			}
			return;
		}

		const id = ++fetchIdRef.current;
		setIsFetching(true);

		try {
			const rawInput = inputRef.current;
			const shouldBypassCache = rawInput.bypassCache === true;
			const cacheKey = JSON.stringify({
				sql: rawInput.sql,
				values: rawInput.values ?? [],
				bypassCache: shouldBypassCache,
				deps,
			});
			const cached = shouldBypassCache ? null : queryCache.get(cacheKey);

			// `bypassCache` is used for "show me the backend's current answer" reads.
			// Replaying the hook-level cache here would undermine that request even if
			// the lower driver skips its own query cache correctly.
			if (cached && id === fetchIdRef.current) {
				setData(cached.data as TRow[]);
				setIsLoading(false);
			}

			let result: TRow[];
			if (inflightQueries.has(cacheKey)) {
				result = (await inflightQueries.get(cacheKey)) as TRow[];
			} else {
				const run = sqlHelper.query<TRow, TValues>(rawInput);
				inflightQueries.set(cacheKey, run as Promise<unknown[]>);
				result = await run;
				if (!shouldBypassCache) {
					queryCache.set(cacheKey, {
						data: result as unknown[],
						updatedAt: Date.now(),
					});
				}
				inflightQueries.delete(cacheKey);
			}

			if (id === fetchIdRef.current) {
				setData(result);
				setError(null);
			}
		} catch (err) {
			if (id === fetchIdRef.current) {
				setError(err instanceof Error ? err : new Error(String(err)));
			}
		} finally {
			if (id === fetchIdRef.current) {
				setIsLoading(false);
				setIsFetching(false);
			}
		}
	}, [sqlHelper, enabled, ctx.isLoading, depsKey, inputKey]);

	useEffect(() => {
		void execute();
	}, [execute, clientVersion, inputKey]);

	useEffect(() => {
		if (!refetchOnFocus || typeof window === "undefined") return;

		const handler = () => {
			if (document.visibilityState === "visible") {
				void execute();
			}
		};

		document.addEventListener("visibilitychange", handler);
		return () => document.removeEventListener("visibilitychange", handler);
	}, [refetchOnFocus, execute]);

	useEffect(() => {
		if (!refetchOnReconnect || typeof window === "undefined") return;

		const handler = () => {
			void execute();
		};
		window.addEventListener("online", handler);
		return () => window.removeEventListener("online", handler);
	}, [refetchOnReconnect, execute]);

	useEffect(() => {
		if (!enabled || !subscribe) return;
		// The raw query hook also participates in driver-level stale-while-
		// revalidate. When the offline layer refreshes data in the background, the
		// shared subscription prompts this hook to re-run with the latest rows.
		return subscribe(() => {
			void execute();
		});
	}, [enabled, subscribe, execute]);

	useEffect(() => {
		if (!refetchInterval || !enabled || !sqlHelper) return;

		const id = setInterval(() => {
			void execute();
		}, refetchInterval);
		return () => clearInterval(id);
	}, [refetchInterval, enabled, sqlHelper, execute]);

	const mutate = useCallback((newData: TRow[] | null) => {
		setData(newData);
	}, []);

	return {
		data,
		error,
		isLoading,
		isFetching,
		refetch: execute,
		mutate,
	};
}
