import type {
	CompiledQuery,
	DatabaseConnection,
	Driver,
	QueryResult,
} from "kysely";
import type { SyncManager } from "./sync.ts";
import type { OfflineAdapter } from "./types.ts";

type OfflineDriverConfig = {
	adapter: OfflineAdapter;
	syncManager: SyncManager;
};

type RefreshState = {
	inFlightRefreshes: Set<string>;
	lastRefreshAt: Map<string, number>;
};

type QueryCacheEntry = {
	rows: Record<string, unknown>[];
	updatedAt: number;
};

type OfflineCompiledQueryMeta = {
	isReadOnly?: boolean;
	primaryTable?: string | null;
	isAmbiguous?: boolean;
	bypassCache?: boolean;
};

const BACKGROUND_REFRESH_COOLDOWN_MS = 5_000;
const QUERY_CACHE_TABLE = "__ffdb_query_cache";
const QUERY_CACHE_MAX_ENTRIES = 250;
const QUERY_CACHE_TTL_MS = 1000 * 60 * 60 * 24;

/**
 * Split a CREATE TABLE definition list on top-level commas only.
 *
 * Column definitions can contain nested commas inside constraints or quoted
 * text, so a simple `split(",")` would misparse valid SQL and produce the
 * wrong offline schema checks.
 */
function splitSqlDefinitions(definitions: string) {
	const parts: string[] = [];
	let current = "";
	let depth = 0;
	let quote: string | null = null;

	for (let index = 0; index < definitions.length; index += 1) {
		const char = definitions[index];
		const next = definitions[index + 1];

		current += char;

		if (quote) {
			if (char === quote) {
				if ((quote === "'" || quote === '"') && next === quote) {
					current += next;
					index += 1;
					continue;
				}
				quote = null;
			}
			continue;
		}

		if (char === "'" || char === '"' || char === "`") {
			quote = char;
			continue;
		}

		if (char === "(") {
			depth += 1;
			continue;
		}

		if (char === ")") {
			depth = Math.max(0, depth - 1);
			continue;
		}

		if (char === "," && depth === 0) {
			parts.push(current.slice(0, -1).trim());
			current = "";
		}
	}

	if (current.trim()) {
		parts.push(current.trim());
	}

	return parts.filter(Boolean);
}

/**
 * Normalize SQL identifiers so lightweight schema checks can compare names
 * without caring about quoting style or case differences.
 */
function normalizeIdentifier(value: string) {
	return value.replace(/^["`[]|["`\]]$/g, "").toLowerCase();
}

/**
 * Remove leading SQL comments before statement classification.
 *
 * Kysely-generated SQL is usually clean, but raw SQL helpers can prepend
 * comments. Stripping them keeps the read/write classifier focused on the real
 * statement instead of the first comment token.
 */
function stripLeadingSqlComments(sql: string): string {
	let remaining = sql.trimStart();

	while (remaining.length > 0) {
		if (remaining.startsWith("--")) {
			const newlineIndex = remaining.indexOf("\n");
			remaining =
				newlineIndex === -1
					? ""
					: remaining.slice(newlineIndex + 1).trimStart();
			continue;
		}

		if (remaining.startsWith("/*")) {
			const commentEnd = remaining.indexOf("*/");
			remaining =
				commentEnd === -1 ? "" : remaining.slice(commentEnd + 2).trimStart();
			continue;
		}

		break;
	}

	return remaining;
}

/**
 * Decide whether a statement is safe to treat as read-only for offline
 * caching.
 *
 * The driver is intentionally conservative: once a statement looks like it
 * might mutate data, it is routed down the write path so cache invalidation and
 * sync queue behavior stay correct.
 */
function isReadOnlySql(sql: string): boolean {
	const normalized = stripLeadingSqlComments(sql).toUpperCase();
	if (!normalized) return false;

	if (
		normalized.startsWith("SELECT") ||
		normalized.startsWith("PRAGMA") ||
		normalized.startsWith("EXPLAIN SELECT") ||
		normalized.startsWith("EXPLAIN QUERY PLAN SELECT")
	) {
		return true;
	}

	if (!normalized.startsWith("WITH")) {
		return false;
	}

	let depth = 0;
	let quote: string | null = null;

	for (let index = 0; index < normalized.length; index += 1) {
		const char = normalized[index];
		const next = normalized[index + 1];

		if (quote) {
			if (char === quote) {
				if ((quote === "'" || quote === '"') && next === quote) {
					index += 1;
					continue;
				}
				quote = null;
			}
			continue;
		}

		if (char === "'" || char === '"' || char === "`") {
			quote = char;
			continue;
		}

		if (char === "(") {
			depth += 1;
			continue;
		}

		if (char === ")") {
			depth = Math.max(0, depth - 1);
			continue;
		}

		if (depth === 0) {
			const remainder = normalized.slice(index).trimStart();
			if (remainder.startsWith("SELECT")) return true;
			if (remainder.startsWith("VALUES")) return true;
			if (remainder.startsWith("INSERT")) return false;
			if (remainder.startsWith("UPDATE")) return false;
			if (remainder.startsWith("DELETE")) return false;
		}
	}

	return false;
}

/**
 * Offline metadata can be attached in a few shapes depending on which helper
 * produced the compiled query. This consolidates the lookup so the execution
 * path only needs one read.
 */
function getOfflineCompiledQueryMeta(
	compiledQuery: CompiledQuery,
): OfflineCompiledQueryMeta {
	const offlineMeta = (
		compiledQuery as CompiledQuery & {
			offline?: OfflineCompiledQueryMeta;
		}
	).offline;
	if (offlineMeta) return offlineMeta;

	const ffdbMeta = (
		compiledQuery as CompiledQuery & {
			ffdb?: OfflineCompiledQueryMeta;
		}
	).ffdb;
	if (ffdbMeta) return ffdbMeta;

	return {};
}

/**
 * Detect statements where regex-based table extraction is too risky.
 *
 * For complex SQL we would rather skip targeted refresh/invalidation than guess
 * the wrong table and leave stale data behind.
 */
function isAmbiguousSqlForTableExtraction(sql: string): boolean {
	const normalized = stripLeadingSqlComments(sql)
		.toUpperCase()
		.replace(/\s+/g, " ");

	return (
		normalized.includes(" JOIN ") ||
		normalized.startsWith("WITH ") ||
		normalized.includes(" FROM (") ||
		normalized.includes(" INTO (") ||
		normalized.includes(" UPDATE (") ||
		normalized.includes(" DELETE FROM (")
	);
}

/**
 * Guardrail for offline-created tables.
 *
 * The sync layer assumes application rows are keyed by text IDs. Rejecting
 * integer primary keys early prevents local-only tables from drifting away from
 * the ID strategy used by sync, replication, and client-generated records.
 */
function assertSupportedOfflineMutation(sql: string) {
	const trimmed = sql.trim();
	if (!/^create\s+table\b/i.test(trimmed)) return;

	const openParen = trimmed.indexOf("(");
	const closeParen = trimmed.lastIndexOf(")");
	if (openParen === -1 || closeParen <= openParen) return;

	const definitions = splitSqlDefinitions(
		trimmed.slice(openParen + 1, closeParen),
	);
	let idType: string | null = null;
	let idIsPrimaryKey = false;

	for (const definition of definitions) {
		const normalized = definition.trim().replace(/\s+/g, " ");

		if (/^primary\s+key\s*\(/i.test(normalized)) {
			const match = normalized.match(/^primary\s+key\s*\(([^)]+)\)/i);
			const columns = (match?.[1] ?? "")
				.split(",")
				.map((column) => normalizeIdentifier(column.trim()));
			if (columns.includes("id")) {
				idIsPrimaryKey = true;
			}
			continue;
		}

		const columnMatch = normalized.match(
			/^(["`[]?[a-zA-Z_][\w$]*["`\]]?)\s+([^\s,]+)/i,
		);
		if (!columnMatch) continue;

		const columnName = normalizeIdentifier(columnMatch[1]);
		if (columnName !== "id") continue;

		idType = columnMatch[2];
		if (/\bprimary\s+key\b/i.test(normalized)) {
			idIsPrimaryKey = true;
		}
	}

	if (!idIsPrimaryKey) return;

	if (!idType || !/^(text|varchar|char|clob|nchar|nvarchar)\b/i.test(idType)) {
		throw new Error(
			"FFDB requires application tables to use 'id TEXT PRIMARY KEY' instead of integer primary keys.",
		);
	}
}

/**
 * Transactions are intentionally unsupported today because the driver cannot
 * guarantee atomic behavior across the local write and the sync queue.
 */
function unsupportedOfflineTransactionError(): Error {
	return new Error(
		"OfflineDriver does not support transactions. Avoid transaction-scoped Kysely operations against the offline driver until atomic local transaction semantics are implemented.",
	);
}

/**
 * Surface the real failure mode when a local write succeeds but queueing the
 * matching remote mutation fails.
 *
 * That state matters because the local cache may now contain data that will
 * never reach the server unless the caller intervenes.
 */
function offlineMutationQueueFailureError(sql: string, cause: unknown): Error {
	const error = new Error(
		`OfflineDriver applied a local write but failed to queue it for sync. The local database may now be ahead of the durable mutation queue. Statement: ${sql.slice(0, 120)}`,
	);
	(error as Error & { cause?: unknown }).cause = cause;
	return error;
}

/**
 * Kysely Driver that reads from a local SQLite database
 * and queues writes for later sync.
 *
 * - SELECT → local adapter (instant), then background refresh from server
 * - INSERT/UPDATE/DELETE → local adapter + mutation queue
 *
 * Read-through (stale-while-revalidate):
 *   When the device is online, read queries also fire against the server
 *   in the background. Server results refresh the query cache, and
 *   data-change subscribers are notified so the UI can re-render with
 *   fresher data. The initial response is always from the local database
 *   (instant), so the user sees data immediately.
 */
export class OfflineDriver implements Driver {
	#adapter: OfflineAdapter;
	#syncManager: SyncManager;
	#initPromise: Promise<void> | null = null;
	#isInitialized = false;
	#refreshState: RefreshState = {
		inFlightRefreshes: new Set<string>(),
		lastRefreshAt: new Map<string, number>(),
	};

	constructor(config: OfflineDriverConfig) {
		this.#adapter = config.adapter;
		this.#syncManager = config.syncManager;
	}

	/**
	 * Create the query-cache table lazily so the driver is safe to construct in
	 * environments where the underlying SQLite file starts empty.
	 */
	async init(): Promise<void> {
		if (this.#isInitialized) return;
		if (this.#initPromise) {
			await this.#initPromise;
			return;
		}

		this.#initPromise = (async () => {
			await this.#adapter.execute(`
				CREATE TABLE IF NOT EXISTS ${QUERY_CACHE_TABLE} (
					key TEXT PRIMARY KEY,
					table_name TEXT,
					sql TEXT NOT NULL,
					rows TEXT NOT NULL,
					updated_at INTEGER NOT NULL
				)
			`);
			this.#isInitialized = true;
		})();

		try {
			await this.#initPromise;
		} catch (error) {
			this.#initPromise = null;
			throw error;
		}
	}

	/**
	 * Most driver entry points can be called before explicit initialization, so
	 * they all funnel through this guard instead of assuming `init()` ran first.
	 */
	async #ensureInitialized(): Promise<void> {
		if (this.#isInitialized) return;
		if (this.#initPromise) {
			await this.#initPromise;
			return;
		}
		await this.init();
	}

	/**
	 * Each Kysely connection shares the same adapter and refresh bookkeeping so
	 * repeated reads can coalesce background refreshes instead of stampeding the
	 * network.
	 */
	async acquireConnection(): Promise<DatabaseConnection> {
		await this.#ensureInitialized();
		return new OfflineConnection(
			this.#adapter,
			this.#syncManager,
			this.#refreshState,
		);
	}

	/**
	 * Throw instead of silently pretending transactions work.
	 *
	 * A no-op transaction API would be worse here because callers could believe a
	 * series of local mutations is atomic when it is not.
	 */
	async beginTransaction(): Promise<void> {
		throw unsupportedOfflineTransactionError();
	}
	async commitTransaction(): Promise<void> {
		throw unsupportedOfflineTransactionError();
	}
	async rollbackTransaction(): Promise<void> {
		throw unsupportedOfflineTransactionError();
	}
	async releaseConnection(): Promise<void> {}
	async destroy(): Promise<void> {}
}

class OfflineConnection implements DatabaseConnection {
	#adapter: OfflineAdapter;
	#syncManager: SyncManager;
	#refreshState: RefreshState;

	constructor(
		adapter: OfflineAdapter,
		syncManager: SyncManager,
		refreshState: RefreshState,
	) {
		this.#adapter = adapter;
		this.#syncManager = syncManager;
		this.#refreshState = refreshState;
	}

	/**
	 * Combine explicit compiled-query hints with conservative SQL heuristics.
	 *
	 * The hints let callers override regex-based guessing when they know more
	 * about the statement shape, while the fallback keeps raw SQL helpers working.
	 */
	#classifyQuery(options: { sql: string; meta?: OfflineCompiledQueryMeta }): {
		isReadOnly: boolean;
		primaryTable: string | null;
		isAmbiguous: boolean;
	} {
		const sql = options.sql.trimStart();
		const meta = options.meta ?? {};
		const isAmbiguous =
			meta.isAmbiguous ?? isAmbiguousSqlForTableExtraction(sql);
		const isReadOnly = meta.isReadOnly ?? isReadOnlySql(sql);
		const primaryTable =
			meta.primaryTable !== undefined
				? meta.primaryTable
				: isAmbiguous
					? null
					: this.#extractPrimaryTableName(sql);

		return {
			isReadOnly,
			primaryTable,
			isAmbiguous,
		};
	}

	/**
	 * Fire-and-forget query-cache refreshes only when the sync layer can still
	 * authenticate remote reads.
	 */
	#scheduleBackgroundRefresh(
		sql: string,
		params: unknown[],
		meta: OfflineCompiledQueryMeta,
		canUseRemoteQuery: boolean,
	): void {
		if (!this.#syncManager.status.isOnline || !canUseRemoteQuery) {
			return;
		}

		this.#backgroundRefresh(sql, params, meta).catch(() => {
			/* swallow — stale query cache refresh is best-effort */
		});
	}

	async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
		const sql = compiledQuery.sql.trimStart();
		const params = compiledQuery.parameters as unknown[];
		const queryMeta = getOfflineCompiledQueryMeta(compiledQuery);
		// Prefer explicit compiled-query metadata when present, and fall back to conservative SQL heuristics for raw statements.
		const queryInfo = this.#classifyQuery({
			sql,
			meta: queryMeta,
		});
		const isWrite = !queryInfo.isReadOnly;
		const tableName = queryInfo.primaryTable;
		const shouldBypassCache = queryMeta.bypassCache === true;
		const cachedQuery =
			!isWrite && !shouldBypassCache
				? await this.#readCachedQuery(sql, params)
				: null;
		const hasPendingLocalWrites = this.#syncManager.status.pendingMutations > 0;
		const canUseRemoteQuery =
			typeof this.#syncManager.remoteQuery === "function" &&
			this.#syncManager.isAuthReady !== false;
		const wasWaitingForInitialHydration =
			!isWrite &&
			!hasPendingLocalWrites &&
			this.#syncManager.status.isSyncing &&
			this.#syncManager.status.lastSyncedAt === null;

		if (isWrite) {
			assertSupportedOfflineMutation(sql);
		}

		if (wasWaitingForInitialHydration) {
			// If a first-run sync is already building the local cache, wait briefly so
			// we can read from the hydrated table instead of racing into fallback paths.
			await this.#syncManager.waitForIdle?.(5_000);
		}

		// `bypassCache` is the caller's explicit request to see the backend's current
		// answer instead of the local query-cache or synced SQLite snapshot. When we
		// can still reach the query endpoint, honor that directly.
		if (
			!isWrite &&
			shouldBypassCache &&
			this.#syncManager.status.isOnline &&
			canUseRemoteQuery
		) {
			const remoteRows = await this.#refreshQueryCache(sql, params, tableName);
			return {
				rows: remoteRows as O[],
			};
		}

		if (!isWrite && !hasPendingLocalWrites && cachedQuery) {
			// Serve the local answer immediately, then try to freshen the query cache in
			// the background so the UI can re-render with newer rows later.
			this.#scheduleBackgroundRefresh(
				sql,
				params,
				queryMeta,
				canUseRemoteQuery,
			);
			return {
				rows: cachedQuery.rows as O[],
			};
		}

		let rows: Record<string, unknown>[];
		try {
			const result = await this.#adapter.execute(sql, params, compiledQuery);
			rows = result.rows;
			if (!isWrite) {
				// Cache the exact query result, not just the table rows, so repeated local
				// reads can avoid re-executing expensive remote-backed queries.
				await this.#writeCachedQuery(sql, params, rows, tableName);
				this.#scheduleBackgroundRefresh(
					sql,
					params,
					queryMeta,
					canUseRemoteQuery,
				);
			}
		} catch (error) {
			// Missing-table recovery is only appropriate for the initial hydration race.
			// Outside that narrow window, we fall back to cached results or remote query
			// refresh rather than silently rebuilding tables.
			const recoveredRows = await this.#recoverMissingLocalTable(
				error,
				sql,
				params,
				compiledQuery,
				tableName,
				wasWaitingForInitialHydration,
			);
			if (!isWrite && recoveredRows !== null) {
				rows = recoveredRows;
			} else if (!isWrite && cachedQuery) {
				rows = cachedQuery.rows;
			} else if (!isWrite && this.#isMissingTableError(error)) {
				if (this.#syncManager.status.isOnline && canUseRemoteQuery) {
					rows = await this.#refreshQueryCache(sql, params, tableName);
				} else {
					rows = [];
				}
			} else {
				throw error;
			}
		}

		/**
		 * Queue persistence happens before query-cache invalidation so a failed
		 * enqueue does not erase cached reads for a mutation that was never durably
		 * recorded for later sync.
		 *
		 * The local adapter write has still already happened at this point, so this
		 * error path remains intentionally loud: callers need to know the local cache
		 * may now be ahead of the durable mutation queue.
		 */
		if (isWrite) {
			try {
				await this.#syncManager.queueMutation(sql, params);
			} catch (error) {
				throw offlineMutationQueueFailureError(sql, error);
			}
			await this.#invalidateQueryCache(tableName);
		}

		return {
			rows: rows as O[],
		};
	}

	async #recoverMissingLocalTable(
		error: unknown,
		sql: string,
		params: unknown[],
		compiledQuery: CompiledQuery,
		tableName: string | null,
		wasWaitingForInitialHydration: boolean,
	): Promise<Record<string, unknown>[] | null> {
		if (!this.#isMissingTableError(error)) return null;
		if (!tableName) return null;
		// Only reconcile when a first-run sync was already in progress. Outside that
		// window, a missing table is more likely to mean the query was never part of
		// the synced table set.
		if (!wasWaitingForInitialHydration) return null;
		if (this.#syncManager.status.pendingMutations > 0) return null;
		if (this.#syncManager.isAuthReady === false) return null;

		if (!this.#syncManager.status.isOnline) return null;

		await this.#syncManager.reconcileTables?.([tableName]);

		try {
			const retried = await this.#adapter.execute(sql, params, compiledQuery);
			return retried.rows;
		} catch (retryError) {
			if (this.#isMissingTableError(retryError)) {
				return null;
			}
			throw retryError;
		}
	}

	/**
	 * Fire the same read query against the server, refresh the query-cache entry,
	 * and notify subscribers if the cached result changed.
	 *
	 * This is intentionally limited to simple single-table queries. Ambiguous SQL
	 * such as JOINs, CTEs, and sub-selects is skipped until table extraction is
	 * strong enough to make targeted refresh decisions safely.
	 */
	#makeRefreshKey(sql: string, params: unknown[]): string {
		return this.#makeQueryCacheKey(sql, params);
	}

	/**
	 * Cooldown timestamps are tracked per exact query so repeated renders of the
	 * same screen do not fire overlapping refreshes for identical cache entries.
	 */
	#markTableFresh(refreshKey: string): void {
		this.#refreshState.lastRefreshAt.set(refreshKey, Date.now());
	}

	#wasTableRefreshedRecently(refreshKey: string): boolean {
		const lastRefreshAt = this.#refreshState.lastRefreshAt.get(refreshKey) ?? 0;
		return Date.now() - lastRefreshAt < BACKGROUND_REFRESH_COOLDOWN_MS;
	}

	#extractPrimaryTableName(sql: string): string | null {
		const match = sql.match(/(?:FROM|INTO|UPDATE|DELETE\s+FROM)\s+"?(\w+)"?/i);
		return match?.[1] ?? null;
	}

	/**
	 * SQLite adapters vary in their exact error objects, so this stays string-
	 * based and intentionally broad.
	 */
	#isMissingTableError(error: unknown): boolean {
		const message = error instanceof Error ? error.message : String(error);
		return message.includes("no such table");
	}

	#makeQueryCacheKey(sql: string, params: unknown[]): string {
		// Normalize whitespace so semantically identical queries reuse the same cache
		// entry even if formatting differs between call sites.
		return JSON.stringify({
			sql: sql.replace(/\s+/g, " ").trim(),
			params,
		});
	}

	/**
	 * Read a cached query result and enforce TTL eagerly.
	 *
	 * Expired or malformed cache entries are deleted on read so long-idle clients
	 * do not keep serving stale rows forever just because no later write happened.
	 */
	async #readCachedQuery(
		sql: string,
		params: unknown[],
	): Promise<QueryCacheEntry | null> {
		const key = this.#makeQueryCacheKey(sql, params);

		try {
			const result = await this.#adapter.execute(
				`SELECT rows, updated_at FROM ${QUERY_CACHE_TABLE} WHERE key = ?`,
				[key],
			);
			const row = result.rows[0];
			if (!row) return null;

			const updatedAt = Number(row.updated_at ?? 0);
			if (!Number.isFinite(updatedAt)) {
				await this.#adapter.execute(
					`DELETE FROM ${QUERY_CACHE_TABLE} WHERE key = ?`,
					[key],
				);
				return null;
			}

			if (Date.now() - updatedAt > QUERY_CACHE_TTL_MS) {
				await this.#adapter.execute(
					`DELETE FROM ${QUERY_CACHE_TABLE} WHERE key = ?`,
					[key],
				);
				return null;
			}

			try {
				return {
					rows: JSON.parse(String(row.rows)) as Record<string, unknown>[],
					updatedAt,
				};
			} catch {
				await this.#adapter.execute(
					`DELETE FROM ${QUERY_CACHE_TABLE} WHERE key = ?`,
					[key],
				);
				return null;
			}
		} catch (error) {
			if (this.#isMissingTableError(error)) {
				return null;
			}
			return null;
		}
	}

	async #writeCachedQuery(
		sql: string,
		params: unknown[],
		rows: Record<string, unknown>[],
		tableName: string | null,
	): Promise<void> {
		const key = this.#makeQueryCacheKey(sql, params);
		const serializedRows = JSON.stringify(rows);
		const updatedAt = Date.now();

		// Query-cache entries are keyed by full SQL + params so different filtered
		// views of the same table do not overwrite one another.
		await this.#adapter.execute(
			`INSERT OR REPLACE INTO ${QUERY_CACHE_TABLE} (key, table_name, sql, rows, updated_at) VALUES (?, ?, ?, ?, ?)`,
			[key, tableName, sql, serializedRows, updatedAt],
		);
		await this.#pruneQueryCache();
	}

	/**
	 * Invalidate aggressively when table extraction is uncertain.
	 *
	 * Broad invalidation costs some cache warmth, but it is safer than leaving
	 * stale filtered queries around after a local write changes the table.
	 */
	async #invalidateQueryCache(tableName: string | null): Promise<void> {
		if (!tableName) {
			await this.#adapter.execute(`DELETE FROM ${QUERY_CACHE_TABLE}`);
			return;
		}

		await this.#adapter.execute(
			`DELETE FROM ${QUERY_CACHE_TABLE} WHERE table_name = ? OR table_name IS NULL`,
			[tableName],
		);
	}

	async #pruneQueryCache(): Promise<void> {
		// Keep the cache bounded both by age and by count so a long-lived offline
		// client does not let query results grow without limit.
		const expirationCutoff = Date.now() - QUERY_CACHE_TTL_MS;
		await this.#adapter.execute(
			`DELETE FROM ${QUERY_CACHE_TABLE} WHERE updated_at < ?`,
			[expirationCutoff],
		);
		await this.#adapter.execute(
			`DELETE FROM ${QUERY_CACHE_TABLE}
			 WHERE key NOT IN (
				SELECT key FROM ${QUERY_CACHE_TABLE}
				ORDER BY updated_at DESC
				LIMIT ?
			 )`,
			[QUERY_CACHE_MAX_ENTRIES],
		);
	}

	#normalizeComparableValue(value: unknown): unknown {
		// Background refreshes compare cached and remote rows. Sorting object keys
		// first avoids false-positive diffs caused only by key order changes.
		if (Array.isArray(value)) {
			return value.map((entry) => this.#normalizeComparableValue(entry));
		}

		if (value && typeof value === "object") {
			return Object.fromEntries(
				Object.entries(value as Record<string, unknown>)
					.sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
					.map(([key, entryValue]) => [
						key,
						this.#normalizeComparableValue(entryValue),
					]),
			);
		}

		return value;
	}

	#stableStringifyRows(rows: Record<string, unknown>[]): string {
		return JSON.stringify(this.#normalizeComparableValue(rows));
	}

	#rowsEqual(
		left: Record<string, unknown>[],
		right: Record<string, unknown>[],
	): boolean {
		return this.#stableStringifyRows(left) === this.#stableStringifyRows(right);
	}

	async #refreshQueryCache(
		sql: string,
		params: unknown[],
		tableName: string | null,
	): Promise<Record<string, unknown>[]> {
		const previous = await this.#readCachedQuery(sql, params);
		const remoteRows = await this.#syncManager.remoteQuery(sql, params);
		await this.#writeCachedQuery(sql, params, remoteRows, tableName);

		// Only notify subscribers when the query result actually changed. That keeps
		// background refreshes from causing pointless re-renders on stable screens.
		if (!previous || !this.#rowsEqual(previous.rows, remoteRows)) {
			this.#syncManager.notifyDataChange();
		}

		return remoteRows;
	}

	async #deferBackgroundWork(): Promise<void> {
		// Prefer idle time in the browser so cache refreshes do not compete with the
		// user-visible read path. In non-browser runtimes we just yield once.
		if (typeof globalThis.requestIdleCallback === "function") {
			await new Promise<void>((resolve) => {
				globalThis.requestIdleCallback(() => resolve(), { timeout: 250 });
			});
			return;
		}

		await Promise.resolve();
	}

	/**
	 * Refresh a cached query from the server without blocking the local read that
	 * triggered it.
	 *
	 * This updates query-cache entries, not the underlying synced SQLite tables.
	 * That narrower behavior is intentional for now: it keeps stale-while-
	 * revalidate semantics predictable while the table-level reconciliation logic
	 * remains owned by `SyncManager`.
	 */
	async #backgroundRefresh(
		sql: string,
		params: unknown[],
		meta?: OfflineCompiledQueryMeta,
	): Promise<void> {
		if (typeof this.#syncManager.remoteQuery !== "function") return;
		const queryInfo = this.#classifyQuery({ sql, meta });
		if (queryInfo.isAmbiguous) return;
		const tableName = queryInfo.primaryTable;
		if (!tableName) return;

		// Internal bookkeeping tables are not meaningful remote refresh targets.
		if (tableName.startsWith("__ffdb_") || tableName === "sqlite_master") {
			return;
		}

		const refreshKey = this.#makeRefreshKey(sql, params);
		if (this.#refreshState.inFlightRefreshes.has(refreshKey)) return;
		if (this.#wasTableRefreshedRecently(refreshKey)) return;
		this.#refreshState.inFlightRefreshes.add(refreshKey);

		try {
			await this.#deferBackgroundWork();
			await this.#refreshQueryCache(sql, params, tableName);
			// Mark success time, not start time, so failed refreshes do not suppress an
			// immediate retry forever.
			this.#markTableFresh(refreshKey);
		} finally {
			this.#refreshState.inFlightRefreshes.delete(refreshKey);
		}
	}

	/**
	 * Kysely's streaming API is satisfied by yielding the single locally resolved
	 * result. The driver does not provide true incremental cursor streaming.
	 */
	async *streamQuery<O>(
		compiledQuery: CompiledQuery,
	): AsyncIterableIterator<QueryResult<O>> {
		const result = await this.executeQuery<O>(compiledQuery);
		yield result;
	}
}
