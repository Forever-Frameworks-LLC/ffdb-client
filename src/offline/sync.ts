import type { FetchFn } from "../adapter.ts";
import { type FFDBLogger, noopLogger } from "../logger.ts";
import type {
	MutationEntry,
	NetworkAdapter,
	OfflineAdapter,
	SyncStatus,
} from "./types.ts";

/* ─── Sync endpoint types (mirror server response) ─── */

type SyncColumnMeta = {
	name: string;
	type: string;
	notNull: boolean;
	primaryKey: boolean;
};

type SyncTableResult = {
	upserts: Record<string, unknown>[];
	deletes: unknown[];
	cursor: unknown | null;
	hasMore: boolean;
	rowCount?: number;
	columns?: SyncColumnMeta[];
	/** Which sync strategy the server used for this table. */
	syncMode?: "full" | "delta:updated_at" | "delta:row_audit";
};

type SyncPullResponse = {
	tables: Record<string, SyncTableResult>;
	syncedAt: number;
	schemaChanged: boolean;
};

/* ─── Config ─── */

type SyncManagerConfig = {
	adapter: OfflineAdapter;
	fetchFn: FetchFn;
	endpoint: string;
	tables?: string[];
	skipTables?: string[];
	maxRowsPerTable: number;
	pageSize?: number;
	syncInterval?: number;
	syncOnReconnect?: boolean;
	syncOnFocus?: boolean;
	/**
	 * Optional runtime-specific online/offline detector.
	 *
	 * Browsers can fall back to `online`/`offline` events, but desktop shells and
	 * native runtimes such as Tauri or React Native usually need their own
	 * reachability source wired in here.
	 */
	network?: NetworkAdapter;
	logger?: FFDBLogger;
};

const DEFAULT_PAGE_SIZE = 500;
const LARGE_RECONCILE_GAP_THRESHOLD = 8;
const TRACKED_TABLES_META_KEY = "tracked_tables";
const DEFAULT_EXCLUDED_SYNC_TABLES = [
	"account",
	"apikey",
	"ddl_audit",
	"policies",
	"query_metrics",
	"row_audit",
	"session",
	"system_metrics",
	"table_counts",
	"verification",
];

export class SyncManager {
	#adapter: OfflineAdapter;
	#fetch: FetchFn;
	#endpoint: string;
	#tables: string[] | undefined;
	#skipTables: Set<string>;
	#pageSize: number;
	#logger: FFDBLogger;
	#status: SyncStatus = {
		isSyncing: false,
		isOnline: true,
		lastSyncedAt: null,
		pendingMutations: 0,
		error: null,
	};
	#listeners: Set<() => void> = new Set();
	#dataListeners: Set<() => void> = new Set();

	/**
	 * Per-table cursor values. Persisted in __ffdb_sync_meta so pagination
	 * survives restarts. Only used during full sync when hasMore = true.
	 */
	#cursors: Record<string, unknown> = {};

	/** Cached column info per table — used for local upserts / deletes. */
	#schemaCache: Map<string, SyncColumnMeta[]> = new Map();

	// Auto-sync handles
	#initPromise: Promise<void> | null = null;
	#isInitialized = false;
	#intervalTimer: ReturnType<typeof setInterval> | null = null;
	#cleanupFns: (() => void)[] = [];
	#destroyed = false;
	#activePullPromise: Promise<void> | null = null;
	#maxRowsPerTable: number;
	#queuedReconcileTables: Set<string> = new Set();
	#isAuthReady = true;

	constructor(config: SyncManagerConfig) {
		this.#adapter = config.adapter;
		this.#fetch = config.fetchFn;
		this.#endpoint = config.endpoint;
		this.#skipTables = new Set([
			...DEFAULT_EXCLUDED_SYNC_TABLES,
			...(config.skipTables ?? []),
		]);
		this.#tables = this.#filterTables(config.tables);
		this.#pageSize = config.pageSize ?? DEFAULT_PAGE_SIZE;
		this.#maxRowsPerTable = config.maxRowsPerTable;
		this.#logger = config.logger ?? noopLogger;

		this.#setupAutoSync(config);
	}

	/* ─── Public API ─── */

	/** Update the fetch function (e.g. after API key rotation). */
	updateFetch(fetchFn: FetchFn): void {
		this.#fetch = fetchFn;
	}

	/**
	 * Central gate for remote sync work.
	 *
	 * Offline-first local reads can still function without auth, but any pull,
	 * push, or reconcile path that talks to the backend should stop immediately
	 * once auth is known to be unavailable or invalid.
	 */
	#canSyncRemotely(): boolean {
		if (this.#isAuthReady) return true;
		this.#logger.debug("Skipping remote sync while auth is not ready");
		return false;
	}

	/**
	 * Reconcile one or more specific tables in the background after a local-first read.
	 * Skips while writes are pending so optimistic local state is not overwritten.
	 */
	async reconcileTables(tables: string[]): Promise<void> {
		await this.#ensureInitialized();
		tables = this.#filterTables(tables) ?? [];
		if (!this.#canSyncRemotely()) return;
		if (!tables.length || !this.#status.isOnline || this.#destroyed) return;
		if (this.#status.isSyncing) {
			// Reconcile requests often come from read paths racing against an active
			// pull. Queue them so the table gets another targeted refresh once the
			// current sync has settled instead of running overlapping pulls.
			for (const table of tables) {
				this.#queuedReconcileTables.add(table);
			}
			return;
		}
		if (this.#status.pendingMutations > 0) {
			// When optimistic local writes are pending, prefer a full sync cycle so a
			// targeted reconcile does not overwrite local state that still needs to be
			// pushed upstream.
			await this.sync();
			return;
		}

		this.#setStatus({ isSyncing: true, error: null });
		try {
			const { syncedAt, didChange } = await this.#runPull({
				tables,
				cursors: {},
				persistCursors: false,
			});
			await this.#setMeta("last_synced_at", String(syncedAt));
			this.#setStatus({ isSyncing: false, lastSyncedAt: syncedAt });
			if (didChange) {
				try {
					await this.#invalidateHttpCache();
				} catch (err) {
					this.#logger.warn("Failed to invalidate HTTP cache", err);
				}
				try {
					// Clear in-memory React query cache so subscribers refetch fresh data.
					const { clearQueryCache } = await import("../query-cache.js");
					clearQueryCache();
				} catch (err) {
					this.#logger.debug("Failed to clear in-memory query cache", err);
				}
				this.notifyDataChange();
			}
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));
			this.#setStatus({ isSyncing: false, error });
		}
	}

	/**
	 * Dynamically set which tables to sync.
	 * Only applies if no explicit `tables` list was passed in the config.
	 */
	setTableFilter(tables: string[]): void {
		if (!this.#tables) {
			this.#tables = this.#filterTables(tables);
		}
	}

	/** Update online status. */
	setOnline(online: boolean): void {
		this.#setStatus({ isOnline: online });
	}

	/** Mark whether authenticated remote queries are ready to run. */
	setAuthReady(ready: boolean): void {
		const wasReady = this.#isAuthReady;
		this.#isAuthReady = ready;
		if (ready && !wasReady) {
			// Auth becoming available is effectively a data-readiness transition for
			// consumers that were waiting on remote-backed local caches.
			this.notifyDataChange();
		}
	}

	get status(): Readonly<SyncStatus> {
		return { ...this.#status };
	}

	get isAuthReady(): boolean {
		return this.#isAuthReady;
	}

	/** Wait for any active sync to finish. */
	async waitForIdle(timeoutMs = 10_000): Promise<void> {
		await this.#ensureInitialized();
		if (!this.#status.isSyncing) return;

		await new Promise<void>((resolve) => {
			const timeout = setTimeout(() => {
				unsubscribe();
				resolve();
			}, timeoutMs);
			const unsubscribe = this.subscribe(() => {
				if (!this.#status.isSyncing) {
					clearTimeout(timeout);
					unsubscribe();
					resolve();
				}
			});
		});
	}

	/** Subscribe to status changes. Returns an unsubscribe function. */
	subscribe(listener: () => void): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	/**
	 * Subscribe to data changes (sync pull complete, local writes, read-through refresh).
	 * The callback receives no arguments — the consumer re-queries to get fresh data.
	 * This is the primitive that frameworks (React, Vue, Svelte, vanilla JS) build on.
	 *
	 * Returns an unsubscribe function.
	 */
	subscribeData(listener: () => void): () => void {
		this.#dataListeners.add(listener);
		return () => this.#dataListeners.delete(listener);
	}

	/** Notify all data subscribers that local data has changed. */
	notifyDataChange(): void {
		for (const listener of this.#dataListeners) {
			try {
				listener();
			} catch {
				/* swallow */
			}
		}
	}

	/**
	 * Get cached column metadata for a table.
	 * Available after the first sync pull or read-through query.
	 */
	getTableSchema(tableName: string): SyncColumnMeta[] | undefined {
		return this.#schemaCache.get(tableName);
	}

	/**
	 * Execute a query against the remote server.
	 * Used by OfflineDriver for read-through caching (stale-while-revalidate).
	 * Uses the current (always-rotated) auth token.
	 */
	async remoteQuery(
		sql: string,
		params: unknown[],
	): Promise<Record<string, unknown>[]> {
		// Read-through refresh uses the same policy endpoint as normal remote SQL so
		// the cache sees whatever the current auth context is actually allowed to
		// read.
		const { data, error } = (await this.#fetch(this.#endpoint, {
			method: "POST",
			body: { sql, values: params },
		})) as {
			data: { data: { rows: Record<string, unknown>[] } } | null;
			error: unknown;
		};

		if (error || !data?.data?.rows) return [];
		return data.data.rows;
	}

	/** Initialize internal tables. Call once on startup. */
	async init(): Promise<void> {
		if (this.#isInitialized) return;
		if (this.#initPromise) {
			await this.#initPromise;
			return;
		}

		this.#initPromise = (async () => {
			// Mutations and sync metadata live alongside application tables inside the
			// same local SQLite store so offline writes and pagination state survive
			// process restarts.
			await this.#adapter.execute(`
				CREATE TABLE IF NOT EXISTS __ffdb_mutations (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					sql TEXT NOT NULL,
					params TEXT NOT NULL,
					created_at INTEGER NOT NULL
				)
			`);
			await this.#adapter.execute(`
				CREATE TABLE IF NOT EXISTS __ffdb_sync_meta (
					key TEXT PRIMARY KEY,
					value TEXT NOT NULL
				)
			`);
			await this.#refreshPendingCount();

			// Restore last sync timestamp
			const tsRow = await this.#getMeta("last_synced_at");
			if (tsRow !== null) {
				this.#status.lastSyncedAt = Number(tsRow);
			}

			// Restore cursors
			const cursorsJson = await this.#getMeta("cursors");
			if (cursorsJson) {
				try {
					this.#cursors = JSON.parse(cursorsJson);
				} catch {
					/* ignore corrupt data */
				}
			}

			this.#isInitialized = true;
		})();

		try {
			await this.#initPromise;
		} catch (error) {
			this.#initPromise = null;
			throw error;
		}
	}

	async #ensureInitialized(): Promise<void> {
		// Public APIs like `pull()` and `queueMutation()` can be called directly by
		// consumers, so initialization has to be self-healing instead of relying on
		// callers to remember a separate startup sequence.
		if (this.#isInitialized) return;
		if (this.#initPromise) {
			await this.#initPromise;
			return;
		}
		await this.init();
	}

	/**
	 * Pull data from the server into the local database.
	 *
	 * Uses the dedicated `/api/sync/pull` endpoint which handles:
	 * - Cursor-based pagination (resumes where it left off)
	 * - Delta sync via `updated_at` (only changed rows)
	 * - Delete detection via server-side `row_audit`
	 * - Schema migration detection via `ddl_audit`
	 * - RLS enforcement (only returns rows the user can read)
	 */
	async pull(): Promise<void> {
		await this.#ensureInitialized();
		if (!this.#canSyncRemotely()) return;
		if (!this.#status.isOnline) {
			this.#logger.debug("Skipping pull while offline");
			return;
		}

		if (this.#activePullPromise) {
			return this.#activePullPromise;
		}

		const pullPromise = (async () => {
			this.#logger.debug("Starting pull sync", { tables: this.#tables });
			this.#setStatus({ isSyncing: true, error: null });

			try {
				// Pull pagination mutates the shared cursor object over multiple round
				// trips, so we serialize active pulls behind one promise instead of
				// letting concurrent callers race the cursor state.
				const { syncedAt, didChange } = await this.#runPull({
					tables: this.#tables,
					cursors: this.#cursors,
					persistCursors: true,
				});

				// Record sync timestamp from the SERVER's clock, not the client's.
				// row_audit.timestamp is set by the server, so `since` must match.
				await this.#setMeta("last_synced_at", String(syncedAt));
				this.#clearSatisfiedQueuedReconciles(this.#tables);
				this.#setStatus({ isSyncing: false, lastSyncedAt: syncedAt });
				if (didChange) {
					try {
						await this.#invalidateHttpCache();
					} catch (err) {
						this.#logger.warn("Failed to invalidate HTTP cache", err);
					}
					try {
						const { clearQueryCache } = await import("../query-cache.js");
						clearQueryCache();
					} catch (err) {
						this.#logger.debug("Failed to clear in-memory query cache", err);
					}
					this.notifyDataChange();
				}
				this.#scheduleQueuedReconciles();
			} catch (err) {
				const error = err instanceof Error ? err : new Error(String(err));
				this.#setStatus({ isSyncing: false, error });
				throw error;
			} finally {
				this.#activePullPromise = null;
			}
		})();

		this.#activePullPromise = pullPromise;
		return pullPromise;
	}

	/**
	 * Push pending mutations to the server (last-write-wins).
	 * After pushing, triggers a pull to reconcile local state.
	 */
	async push(): Promise<{ pushed: number; failed: number }> {
		await this.#ensureInitialized();
		if (!this.#canSyncRemotely()) {
			return { pushed: 0, failed: 0 };
		}
		if (!this.#status.isOnline) {
			this.#logger.debug("Skipping push while offline");
			return { pushed: 0, failed: 0 };
		}

		this.#logger.debug("Starting push sync");
		this.#setStatus({ isSyncing: true, error: null });

		let pushed = 0;
		let failed = 0;

		try {
			const mutations = await this.#getPendingMutations();

			for (const mutation of mutations) {
				try {
					// Params are stored as JSON so queued writes stay transport-agnostic
					// across browser, Node, and replica runtimes.
					const params = JSON.parse(mutation.params) as unknown[];
					await this.#executeRemote(mutation.sql, params);

					await this.#adapter.execute(
						"DELETE FROM __ffdb_mutations WHERE id = ?",
						[mutation.id],
					);
					pushed++;
				} catch (err) {
					this.#logger.warn("Failed to push queued mutation", {
						mutationId: mutation.id,
						error: err,
					});
					failed++;
				}
			}

			await this.#refreshPendingCount();

			if (pushed > 0) {
				await this.pull();
			} else {
				this.#setStatus({ isSyncing: false });
			}

			return { pushed, failed };
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));
			this.#setStatus({ isSyncing: false, error });
			throw error;
		}
	}

	/** Full sync: push pending mutations, then pull fresh data. */
	async sync(): Promise<{ pushed: number; failed: number }> {
		await this.#ensureInitialized();
		if (!this.#canSyncRemotely()) {
			return { pushed: 0, failed: 0 };
		}
		if (!this.#status.isOnline) {
			this.#logger.debug("Skipping sync while offline");
			return { pushed: 0, failed: 0 };
		}
		if (this.#status.isSyncing) {
			return { pushed: 0, failed: 0 };
		}

		const pending = await this.#getPendingMutations();
		if (pending.length > 0) {
			return this.push();
		}
		await this.pull();
		return { pushed: 0, failed: 0 };
	}

	/** Queue a mutation for later sync. Called by OfflineDriver on writes. */
	async queueMutation(sql: string, params: unknown[]): Promise<void> {
		await this.#ensureInitialized();
		await this.#adapter.execute(
			"INSERT INTO __ffdb_mutations (sql, params, created_at) VALUES (?, ?, ?)",
			[sql, JSON.stringify(params), Date.now()],
		);
		await this.#refreshPendingCount();
		this.notifyDataChange();
	}

	/** Stop all auto-sync timers and event listeners. */
	destroy(): void {
		this.#destroyed = true;

		if (this.#intervalTimer) {
			clearInterval(this.#intervalTimer);
			this.#intervalTimer = null;
		}

		for (const cleanup of this.#cleanupFns) {
			cleanup();
		}
		this.#cleanupFns = [];
		this.#listeners.clear();
		this.#dataListeners.clear();
	}

	/* ─── Private: Sync Endpoint ─── */

	async #runPull(options: {
		tables?: string[];
		cursors: Record<string, unknown>;
		persistCursors: boolean;
	}): Promise<{ syncedAt: number; didChange: boolean }> {
		const { tables, cursors, persistCursors } = options;
		const lastSyncedAt = this.#status.lastSyncedAt;
		const shouldPruneMissingTables = this.#shouldPruneMissingTables(tables);
		let allDone = false;
		let serverSyncedAt = Date.now();
		let didChange = false;

		while (!allDone) {
			// Each iteration fetches one page per table. Tables that still have
			// `hasMore = true` keep their individual cursor while finished tables stay
			// untouched until the loop drains.
			let response = await this.#fetchSyncPull(lastSyncedAt, tables, cursors);
			serverSyncedAt = response.syncedAt ?? serverSyncedAt;

			if (shouldPruneMissingTables) {
				const prunedTables = await this.#syncTrackedTables(
					Object.keys(response.tables),
				);
				if (prunedTables) {
					didChange = true;
				}
			}

			if (
				response.schemaChanged ||
				(lastSyncedAt === null && Object.keys(cursors).length === 0)
			) {
				await this.#applySchema(response);
				didChange = true;
			}

			allDone = true;
			try {
				for (const [tableName, result] of Object.entries(response.tables)) {
					let tableDidChange = await this.#applyTableResult(tableName, result);
					if (!tableDidChange) {
						// Some delta strategies can legitimately report "no changes" even when a
						// local cache is slightly behind. A small key probe lets us repair that
						// drift without jumping straight to a full table rebuild.
						tableDidChange = await this.#backfillMissingRowsWithKeyProbe(
							tableName,
							result,
						);
					}
					if (tableDidChange) {
						didChange = true;
					}

					if (await this.#shouldRefreshTableFromScratch(tableName, result)) {
						await this.#refreshTableFromScratch(tableName, result.rowCount);
						didChange = true;
					}

					if (result.cursor !== null) {
						cursors[tableName] = result.cursor;
					}

					if (result.hasMore) {
						allDone = false;
					}
				}
			} catch (err) {
				if (lastSyncedAt !== null && this.#isMissingTableError(err)) {
					// If a delta pull references a table that the local cache no longer has,
					// fall back to a fresh schema pull and replay the page once. This keeps
					// cold-start or repaired replicas from getting stuck on missing-table
					// errors forever.
					response = await this.#fetchSyncPull(null, tables, cursors);
					serverSyncedAt = response.syncedAt ?? serverSyncedAt;
					if (shouldPruneMissingTables) {
						const prunedTables = await this.#syncTrackedTables(
							Object.keys(response.tables),
						);
						if (prunedTables) {
							didChange = true;
						}
					}
					await this.#applySchema(response);
					didChange = true;
					allDone = true;

					for (const [tableName, result] of Object.entries(response.tables)) {
						let tableDidChange = await this.#applyTableResult(
							tableName,
							result,
						);
						if (!tableDidChange) {
							tableDidChange = await this.#backfillMissingRowsWithKeyProbe(
								tableName,
								result,
							);
						}
						if (tableDidChange) {
							didChange = true;
						}

						if (result.cursor !== null) {
							cursors[tableName] = result.cursor;
						}

						if (result.hasMore) {
							allDone = false;
						}
					}
				} else {
					throw err;
				}
			}

			if (allDone) {
				// Cursors are only meaningful while a table still has more pages to read.
				// Clearing them after a complete pass avoids resuming from stale cursors
				// on the next full pull.
				for (const key of Object.keys(cursors)) {
					delete cursors[key];
				}
			}

			if (persistCursors) {
				this.#cursors = { ...cursors };
				await this.#setMeta("cursors", JSON.stringify(this.#cursors));
			}
		}

		return { syncedAt: serverSyncedAt, didChange };
	}

	async #fetchSyncPull(
		since: number | null,
		tables = this.#tables,
		cursors = this.#cursors,
		pageSize = this.#pageSize,
	): Promise<SyncPullResponse> {
		const filteredTables = this.#filterTables(tables);
		if (filteredTables && filteredTables.length === 0) {
			// Treat a fully filtered request as a successful no-op so callers do not
			// need special handling for "all candidate tables were excluded".
			return {
				tables: {},
				syncedAt: Date.now(),
				schemaChanged: false,
			};
		}

		const { data, error } = (await this.#fetch("/api/sync/pull", {
			method: "POST",
			body: {
				tables: filteredTables,
				since,
				cursors,
				pageSize,
			},
		})) as {
			data: { data: SyncPullResponse } | null;
			error: { statusText?: string; message?: string } | null;
		};

		if (error || !data?.data) {
			throw new Error(
				`Sync pull failed: ${error?.message ?? error?.statusText ?? "Unknown error"}`,
			);
		}

		// Server responses can still include internal/auth tables depending on sync
		// policy configuration. Filter once here so the rest of the pipeline only
		// works with application-visible tables.
		return {
			...data.data,
			tables: this.#filterSyncTables(data.data.tables),
		};
	}

	/**
	 * Create or recreate local tables from column metadata in the sync response.
	 * Only runs on first sync or when the server reports schemaChanged = true.
	 */
	async #applySchema(response: SyncPullResponse): Promise<void> {
		for (const [tableName, result] of Object.entries(response.tables)) {
			if (!result.columns) continue;
			// Recreate instead of migrating in place so first sync and schema-change
			// syncs both converge to exactly what the server declared.
			await this.#recreateLocalTable(tableName, result.columns);
		}
	}

	/**
	 * Apply a single table's sync result: upsert changed rows, delete removed rows.
	 */
	async #applyTableResult(
		tableName: string,
		result: SyncTableResult,
	): Promise<boolean> {
		let didChange = false;
		let processedRows = 0;

		// If we got fresh column metadata, it's already cached by #applySchema.
		// Otherwise load from cache or from the persisted local SQLite table.
		let columns = this.#schemaCache.get(tableName);
		if (!columns && result.columns) {
			await this.#recreateLocalTable(tableName, result.columns);
			columns = result.columns;
		}
		if (!columns) {
			columns = await this.#loadLocalTableSchema(tableName);
		}

		if (result.upserts.length > 0) {
			const hasLocalTable = await this.#localTableExists(tableName);
			if (!hasLocalTable) {
				// Let the outer pull loop decide whether to retry with a full schema pull.
				throw new Error(`missing_local_table:${tableName}`);
			}
		}

		// Upserts
		if (result.upserts.length > 0) {
			const sampleRow = result.upserts[0];
			const colNames = columns
				? columns.map((c) => c.name)
				: Object.keys(sampleRow);
			const quotedCols = colNames.map((c) => `"${c}"`).join(", ");
			const placeholders = colNames.map(() => "?").join(", ");
			const sql = `INSERT OR REPLACE INTO "${tableName}" (${quotedCols}) VALUES (${placeholders})`;
			const primaryKey = columns?.find((c) => c.primaryKey)?.name;

			for (const row of result.upserts) {
				if (
					primaryKey &&
					!(await this.#shouldApplyUpsert(tableName, primaryKey, row, colNames))
				) {
					// Skip byte-for-byte identical rows so noisy pulls do not churn the local
					// database or trigger downstream listeners unnecessarily.
					continue;
				}

				const values = colNames.map((c) => row[c] ?? null);
				await this.#adapter.execute(sql, values);
				didChange = true;
				processedRows += 1;
				if (processedRows % 100 === 0) {
					await this.#yieldToMainThread();
				}
			}
		}

		// Deletes
		if (result.deletes.length > 0) {
			const pk = columns?.find((c) => c.primaryKey);
			if (pk) {
				for (const id of result.deletes) {
					const { rows } = await this.#adapter.execute(
						`SELECT * FROM "${tableName}" WHERE "${pk.name}" = ? LIMIT 1`,
						[id],
					);

					await this.#adapter.execute(
						`DELETE FROM "${tableName}" WHERE "${pk.name}" = ?`,
						[id],
					);
					if (rows.length > 0) {
						didChange = true;
					}
					processedRows += 1;
					if (processedRows % 100 === 0) {
						await this.#yieldToMainThread();
					}
				}
			}
		}

		return didChange;
	}

	async #shouldApplyUpsert(
		tableName: string,
		primaryKey: string,
		incomingRow: Record<string, unknown>,
		colNames: string[],
	): Promise<boolean> {
		// Upsert deduplication is only possible when the row has a stable primary
		// key. Rows without one are applied pessimistically.
		const id = incomingRow[primaryKey];
		if (id === undefined || id === null) return true;

		const { rows } = await this.#adapter.execute(
			`SELECT * FROM "${tableName}" WHERE "${primaryKey}" = ? LIMIT 1`,
			[id],
		);
		if (rows.length === 0) return true;

		const existingRow = rows[0] as Record<string, unknown>;
		return colNames.some(
			(columnName) =>
				!this.#valuesEqual(
					existingRow[columnName] ?? null,
					incomingRow[columnName] ?? null,
				),
		);
	}

	#valuesEqual(left: unknown, right: unknown): boolean {
		// JSON comparison is intentionally pragmatic here because sync payloads are
		// JSON-compatible values already. If this starts producing false positives,
		// this is the place to swap in a more structured comparator.
		return JSON.stringify(left) === JSON.stringify(right);
	}

	async #yieldToMainThread(): Promise<void> {
		// Large sync batches can block rendering in the browser. Yield periodically
		// so big table rebuilds stay responsive without complicating the row loop.
		if (typeof window !== "undefined") {
			await new Promise<void>((resolve) => {
				setTimeout(resolve, 0);
			});
			return;
		}

		await Promise.resolve();
	}

	#quoteIdentifier(identifier: string): string {
		return `"${identifier.replaceAll('"', '""')}"`;
	}

	#filterTables(tables?: string[]): string[] | undefined {
		// Internal SQLite tables and explicitly skipped tables should never enter the
		// sync request, even if callers passed them directly.
		if (!tables) return undefined;
		return tables.filter(
			(table) =>
				!this.#skipTables.has(table) && !table.toLowerCase().includes("sqlite"),
		);
	}

	#filterSyncTables(
		tables: Record<string, SyncTableResult>,
	): Record<string, SyncTableResult> {
		return Object.fromEntries(
			Object.entries(tables).filter(
				([tableName]) =>
					!this.#skipTables.has(tableName) &&
					!tableName.toLowerCase().includes("sqlite"),
			),
		);
	}

	#shouldPruneMissingTables(tables?: string[]): boolean {
		if (!tables) return true;
		if (!this.#tables) return false;
		const filteredTables = this.#filterTables(tables) ?? [];
		if (filteredTables.length !== this.#tables.length) return false;
		const activeTables = new Set(this.#tables);
		return filteredTables.every((tableName) => activeTables.has(tableName));
	}

	async #syncTrackedTables(incomingTables: string[]): Promise<boolean> {
		// Track which application tables the server currently considers part of the
		// sync surface so removed tables can be pruned locally on later pulls.
		const trackedTables = await this.#getTrackedTables();
		const localTables = await this.#listLocalTables();
		const knownTables = new Set([...trackedTables, ...localTables]);
		let didChange = false;

		for (const tableName of knownTables) {
			if (!incomingTables.includes(tableName)) {
				await this.#dropLocalTable(tableName);
				didChange = true;
			}
		}

		await this.#setTrackedTables(incomingTables);
		return didChange;
	}

	async #backfillMissingRowsWithKeyProbe(
		tableName: string,
		result: SyncTableResult,
	): Promise<boolean> {
		// Only attempt this narrow repair after at least one completed sync. During
		// initial hydration the absence of rows is usually just part of the first
		// full pull, not evidence of drift.
		if (this.#status.lastSyncedAt === null) return false;
		if (this.#status.pendingMutations > 0) return false;
		if (typeof result.rowCount !== "number") return false;
		if (result.hasMore) return false;
		if (result.upserts.length > 0 || result.deletes.length > 0) return false;

		const localCount = await this.#getLocalRowCount(tableName);
		if (localCount === null || localCount >= result.rowCount) return false;

		const rowGap = result.rowCount - localCount;
		if (rowGap > LARGE_RECONCILE_GAP_THRESHOLD) {
			return false;
		}

		const columns =
			this.#schemaCache.get(tableName) ??
			(await this.#loadLocalTableSchema(tableName));
		const primaryKey = columns?.find((column) => column.primaryKey)?.name;
		if (!primaryKey) return false;

		const updatedAtColumn = columns?.find(
			(column) => column.name === "updated_at" || column.name === "updatedAt",
		)?.name;
		const probeSize = Math.min(
			128,
			Math.max(16, (result.rowCount - localCount) * 4),
		);
		const orderBy = updatedAtColumn
			? `${this.#quoteIdentifier(updatedAtColumn)} DESC, ${this.#quoteIdentifier(primaryKey)} DESC`
			: `${this.#quoteIdentifier(primaryKey)} DESC`;

		for (let offset = 0; offset < probeSize * 3; offset += probeSize) {
			// Probe the newest keys first because recent rows are the most likely place
			// for a delta/local-count mismatch to show up.
			const manifestSql = updatedAtColumn
				? `SELECT ${this.#quoteIdentifier(primaryKey)}, ${this.#quoteIdentifier(updatedAtColumn)} FROM ${this.#quoteIdentifier(tableName)} ORDER BY ${orderBy} LIMIT ? OFFSET ?`
				: `SELECT ${this.#quoteIdentifier(primaryKey)} FROM ${this.#quoteIdentifier(tableName)} ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
			const manifestRows = await this.remoteQuery(manifestSql, [
				probeSize,
				offset,
			]);
			if (manifestRows.length === 0) return false;

			const missingIds: unknown[] = [];
			for (const row of manifestRows) {
				const id = row[primaryKey];
				if (id === undefined || id === null) continue;

				const { rows: localRows } = await this.#adapter.execute(
					`SELECT * FROM ${this.#quoteIdentifier(tableName)} WHERE ${this.#quoteIdentifier(primaryKey)} = ? LIMIT 1`,
					[id],
				);
				const localRow = localRows[0] as Record<string, unknown> | undefined;
				if (!localRow) {
					missingIds.push(id);
					continue;
				}

				if (
					updatedAtColumn &&
					!this.#valuesEqual(
						localRow[updatedAtColumn] ?? null,
						row[updatedAtColumn] ?? null,
					)
				) {
					missingIds.push(id);
				}
			}

			if (missingIds.length === 0) {
				if (manifestRows.length < probeSize) return false;
				continue;
			}

			const placeholders = missingIds.map(() => "?").join(", ");
			const repairedRows = await this.remoteQuery(
				`SELECT * FROM ${this.#quoteIdentifier(tableName)} WHERE ${this.#quoteIdentifier(primaryKey)} IN (${placeholders})`,
				missingIds,
			);
			if (repairedRows.length === 0) return false;

			return this.#applyTableResult(tableName, {
				...result,
				upserts: repairedRows,
				deletes: [],
			});
		}

		return false;
	}

	async #shouldRefreshTableFromScratch(
		tableName: string,
		result: SyncTableResult,
	): Promise<boolean> {
		// Only self-heal after we already had a completed sync.
		// This avoids overreacting during first-load hydration.
		if (this.#status.lastSyncedAt === null) return false;
		if (this.#status.pendingMutations > 0) return false;
		if (typeof result.rowCount !== "number") return false;
		if (result.hasMore) return false;

		// If the server actually sent changes, let those reconcile first.
		if (result.upserts.length > 0 || result.deletes.length > 0) return false;

		const localCount = await this.#getLocalRowCount(tableName);
		if (localCount === null) return false;

		// Keep this intentionally conservative: rebuild when the client has
		// extra local rows, or when it is missing a large enough gap of rows that
		// a key-probe repair would create more request noise than a single refresh.
		if (localCount > result.rowCount) {
			return true;
		}

		return result.rowCount - localCount > LARGE_RECONCILE_GAP_THRESHOLD;
	}

	async #refreshTableFromScratch(
		tableName: string,
		expectedRowCount?: number,
	): Promise<void> {
		const repairCursors: Record<string, unknown> = {};
		let hasMore = true;
		let isFirstPage = true;
		const repairPageSize = Math.min(
			this.#maxRowsPerTable,
			Math.max(this.#pageSize, expectedRowCount ?? this.#pageSize),
		);

		this.#logger.info("Local count mismatch detected; rebuilding table", {
			tableName,
		});

		while (hasMore) {
			// A scratch rebuild intentionally ignores the normal `since` cursor so the
			// local table can be replaced by a clean full snapshot from the server.
			const response = await this.#fetchSyncPull(
				null,
				[tableName],
				repairCursors,
				repairPageSize,
			);
			if (isFirstPage || response.schemaChanged) {
				await this.#applySchema(response);
			}
			const result = response.tables[tableName];
			if (!result) break;

			await this.#applyTableResult(tableName, result);
			hasMore = result.hasMore;
			isFirstPage = false;

			if (result.cursor !== null) {
				repairCursors[tableName] = result.cursor;
			}
		}
	}

	async #getLocalRowCount(tableName: string): Promise<number | null> {
		const hasLocalTable = await this.#localTableExists(tableName);
		if (!hasLocalTable) return 0;

		const { rows } = await this.#adapter.execute(
			`SELECT COUNT(*) as count FROM "${tableName}"`,
		);
		return Number(rows[0]?.count ?? 0);
	}

	async #loadLocalTableSchema(
		tableName: string,
	): Promise<SyncColumnMeta[] | undefined> {
		const { rows } = await this.#adapter.execute(
			`PRAGMA table_info("${tableName}")`,
		);

		if (rows.length === 0) return undefined;

		const columns = rows.map((row) => ({
			name: String(row.name),
			type: String(row.type ?? "TEXT"),
			notNull: Number(row.notnull ?? 0) === 1,
			primaryKey: Number(row.pk ?? 0) === 1,
		}));

		this.#schemaCache.set(tableName, columns);
		return columns;
	}

	async #recreateLocalTable(
		tableName: string,
		columns: SyncColumnMeta[],
	): Promise<void> {
		this.#schemaCache.set(tableName, columns);

		// The local replica only needs enough schema fidelity to store synced rows
		// and preserve primary/not-null constraints that affect merge behavior.
		const cols = columns
			.map((c) => {
				const parts = [`"${c.name}" ${c.type}`];
				if (c.primaryKey) parts.push("PRIMARY KEY");
				if (c.notNull) parts.push("NOT NULL");
				return parts.join(" ");
			})
			.join(", ");

		await this.#adapter.execute(`DROP TABLE IF EXISTS "${tableName}"`);
		await this.#adapter.execute(`CREATE TABLE "${tableName}" (${cols})`);
	}

	async #dropLocalTable(tableName: string): Promise<void> {
		this.#schemaCache.delete(tableName);
		await this.#adapter.execute(`DROP TABLE IF EXISTS "${tableName}"`);
	}

	async #listLocalTables(): Promise<string[]> {
		const { rows } = await this.#adapter.execute(
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '__ffdb_%' AND name NOT LIKE 'sqlite_%'",
		);
		return rows
			.map((row) => String(row.name ?? ""))
			.filter(
				(tableName) =>
					tableName.length > 0 &&
					!this.#skipTables.has(tableName) &&
					!tableName.toLowerCase().includes("sqlite"),
			);
	}

	async #localTableExists(tableName: string): Promise<boolean> {
		const { rows } = await this.#adapter.execute(
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
			[tableName],
		);
		return rows.length > 0;
	}

	#isMissingTableError(err: unknown): boolean {
		const message = err instanceof Error ? err.message : String(err);
		return (
			message.includes("no such table") ||
			message.includes("missing_local_table:")
		);
	}

	/* ─── Private: Remote Mutations ─── */

	async #executeRemote(sql: string, params: unknown[]): Promise<void> {
		// Pushes use the general policy endpoint rather than the sync endpoint so
		// queued writes flow through the same authorization path as live writes.
		const { error } = (await this.#fetch(this.#endpoint, {
			method: "POST",
			body: { sql, values: params },
		})) as { data: unknown; error: { statusText?: string } | null };

		if (error) {
			throw new Error(error.statusText ?? "Remote query failed");
		}
	}

	/* ─── Private: Status & Meta ─── */

	#setStatus(partial: Partial<SyncStatus>) {
		// Status updates fan out through a tiny observer API so frameworks can build
		// whatever reactive wrapper they want on top without the sync layer knowing
		// about React/Vue/etc.
		Object.assign(this.#status, partial);
		for (const listener of this.#listeners) {
			try {
				listener();
			} catch {
				/* swallow */
			}
		}
	}

	async #refreshPendingCount(): Promise<void> {
		const { rows } = await this.#adapter.execute(
			"SELECT COUNT(*) as count FROM __ffdb_mutations",
		);
		this.#status.pendingMutations = Number(rows[0]?.count ?? 0);
	}

	async #getMeta(key: string): Promise<string | null> {
		const { rows } = await this.#adapter.execute(
			"SELECT value FROM __ffdb_sync_meta WHERE key = ?",
			[key],
		);
		return rows.length > 0 ? String(rows[0].value) : null;
	}

	async #getTrackedTables(): Promise<string[]> {
		const value = await this.#getMeta(TRACKED_TABLES_META_KEY);
		if (!value) return [];
		try {
			const parsed = JSON.parse(value);
			return Array.isArray(parsed)
				? parsed.filter((table): table is string => typeof table === "string")
				: [];
		} catch {
			return [];
		}
	}

	async #setTrackedTables(tables: string[]): Promise<void> {
		await this.#setMeta(
			TRACKED_TABLES_META_KEY,
			JSON.stringify([...new Set(tables)]),
		);
	}

	async #setMeta(key: string, value: string): Promise<void> {
		await this.#adapter.execute(
			"INSERT OR REPLACE INTO __ffdb_sync_meta (key, value) VALUES (?, ?)",
			[key, value],
		);
	}

	async #getPendingMutations(): Promise<MutationEntry[]> {
		const { rows } = await this.#adapter.execute(
			"SELECT id, sql, params, created_at FROM __ffdb_mutations ORDER BY id ASC",
		);
		return rows as unknown as MutationEntry[];
	}

	#clearSatisfiedQueuedReconciles(activeTables?: string[]): void {
		if (!this.#queuedReconcileTables.size) return;
		if (!activeTables || activeTables.length === 0) {
			this.#queuedReconcileTables.clear();
			return;
		}

		for (const table of activeTables) {
			this.#queuedReconcileTables.delete(table);
		}
	}

	/** Invalidate read-through HTTP cache after a remote pull updated local data. */
	async #invalidateHttpCache(): Promise<void> {
		try {
			await this.#adapter.execute("DELETE FROM __ffdb_http_cache");
		} catch (err) {
			// If the cache table doesn't exist or deletion fails, log and continue.
			this.#logger.debug("HTTP cache invalidate skipped or failed", err);
		}
	}

	#scheduleQueuedReconciles(): void {
		if (!this.#queuedReconcileTables.size || this.#destroyed) return;
		const tables = [...this.#queuedReconcileTables];
		this.#queuedReconcileTables.clear();
		queueMicrotask(() => {
			// Defer to the next microtask so a finishing sync can fully settle its own
			// status updates before a queued reconcile decides whether it should run.
			if (this.#destroyed || this.#status.isSyncing || !this.#status.isOnline) {
				for (const table of tables) {
					this.#queuedReconcileTables.add(table);
				}
				return;
			}
			void this.reconcileTables(tables).catch((err) =>
				this.#logger.warn("Queued reconcile failed", err),
			);
		});
	}

	/* ─── Private: Auto-Sync Setup ─── */

	#setupAutoSync(config: SyncManagerConfig): void {
		if (config.syncInterval && config.syncInterval > 0) {
			this.#intervalTimer = setInterval(() => {
				if (
					!this.#destroyed &&
					this.#status.isOnline &&
					!this.#status.isSyncing
				) {
					this.sync().catch((err) =>
						this.#logger.warn("Auto interval sync failed", err),
					);
				}
			}, config.syncInterval);
			(this.#intervalTimer as { unref?: () => void }).unref?.();
		}

		// Browser events are only the default fallback. Cross-platform runtimes can
		// provide a custom network adapter so sync state follows the host platform's
		// own connectivity APIs instead of assuming DOM globals exist.
		if (config.network) {
			this.#setupCustomNetwork(config);
		} else {
			this.#setupBrowserNetwork(config);
		}

		if (config.syncOnFocus && typeof document !== "undefined") {
			const onVisibilityChange = () => {
				if (
					document.visibilityState === "visible" &&
					!this.#destroyed &&
					this.#status.isOnline &&
					!this.#status.isSyncing
				) {
					this.sync().catch((err) =>
						this.#logger.warn("Auto focus sync failed", err),
					);
				}
			};

			document.addEventListener("visibilitychange", onVisibilityChange);
			this.#cleanupFns.push(() => {
				document.removeEventListener("visibilitychange", onVisibilityChange);
			});
		}
	}

	#setupCustomNetwork(config: SyncManagerConfig): void {
		const network = config.network as NetworkAdapter;

		// Custom adapters cover non-browser environments where connectivity is
		// exposed through runtime-specific APIs instead of global `online` events.
		// The initial reachability check may therefore be async.
		const initial = network.isOnline();
		if (initial instanceof Promise) {
			initial.then((online) => this.#setStatus({ isOnline: online }));
		} else {
			this.#status.isOnline = initial;
		}

		const unsub = network.subscribe((online) => {
			const wasOffline = !this.#status.isOnline;
			this.#setStatus({ isOnline: online });

			if (online && wasOffline && config.syncOnReconnect !== false) {
				if (!this.#destroyed && !this.#status.isSyncing) {
					this.sync().catch((err) =>
						this.#logger.warn("Auto reconnect sync failed", err),
					);
				}
			}
		});

		this.#cleanupFns.push(unsub);
	}

	#setupBrowserNetwork(config: SyncManagerConfig): void {
		if (
			typeof globalThis === "undefined" ||
			typeof globalThis.addEventListener !== "function"
		) {
			return;
		}

		// This path is intentionally browser-specific. Environments like Tauri,
		// Electron main, React Native, or other native shells should supply
		// `config.network` instead of relying on DOM connectivity events.
		if (config.syncOnReconnect !== false) {
			const onOnline = () => {
				this.#setStatus({ isOnline: true });
				if (!this.#destroyed && !this.#status.isSyncing) {
					this.sync().catch((err) =>
						this.#logger.warn("Auto reconnect sync failed", err),
					);
				}
			};

			const onOffline = () => {
				this.#setStatus({ isOnline: false });
			};

			globalThis.addEventListener("online", onOnline);
			globalThis.addEventListener("offline", onOffline);
			this.#cleanupFns.push(() => {
				globalThis.removeEventListener("online", onOnline);
				globalThis.removeEventListener("offline", onOffline);
			});
		}
	}
}
