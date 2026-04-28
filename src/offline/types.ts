/**
 * Adapter for a local SQLite database.
 *
 * Implement this for your environment:
 * - Browser:       sql.js (Wasm), OPFS
 * - React Native:  expo-sqlite, op-sqlite
 * - Node.js:       better-sqlite3
 *
 * All methods may be sync or async.
 */
import type { CompiledQuery } from "kysely";

export type OfflineAdapter = {
	/** Execute a SQL statement and return rows. */
	execute(
		sql: string,
		params?: unknown[],
		meta?: CompiledQuery,
	):
		| Promise<{ rows: Record<string, unknown>[] }>
		| { rows: Record<string, unknown>[] };

	/** Optional: close/cleanup the local database. */
	close?(): Promise<void> | void;
};

/**
 * Adapter for network status detection.
 *
 * Implement this for your environment:
 * - Browser:       not needed (uses online/offline events automatically)
 * - React Native:  @react-native-community/netinfo
 * - Electron:      not needed (uses Chromium's online/offline events)
 * - Tauri:         @tauri-apps/plugin-network
 * - Node.js:       custom (poll an endpoint, or always-online)
 *
 * If not provided, falls back to browser `online`/`offline` events.
 * If those aren't available either (e.g. plain Node), assumes always online.
 */
export type NetworkAdapter = {
	/** Check if the device is currently online. */
	isOnline(): boolean | Promise<boolean>;

	/**
	 * Subscribe to connectivity changes.
	 * The listener receives `true` when online, `false` when offline.
	 * Returns an unsubscribe function.
	 */
	subscribe(listener: (online: boolean) => void): () => void;
};

export type OfflineConfig = {
	/** The local SQLite adapter. */
	adapter: OfflineAdapter;

	/**
	 * Custom network status adapter.
	 *
	 * If omitted, falls back to browser `online`/`offline` events.
	 * Pass this for React Native (@react-native-community/netinfo),
	 * Tauri, or custom Node.js network detection.
	 */
	network?: NetworkAdapter;

	/**
	 * Tables to sync. Defaults to all tables from the server schema.
	 * Internal tables (__ffdb_*) are always created regardless.
	 */
	tables?: string[];

	/**
	 * Tables to exclude from sync.
	 * Useful for very large, server-derived, or low-value tables that should
	 * stay remote-only and never be hydrated into the local offline cache.
	 *
	 * Internal auth/system tables such as account, apikey, policies,
	 * query_metrics, row_audit, session, system_metrics, table_counts,
	 * verification, plus anything with sqlite in the name, are always
	 * excluded by default.
	 */
	skipTables?: string[];

	/**
	 * Sync data from the server on initial connect.
	 * Default: true
	 */
	syncOnConnect?: boolean;

	/**
	 * Auto-sync on an interval (milliseconds).
	 * Runs a full sync (push pending mutations, then pull) every N ms.
	 * Default: undefined (no interval sync)
	 *
	 * @example 30_000 // sync every 30 seconds
	 */
	syncInterval?: number;

	/**
	 * Auto-sync when the device comes back online.
	 * Uses `window.addEventListener('online', ...)` in browsers.
	 * Default: true
	 */
	syncOnReconnect?: boolean;

	/**
	 * Auto-sync when the window regains focus (browser only).
	 * Uses `document.addEventListener('visibilitychange', ...)`.
	 * Default: false
	 */
	syncOnFocus?: boolean;

	/**
	 * Maximum rows to pull per table during sync.
	 * Default: 10000
	 */
	maxRowsPerTable?: number;

	/**
	 * Rows per page when paginating sync pulls.
	 * The client pages through each table using cursor-based pagination
	 * until all rows are fetched or maxRowsPerTable is reached.
	 * Default: 500
	 */
	pageSize?: number;
};

export type SyncStatus = {
	/** Whether a sync is currently in progress. */
	isSyncing: boolean;

	/** Whether the device is currently online. */
	isOnline: boolean;

	/** Timestamp of the last successful sync (ms since epoch), or null. */
	lastSyncedAt: number | null;

	/** Number of mutations waiting to be pushed. */
	pendingMutations: number;

	/** Last sync error, if any. */
	error: Error | null;
};

export type MutationEntry = {
	id: number;
	sql: string;
	params: string; // JSON-serialized
	created_at: number;
};
