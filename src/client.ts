import { apiKeyClient } from "@better-auth/api-key/client";
import { passkeyClient } from "@better-auth/passkey/client";
import { stripeClient } from "@better-auth/stripe/client";
import {
	type BetterAuthClientOptions,
	createAuthClient,
} from "better-auth/client";
import {
	adminClient,
	emailOTPClient,
	genericOAuthClient,
	magicLinkClient,
	organizationClient,
	twoFactorClient,
	type UserWithRole,
	usernameClient,
} from "better-auth/client/plugins";
import {
	CompiledQuery,
	Kysely,
	type Sql,
	SqliteAdapter,
	SqliteIntrospector,
	SqliteQueryCompiler,
	sql,
} from "kysely";
import { type AccessInfo, fetchAccess } from "./access.ts";
import { HttpDriver, type QueryMeta } from "./adapter.ts";
import { defaultConfig, type FFDB_Config } from "./config.ts";
import { createFetchClient, type FetchClient } from "./fetch.ts";
import { createClientLogger, type FFDBLogger } from "./logger.ts";
import { OfflineDriver } from "./offline/driver.ts";
import { SyncManager } from "./offline/sync.ts";
import type { OfflineConfig, SyncStatus } from "./offline/types.ts";
import { memoryStorage, type StorageAdapter } from "./storage.ts";
import type { Database as BasicDatabase } from "./types.ts";

const REFRESH_BUFFER_MS = 5 * 60 * 1000;
const API_KEY_TTL_MS = (60 * 60 * 60 * 1000) / 60;
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const OFFLINE_STATUS_TTL_MS = 15 * 1000;
const AUTH_CACHE_TABLE = "__ffdb_auth_cache";
const HTTP_CACHE_TABLE = "__ffdb_http_cache";

export type LifecycleHooks = {
	onCreated?: (destroy: () => Promise<void>) => void;
	onDestroyed?: () => void;
};

type FFDBAuthClientOptions = Omit<BetterAuthClientOptions, "plugins">;

export function createFFDBAuthPlugins(): [
	ReturnType<typeof apiKeyClient>,
	ReturnType<typeof passkeyClient>,
	ReturnType<typeof emailOTPClient>,
	ReturnType<typeof genericOAuthClient>,
	ReturnType<typeof magicLinkClient>,
	ReturnType<typeof organizationClient>,
	ReturnType<typeof twoFactorClient>,
	ReturnType<typeof usernameClient>,
	ReturnType<typeof adminClient>,
	ReturnType<typeof stripeClient>,
] {
	// Keep the shared client and the React wrappers on the same Better Auth plugin
	// surface so generated method names and inferred session shapes stay aligned
	// across runtimes.
	return [
		apiKeyClient(),
		passkeyClient(),
		emailOTPClient(),
		genericOAuthClient(),
		magicLinkClient(),
		organizationClient({
			teams: {
				enabled: true,
			},
		}),
		twoFactorClient(),
		usernameClient(),
		adminClient(),
		stripeClient({ subscription: true }),
	] as const;
}

export type AuthPluginList = ReturnType<typeof createFFDBAuthPlugins>;

type FFDBAuthClientConfig = FFDBAuthClientOptions & {
	plugins: AuthPluginList;
};

type PasskeyActionSurface = ReturnType<
	ReturnType<typeof passkeyClient>["getActions"]
>;

export type AuthSessionShape =
	BaseAuthClientInstance["$Infer"]["Session"] extends {
		session: infer TSession;
		user: infer TUser;
	}
		? {
				session: TSession;
				user: TUser & Pick<UserWithRole, "role">;
			}
		: {
				session: unknown;
				user: Pick<UserWithRole, "role">;
			};

function createFFDBAuthClient(options: FFDBAuthClientOptions = {}) {
	return createAuthClient<FFDBAuthClientConfig>({
		...options,
		plugins: createFFDBAuthPlugins(),
	});
}

type BaseAuthClientInstance = ReturnType<typeof createFFDBAuthClient>;

export type AuthClientInstance = Omit<
	BaseAuthClientInstance,
	"$Infer" | "useSession" | "apiKey" | "passkey" | "signIn"
> & {
	signIn: BaseAuthClientInstance["signIn"] & PasskeyActionSurface["signIn"];
	passkey: PasskeyActionSurface["passkey"] & {
		listPasskeys: (...args: any[]) => Promise<any>;
		deletePasskey: (...args: any[]) => Promise<any>;
		updatePasskey: (...args: any[]) => Promise<any>;
	};
	apiKey: {
		create: (...args: any[]) => Promise<any>;
		delete: (...args: any[]) => Promise<any>;
		list: (...args: any[]) => Promise<any>;
	};
	$Infer: Omit<BaseAuthClientInstance["$Infer"], "Session"> & {
		Session: AuthSessionShape;
	};
	getSession: (...args: any[]) => Promise<{
		data: AuthSessionShape | null;
		error: unknown;
	}>;
	waitForAuthReady: () => Promise<void>;
	signOut: (...args: any[]) => Promise<any>;
};

export type CreateClientOptions = {
	config?: Partial<FFDB_Config>;
	lifecycle?: LifecycleHooks;
	storage?: StorageAdapter;
	offline?: OfflineConfig;
	skipHealthCheck?: boolean;
};

export type ClientRequestOptions = RequestInit & {
	retries?: number;
	retryDelay?: number;
	retryOn?: number[];
	parse?: "json" | "text" | "blob";
	useCache?: boolean;
};

export type SyncHandle = {
	/** Friendly alias for a full sync cycle. */
	run: () => Promise<{ pushed: number; failed: number }>;
	/** Push pending mutations, then pull fresh data. */
	sync: () => Promise<{ pushed: number; failed: number }>;
	/** Pull fresh data from the server into the local database. */
	pull: () => Promise<void>;
	/** Push pending mutations to the server. */
	push: () => Promise<{ pushed: number; failed: number }>;
	/** Wait for the current sync to finish. */
	waitForIdle: (timeoutMs?: number) => Promise<void>;
	/** Current sync status. */
	readonly status: Readonly<SyncStatus>;
	/** Subscribe to sync status changes. */
	subscribe: (listener: () => void) => () => void;
};

export type SqlQueryInput<
	TValues extends readonly unknown[] = readonly unknown[],
> = {
	sql: string;
	values?: TValues;
	bypassCache?: boolean;
};

export type SqlHelper = Sql & {
	execute: <
		TRow extends Record<string, unknown> = Record<string, unknown>,
		TValues extends readonly unknown[] = readonly unknown[],
	>(
		input: SqlQueryInput<TValues>,
	) => Promise<{
		rows: TRow[];
		meta: QueryMeta;
	}>;
	query: <
		TRow extends Record<string, unknown> = Record<string, unknown>,
		TValues extends readonly unknown[] = readonly unknown[],
	>(
		input: SqlQueryInput<TValues>,
	) => Promise<TRow[]>;
	first: <
		TRow extends Record<string, unknown> = Record<string, unknown>,
		TValues extends readonly unknown[] = readonly unknown[],
	>(
		input: SqlQueryInput<TValues>,
	) => Promise<TRow | undefined>;
};

export type FFDBClient<ExtraDatabase = unknown> = {
	db: Kysely<BasicDatabase & ExtraDatabase>;
	sql: SqlHelper;
	auth: AuthClientInstance;
	request: <T = unknown>(
		url: string,
		options?: ClientRequestOptions,
	) => Promise<T>;
	destroy: () => Promise<void>;
	sync: SyncHandle | null;
	getAccess: (opts?: { force?: boolean }) => Promise<AccessInfo>;
	subscribe: (listener: () => void) => () => void;
};

async function isApiHealthy(
	$fetch: FetchClient,
	skip: boolean,
	logger?: FFDBLogger,
): Promise<boolean> {
	if (skip) return true;

	try {
		const { data, error } = (await $fetch("/api/health")) as {
			data: {
				status?: string;
				data?: { message: string };
			} | null;
			error: unknown;
		};

		if (data?.status === "ok") return true;

		logger?.warn("API health check failed", error ?? "Unknown error");
		return false;
	} catch (error) {
		logger?.warn("Error checking API health", error);
		return false;
	}
}

async function createDbClient<ExtraDatabase>(
	resolvedConfig: FFDB_Config,
	fetchFn: FetchClient,
	offlineDriver?: OfflineDriver,
) {
	type DB = BasicDatabase & ExtraDatabase;

	// Kysely always sees a SQLite dialect because both runtime modes expose a
	// SQLite-shaped surface: offline mode talks to the real local adapter, while
	// online mode serializes the compiled SQLite SQL to the backend policy-query
	// endpoint.
	return new Kysely<DB>({
		dialect: {
			createAdapter() {
				return new SqliteAdapter();
			},
			createDriver() {
				if (offlineDriver) return offlineDriver;
				return new HttpDriver({
					endpoint: resolvedConfig.endpoint,
					fetch: fetchFn,
				});
			},
			createIntrospector(db) {
				return new SqliteIntrospector(db);
			},
			createQueryCompiler() {
				return new SqliteQueryCompiler();
			},
		},
	});
}

export async function createClient<ExtraDatabase>(
	options: CreateClientOptions,
): Promise<FFDBClient<ExtraDatabase>> {
	type DB = BasicDatabase & ExtraDatabase;

	const {
		config: overrideConfig,
		lifecycle,
		storage = memoryStorage(),
		offline,
		skipHealthCheck = false,
	} = options;

	const resolvedConfig: FFDB_Config = { ...defaultConfig, ...overrideConfig };
	const configuredSessionToken = resolvedConfig.authToken.trim() || null;
	const configuredApiKey = resolvedConfig.apiKey.trim() || null;
	const hasStaticConfiguredAuth = Boolean(
		configuredSessionToken || configuredApiKey,
	);
	const logger = createClientLogger(resolvedConfig);
	const storageKey = `ffdb:api-key:${resolvedConfig.apiUrl}`;
	const storageIdKey = `ffdb:key-id:${resolvedConfig.apiUrl}`;
	const storageExpiresKey = `ffdb:key-expires:${resolvedConfig.apiUrl}`;
	const sessionTokenKey = `ffdb:session-token:${resolvedConfig.apiUrl}`;
	const sessionTokenExpiresKey = `ffdb:session-token-expires:${resolvedConfig.apiUrl}`;
	const sessionDataKey = `ffdb:session-data:${resolvedConfig.apiUrl}`;
	const offlineStatusKey = `ffdb:offline-status:${resolvedConfig.apiUrl}`;

	let currentSessionToken: string | null = configuredSessionToken;
	let currentApiKey: string | null = configuredApiKey;
	let shouldRefreshApiKey = false;
	let isOnline = true;
	let syncManager: SyncManager | null = null;
	let offlineDriver: OfflineDriver | null = null;
	let isDestroyed = false;
	let refreshTimer: ReturnType<typeof setTimeout> | null = null;
	let healthProbeTimer: ReturnType<typeof setInterval> | null = null;
	let authRecoveryPromise: Promise<boolean> | null = null;
	let authClient: AuthClientInstance | null = null;
	const healthProbeIntervalMs = Math.max(
		0,
		resolvedConfig.healthProbeIntervalMs ??
			defaultConfig.healthProbeIntervalMs ??
			0,
	);

	// Offline mode persists auth and small GET responses into the same adapter so a
	// browser refresh or desktop restart can keep working before the network comes
	// back.
	async function initOfflineCacheTables() {
		if (!offline?.adapter) return;
		await offline.adapter.execute(`
			CREATE TABLE IF NOT EXISTS ${AUTH_CACHE_TABLE} (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at INTEGER NOT NULL
			)
		`);
		await offline.adapter.execute(`
			CREATE TABLE IF NOT EXISTS ${HTTP_CACHE_TABLE} (
				cache_key TEXT PRIMARY KEY,
				payload TEXT NOT NULL,
				updated_at INTEGER NOT NULL
			)
		`);
	}

	// Storage is the primary persistence layer because it is available in every
	// runtime. The optional offline adapter mirrors those values into SQLite so the
	// offline package can survive environments where in-memory storage would be too
	// short-lived.
	async function getPersistedAuthValue(key: string): Promise<string | null> {
		const storedValue = (await storage.get(key)) as string | null;
		if (storedValue !== null) return storedValue;
		if (!offline?.adapter) return null;

		try {
			const { rows } = await offline.adapter.execute(
				`SELECT value FROM ${AUTH_CACHE_TABLE} WHERE key = ?`,
				[key],
			);
			return rows.length > 0 ? String(rows[0].value ?? "") : null;
		} catch {
			return null;
		}
	}

	async function setPersistedAuthValue(
		key: string,
		value: string,
	): Promise<void> {
		await storage.set(key, value);
		if (!offline?.adapter) return;
		try {
			await offline.adapter.execute(
				`INSERT OR REPLACE INTO ${AUTH_CACHE_TABLE} (key, value, updated_at) VALUES (?, ?, ?)`,
				[key, value, Date.now()],
			);
		} catch {
			/* ignore cache persistence errors */
		}
	}

	async function removePersistedAuthValue(key: string): Promise<void> {
		await storage.remove(key);
		if (!offline?.adapter) return;
		try {
			await offline.adapter.execute(
				`DELETE FROM ${AUTH_CACHE_TABLE} WHERE key = ?`,
				[key],
			);
		} catch {
			/* ignore cache cleanup errors */
		}
	}

	// Session payloads are stored separately from the bearer token so offline UI can
	// still render "who is signed in" without needing a live auth round-trip.
	const getStoredSessionData = async () => {
		const raw = await getPersistedAuthValue(sessionDataKey);
		if (!raw) return null;
		try {
			return JSON.parse(raw);
		} catch {
			return null;
		}
	};

	const setStoredSessionData = async (session: unknown) => {
		await setPersistedAuthValue(sessionDataKey, JSON.stringify(session));
	};

	type RequestValidationError = {
		message: string;
		field?: string;
	};

	type RequestError = Error & {
		status?: number;
		code?: string;
		errors?: RequestValidationError[];
		fieldErrors?: Record<string, string>;
		payload?: unknown;
	};

	async function buildResponseError(response: Response) {
		let payload: unknown = null;

		try {
			payload = await response.clone().json();
		} catch {
			try {
				payload = await response.text();
			} catch {
				payload = null;
			}
		}

		const structuredPayload =
			payload && typeof payload === "object"
				? (payload as Record<string, any>)
				: null;
		const errorShape =
			structuredPayload?.error && typeof structuredPayload.error === "object"
				? structuredPayload.error
				: structuredPayload;
		const errors = Array.isArray(errorShape?.errors)
			? (errorShape.errors as RequestValidationError[])
			: [];
		const fieldErrors = Object.fromEntries(
			errors
				.filter((entry) => entry.field && entry.message)
				.map((entry) => [entry.field as string, entry.message]),
		);
		const message =
			errorShape?.message ||
			(typeof payload === "string" && payload) ||
			`Request failed with status ${response.status}: ${response.statusText}`;

		const error = new Error(message) as RequestError;
		error.status = response.status;
		error.code = errorShape?.code;
		error.errors = errors;
		error.fieldErrors = fieldErrors;
		error.payload = payload;
		return error;
	}

	// The client deliberately treats transport failures as a runtime capability
	// change, not just a failed request. Once a request looks offline-ish, the rest
	// of the package starts serving cached state and pauses network-dependent sync.
	const isOfflineLikeError = (error: unknown) => {
		const message =
			error instanceof Error ? error.message : String(error ?? "");
		return /failed to fetch|network|err_connection_refused|load failed/i.test(
			message,
		);
	};

	const isUnauthorizedError = (error: unknown) => {
		if (!error) return false;
		if (typeof error === "object") {
			const candidate = error as {
				status?: number;
				statusCode?: number;
				response?: { status?: number };
				statusText?: string;
				message?: string;
			};
			const status =
				candidate.status ?? candidate.statusCode ?? candidate.response?.status;
			if (status === 401 || status === 403) return true;
			const text = `${candidate.statusText ?? ""} ${candidate.message ?? ""}`;
			return /\b(401|403)\b|unauthorized|forbidden/i.test(text);
		}
		return /\b(401|403)\b|unauthorized|forbidden/i.test(String(error));
	};

	// Most auth helpers run before the public client object is returned, so an
	// explicit guard here makes lifecycle mistakes fail loudly instead of becoming
	// null-reference bugs later.
	function requireAuthClient() {
		if (!authClient) {
			throw new Error("Auth client is not initialized");
		}
		return authClient;
	}

	// Unauthorized recovery is centralized so requests, sync pulls, and manual query
	// calls all respond the same way. Static tokens are never auto-rotated; browser
	// sessions are allowed to mint a replacement API key.
	async function recoverFromUnauthorized(): Promise<boolean> {
		if (isDestroyed) return false;
		if (authRecoveryPromise) return authRecoveryPromise;
		if (!authClient) return false;
		if (hasStaticConfiguredAuth) {
			logger.warn(
				"Configured static auth was rejected; disabling offline sync until auth is replaced",
			);
			syncManager?.setAuthReady(false);
			return false;
		}

		authRecoveryPromise = (async () => {
			try {
				shouldRefreshApiKey = true;
				const refreshed = await ensureSessionApiKey();
				if (!refreshed) {
					const session = await getAuthenticatedSession().catch(() => null);
					if (!session) {
						await clearStoredAuth();
					}
					syncManager?.setAuthReady(false);
					logger.warn("Failed to refresh auth after unauthorized response");
					return false;
				}
				return true;
			} catch (error) {
				logger.warn(
					"Unexpected error refreshing auth after unauthorized response",
					error,
				);
				return false;
			} finally {
				authRecoveryPromise = null;
			}
		})();

		return authRecoveryPromise;
	}

	// GET request caching is intentionally much narrower than SQL caching. It exists
	// only to keep a small amount of already-fetched JSON reachable during offline
	// transitions.
	async function readCachedHttpValue<T>(cacheKey: string): Promise<T | null> {
		if (!offline?.adapter) return null;
		try {
			const { rows } = await offline.adapter.execute(
				`SELECT payload FROM ${HTTP_CACHE_TABLE} WHERE cache_key = ?`,
				[cacheKey],
			);
			if (!rows.length) return null;
			return JSON.parse(String(rows[0].payload ?? "null")) as T;
		} catch {
			return null;
		}
	}

	async function writeCachedHttpValue(
		cacheKey: string,
		payload: unknown,
	): Promise<void> {
		if (!offline?.adapter) return;
		try {
			await offline.adapter.execute(
				`INSERT OR REPLACE INTO ${HTTP_CACHE_TABLE} (cache_key, payload, updated_at) VALUES (?, ?, ?)`,
				[cacheKey, JSON.stringify(payload), Date.now()],
			);
		} catch {
			/* ignore cache persistence errors */
		}
	}

	// The health probe loop only runs while the client believes it is offline. Its
	// job is to flip the runtime back online and optionally kick sync once the API is
	// reachable again.
	function stopHealthProbeLoop(): void {
		if (healthProbeTimer) {
			clearInterval(healthProbeTimer);
			healthProbeTimer = null;
		}
	}

	function startHealthProbeLoop(): void {
		if (
			!offline ||
			healthProbeIntervalMs <= 0 ||
			healthProbeTimer ||
			isDestroyed
		) {
			return;
		}

		healthProbeTimer = setInterval(() => {
			if (isDestroyed || isOnline) {
				if (isOnline) {
					stopHealthProbeLoop();
				}
				return;
			}

			void isApiHealthy($fetch, false, logger)
				.then(async (healthy) => {
					if (!healthy) return;
					await markOnline();
					if (offline?.syncOnReconnect ?? true) {
						await triggerSync();
					}
				})
				.catch(() => {
					/* still offline */
				});
		}, healthProbeIntervalMs);
		(healthProbeTimer as { unref?: () => void }).unref?.();
	}

	// Online state is shared across the request wrapper, auth restoration, and the
	// sync manager. Updating it in one place keeps those three pieces from drifting
	// into contradictory states.
	async function setOnlineStatus(nextOnline: boolean): Promise<void> {
		isOnline = nextOnline;
		syncManager?.setOnline(nextOnline);
		if (!offline) return;
		if (nextOnline) {
			await storage.remove(offlineStatusKey);
			stopHealthProbeLoop();
			return;
		}
		await storage.set(
			offlineStatusKey,
			String(Date.now() + OFFLINE_STATUS_TTL_MS),
		);
		startHealthProbeLoop();
	}

	async function markOffline(error?: unknown): Promise<void> {
		if (isOnline) {
			logger.info("Switching to offline mode", error);
		}
		await setOnlineStatus(false);
	}

	async function markOnline(): Promise<void> {
		if (!isOnline) {
			logger.info("Connection restored; switching back online");
			shouldRefreshApiKey = Boolean(currentSessionToken);
		}
		await setOnlineStatus(true);
	}

	function createManagedFetchWrapper(config: FFDB_Config) {
		const rawFetch = createFetchClient({ config });
		const runFetch = async <T>(
			url: string,
			request?: Record<string, unknown>,
			overrideConfig?: FFDB_Config,
		) => {
			const fetcher = overrideConfig
				? createFetchClient({ config: overrideConfig })
				: rawFetch;
			return (await fetcher<T>(url, request)) as {
				data: T | null;
				error: unknown;
			};
		};

		return async <T>(
			url: string,
			request?: Record<string, unknown>,
		): Promise<{ data: T | null; error: unknown }> => {
			try {
				let result = await runFetch<T>(url, request);

				// Recover once on auth failure so the higher-level callers do not each need
				// their own token refresh loop.
				if (result.error && isUnauthorizedError(result.error)) {
					const recovered = await recoverFromUnauthorized();
					if (recovered) {
						result = await runFetch<T>(url, request, {
							...resolvedConfig,
							authToken: currentSessionToken ?? "",
							apiKey: currentApiKey ?? "",
						});
					}
				}

				if (result.error && isOfflineLikeError(result.error)) {
					await markOffline(result.error);
				} else if (!result.error) {
					await markOnline();
				}
				return result;
			} catch (error) {
				if (isUnauthorizedError(error)) {
					const recovered = await recoverFromUnauthorized();
					if (recovered) {
						return await runFetch<T>(url, request, {
							...resolvedConfig,
							authToken: currentSessionToken ?? "",
							apiKey: currentApiKey ?? "",
						});
					}
				}
				if (isOfflineLikeError(error)) {
					await markOffline(error);
					return { data: null, error };
				}
				throw error;
			}
		};
	}

	await initOfflineCacheTables();

	const storedSessionToken = (await getPersistedAuthValue(sessionTokenKey)) as
		| string
		| null;
	const storedSessionTokenExpiresAt = Number(
		(await getPersistedAuthValue(sessionTokenExpiresKey)) ?? 0,
	);
	if (
		!currentSessionToken &&
		storedSessionToken &&
		storedSessionTokenExpiresAt > Date.now()
	) {
		currentSessionToken = storedSessionToken;
		logger.debug("Restored persisted session token", {
			apiUrl: resolvedConfig.apiUrl,
		});
	} else if (!currentSessionToken) {
		await removePersistedAuthValue(sessionTokenKey);
		await removePersistedAuthValue(sessionTokenExpiresKey);
	}

	const storedApiKey = (await getPersistedAuthValue(storageKey)) as
		| string
		| null;
	const storedApiKeyExpiresAt = Number(
		(await getPersistedAuthValue(storageExpiresKey)) ?? 0,
	);
	if (!currentApiKey && storedApiKey && storedApiKeyExpiresAt > Date.now()) {
		currentApiKey = storedApiKey;
		shouldRefreshApiKey = Boolean(currentSessionToken);
		logger.debug("Restored persisted API key", {
			apiUrl: resolvedConfig.apiUrl,
		});
	} else if (!currentApiKey) {
		await removePersistedAuthValue(storageKey);
		await removePersistedAuthValue(storageIdKey);
		await removePersistedAuthValue(storageExpiresKey);
	}

	const $fetchInitial = createManagedFetchWrapper({
		...resolvedConfig,
		authToken: currentSessionToken ?? "",
		apiKey: currentApiKey ?? "",
	});

	// A recent offline verdict is cached briefly so app start does not stall on the
	// same failing health check over and over while the backend is still down.
	if (offline) {
		const offlineCooldownUntil = Number(
			(await storage.get(offlineStatusKey)) ?? 0,
		);
		if (offlineCooldownUntil > Date.now()) {
			isOnline = false;
			logger.info("Using recent offline health status");
		} else {
			try {
				isOnline = await isApiHealthy($fetchInitial, skipHealthCheck, logger);
			} catch {
				isOnline = false;
			}
			if (isOnline) {
				await storage.remove(offlineStatusKey);
			} else {
				await storage.set(
					offlineStatusKey,
					String(Date.now() + OFFLINE_STATUS_TTL_MS),
				);
			}
		}
	} else {
		const isHealthy = await isApiHealthy(
			$fetchInitial,
			skipHealthCheck,
			logger,
		);
		if (!isHealthy) throw new Error("API is not healthy");
	}

	// Set up offline layer
	// The sync manager owns table replication and mutation replay, while the driver
	// owns local query execution. They are initialized separately so non-offline
	// clients can still use the rest of the package without carrying sync state.
	if (offline) {
		syncManager = new SyncManager({
			adapter: offline.adapter,
			fetchFn: $fetchInitial,
			endpoint: resolvedConfig.endpoint,
			tables: offline.tables,
			skipTables: offline.skipTables,
			maxRowsPerTable: offline.maxRowsPerTable ?? 10000,
			pageSize: offline.pageSize,
			syncInterval: offline.syncInterval,
			syncOnReconnect: offline.syncOnReconnect,
			syncOnFocus: offline.syncOnFocus,
			network: offline.network,
			logger,
		});

		syncManager.setOnline(isOnline);
		await syncManager.init();
		syncManager.setAuthReady(Boolean(currentSessionToken || currentApiKey));

		offlineDriver = new OfflineDriver({
			adapter: offline.adapter,
			syncManager,
		});
		await offlineDriver.init();
	}

	let $fetch = $fetchInitial;
	let dbClient = await createDbClient<ExtraDatabase>(
		{
			...resolvedConfig,
			authToken: currentSessionToken ?? "",
			apiKey: currentApiKey ?? "",
		},
		$fetch,
		offlineDriver ?? undefined,
	);

	// The public `db` object stays reference-stable even when auth refresh rebuilds
	// the underlying transport. That matters because React hooks and external code
	// often hold onto the original `db` reference for the life of the provider.
	const db = new Proxy({} as Kysely<DB>, {
		get(_target, prop) {
			const value = Reflect.get(dbClient as object, prop, dbClient);
			return typeof value === "function" ? value.bind(dbClient) : value;
		},
		has(_target, prop) {
			return prop in dbClient;
		},
	}) as Kysely<DB>;
	let authBootstrapPromise: Promise<void> | null = null;

	async function rebuildClients() {
		logger.debug("Rebuilding client internals for updated auth state");
		// Rebuild both fetch and db clients so every future request sees the newest
		// bearer token / API key pair without forcing consumers to recreate their own
		// client references.
		const nextConfig = {
			...resolvedConfig,
			authToken: currentSessionToken ?? "",
			apiKey: currentApiKey ?? "",
		};
		$fetch = createManagedFetchWrapper(nextConfig);

		if (syncManager) {
			syncManager.updateFetch($fetch);
		}

		dbClient = await createDbClient<ExtraDatabase>(
			nextConfig,
			$fetch,
			offlineDriver ?? undefined,
		);
	}

	// Session tokens and short-lived query API keys are tracked separately. The
	// session token proves identity to Better Auth, while the API key is the cheap
	// credential used by the query endpoint and sync loop.
	async function storeSessionToken(
		authToken: string,
		expiresAt = Date.now() + SESSION_TTL_MS,
	) {
		currentSessionToken = authToken;
		await setPersistedAuthValue(sessionTokenKey, authToken);
		await setPersistedAuthValue(sessionTokenExpiresKey, String(expiresAt));
		logger.info("Stored authenticated session token");
		// Rebuild so subsequent auth calls (like apiKey.create) carry the bearer token.
		// setAuthReady is called by applyNewApiKey which always follows on sign-in.
		// On standalone token refresh (no new api key), we also rebuild here.
		await rebuildClients();
	}

	async function applyNewApiKey(
		apiKey: string,
		apiKeyId: string,
		expiresAt = Date.now() + API_KEY_TTL_MS,
	) {
		currentApiKey = apiKey;

		await setPersistedAuthValue(storageKey, apiKey);
		await setPersistedAuthValue(storageIdKey, apiKeyId);
		await setPersistedAuthValue(storageExpiresKey, String(expiresAt));
		logger.info("Stored short-lived query API key", {
			expiresAt,
		});
		await rebuildClients();
		syncManager?.setAuthReady(true);
	}

	// Signing out or losing auth must clear both persisted state and the live
	// in-memory transport configuration, otherwise background requests can keep
	// sending stale credentials after the UI believes the user is logged out.
	async function clearStoredApiKey() {
		currentApiKey = null;
		shouldRefreshApiKey = Boolean(currentSessionToken);
		await removePersistedAuthValue(storageKey);
		await removePersistedAuthValue(storageIdKey);
		await removePersistedAuthValue(storageExpiresKey);
		logger.info("Cleared persisted API key state");
		await rebuildClients();
		syncManager?.setAuthReady(Boolean(currentSessionToken));
	}

	async function clearStoredAuth() {
		currentSessionToken = null;
		currentApiKey = null;
		shouldRefreshApiKey = false;
		syncManager?.setAuthReady(false);
		await removePersistedAuthValue(sessionDataKey);
		await removePersistedAuthValue(sessionTokenKey);
		await removePersistedAuthValue(sessionTokenExpiresKey);
		await removePersistedAuthValue(storageKey);
		await removePersistedAuthValue(storageIdKey);
		await removePersistedAuthValue(storageExpiresKey);
		logger.info("Cleared persisted auth state");
		await rebuildClients();
	}

	function scheduleRefresh() {
		if (refreshTimer) clearTimeout(refreshTimer);
		if (isDestroyed) return;
		refreshTimer = setTimeout(
			refreshApiKey,
			API_KEY_TTL_MS - REFRESH_BUFFER_MS,
		);
		(refreshTimer as { unref?: () => void }).unref?.();
	}

	// API-key rotation is only relevant for browser-session auth. Static authToken
	// and static apiKey callers intentionally manage rotation outside the package.
	async function refreshApiKey() {
		if (isDestroyed || !currentApiKey) return;

		try {
			const activeAuthClient = requireAuthClient();
			const { data: session, error: sessionError } =
				await activeAuthClient.getSession({
					fetchOptions: {
						headers: { "x-api-key": currentApiKey },
					},
				});

			if (sessionError || !session) {
				logger.warn("Failed to get session for API key refresh", sessionError);
				await clearStoredApiKey();
				if (currentSessionToken) {
					await ensureSessionApiKey();
				}
				return;
			}

			const { data: newKeyData, error: createError } =
				await activeAuthClient.apiKey.create({
					name: "client-key",
					expiresIn: API_KEY_TTL_MS / 1000,
					prefix: "ffdb-client-",
					fetchOptions: {
						headers: { "x-api-key": currentApiKey },
					},
				});

			if (createError || !newKeyData?.key || !newKeyData?.id) {
				logger.warn("Failed to create replacement API key", createError);
				if (!isDestroyed) {
					refreshTimer = setTimeout(refreshApiKey, 60 * 1000);
					(refreshTimer as { unref?: () => void }).unref?.();
				}
				return;
			}

			const oldKey = (await storage.get(storageIdKey)) as string;
			await applyNewApiKey(newKeyData.key, newKeyData.id);
			scheduleRefresh();

			await activeAuthClient.apiKey
				.delete({
					keyId: oldKey,
					fetchOptions: { headers: { "x-api-key": newKeyData.key } },
				})
				.catch((err: unknown) =>
					logger.warn("Failed to revoke old API key", err),
				);
		} catch (err) {
			logger.error("Unexpected error during API key refresh", err);
			if (!isDestroyed) {
				refreshTimer = setTimeout(refreshApiKey, 60 * 1000);
				(refreshTimer as { unref?: () => void }).unref?.();
			}
		}
	}

	async function destroy() {
		if (isDestroyed) return;
		isDestroyed = true;

		lifecycle?.onDestroyed?.();

		if (refreshTimer) {
			clearTimeout(refreshTimer);
			refreshTimer = null;
		}
		stopHealthProbeLoop();

		await dbClient.destroy();

		if (syncManager) {
			syncManager.destroy();
		}

		if (offline?.adapter.close) {
			await offline.adapter.close();
		}
	}

	lifecycle?.onCreated?.(destroy);

	let cachedAccess: AccessInfo | null = null;

	// Access metadata is reused by both query enforcement and sync table filtering,
	// so the client caches it until the caller explicitly forces a refresh.
	async function getAccess(opts?: { force?: boolean }): Promise<AccessInfo> {
		if (cachedAccess && !opts?.force) return cachedAccess;
		cachedAccess = await fetchAccess($fetch);
		if (syncManager && !offline?.tables) {
			syncManager.setTableFilter(
				cachedAccess.tables
					.filter((table) => table.access.read)
					.map((table) => table.table),
			);
		}
		return cachedAccess;
	}

	// Session reads prefer live auth when possible, but fall back to the persisted
	// session snapshot so offline screens can keep rendering meaningful user state.
	async function getAuthenticatedSession() {
		if (!isOnline) {
			const cachedSession = await getStoredSessionData();
			if (cachedSession) {
				logger.info("Using cached auth session while offline");
				return cachedSession;
			}
		}

		try {
			const activeAuthClient = requireAuthClient();
			const result = await activeAuthClient.getSession();
			if (!result?.data) {
				if (!isOnline) {
					return await getStoredSessionData();
				}
				return null;
			}

			await setStoredSessionData(result.data);
			if (currentSessionToken) {
				await storage.set(
					sessionTokenExpiresKey,
					String(Date.now() + SESSION_TTL_MS),
				);
			}
			return result.data;
		} catch (error) {
			const cachedSession = await getStoredSessionData();
			if (cachedSession && (!isOnline || isOfflineLikeError(error))) {
				logger.info("Using cached auth session while offline");
				return cachedSession;
			}
			throw error;
		}
	}

	// Session auth is converted into a short-lived query API key because the sync
	// and query endpoints are called frequently; using an API key avoids repeatedly
	// round-tripping through the heavier Better Auth session path.
	async function ensureSessionApiKey() {
		if (hasStaticConfiguredAuth) {
			return Boolean(currentSessionToken || currentApiKey);
		}

		const activeAuthClient = requireAuthClient();
		const session = await getAuthenticatedSession();
		if (!session) return false;

		const existingKeyId = (await getPersistedAuthValue(storageIdKey)) as
			| string
			| null;
		const existingKeyExpiresAt = Number(
			(await getPersistedAuthValue(storageExpiresKey)) ?? 0,
		);
		const forceRefresh = shouldRefreshApiKey === true;
		if (
			!forceRefresh &&
			currentApiKey &&
			existingKeyId &&
			existingKeyExpiresAt > Date.now() + REFRESH_BUFFER_MS
		) {
			try {
				const apiKeyOnlyFetch = createManagedFetchWrapper({
					...resolvedConfig,
					authToken: "",
					apiKey: currentApiKey,
				});
				await fetchAccess(apiKeyOnlyFetch);
				shouldRefreshApiKey = false;
				return true;
			} catch (error) {
				if (isOfflineLikeError(error)) {
					return true;
				}
				logger.warn(
					"Stored API key is no longer valid; minting a replacement",
					error,
				);
				await clearStoredApiKey();
			}
		}

		const previousKeyId = existingKeyId;
		const fetchOptions = currentSessionToken
			? {
					headers: {
						Authorization: `Bearer ${currentSessionToken}`,
					},
				}
			: undefined;

		const { data: newKeyData, error: createError } =
			await activeAuthClient.apiKey.create({
				name: "client-key",
				expiresIn: API_KEY_TTL_MS / 1000,
				prefix: "ffdb-client-",
				fetchOptions,
			});

		if (createError || !newKeyData?.key || !newKeyData?.id) {
			logger.warn("Failed to create API key for session", createError);
			return false;
		}

		await applyNewApiKey(newKeyData.key, newKeyData.id);
		shouldRefreshApiKey = false;
		scheduleRefresh();
		if (previousKeyId && previousKeyId !== newKeyData.id) {
			await activeAuthClient.apiKey
				.delete({
					keyId: previousKeyId,
					fetchOptions: { headers: { "x-api-key": newKeyData.key } },
				})
				.catch((error: unknown) =>
					logger.warn("Failed to revoke stale API key", error),
				);
		}
		return true;
	}

	// Sync startup is lazy and auth-aware. The client only kicks it off once the
	// runtime is online, auth is available, and any access-derived table filters are
	// ready.
	async function triggerSync() {
		if (!syncManager || !isOnline || isDestroyed) return;
		if (!(offline?.syncOnConnect ?? true)) return;
		if (syncManager.status.isSyncing) return;
		if (!(await ensureSessionApiKey())) return;
		if (offline?.skipTables?.length && !cachedAccess && !offline.tables) {
			try {
				await getAccess();
			} catch (error) {
				logger.warn("Failed to load access info before sync", error);
			}
		}

		const willPush = syncManager.status.pendingMutations > 0;
		logger.debug("Triggering sync", {
			mode: willPush ? "push" : "pull",
			pendingMutations: syncManager.status.pendingMutations,
		});

		const syncJob = willPush ? syncManager.push() : syncManager.pull();

		void syncJob.catch((err) => {
			logger.warn("Offline sync failed", err);
		});
	}

	// Better Auth remains the source of truth for sign-in / sign-out flows, but the
	// package hooks its success path so offline sync and query credentials stay in
	// step with whatever auth event just completed.
	authClient = createFFDBAuthClient({
		baseURL: resolvedConfig.apiUrl,
		fetchOptions: {
			credentials: "include",
			headers: {
				origin: resolvedConfig.origin,
			},
			auth: {
				type: "Bearer",
				token: () => currentSessionToken ?? "",
			},
			onSuccess: async (ctx) => {
				const pathname = new URL(ctx.request.url).pathname;
				const authToken =
					ctx.response.headers.get("set-auth-token") ??
					(typeof ctx.data?.sessionToken === "string"
						? ctx.data.sessionToken
						: null);

				if (authToken) {
					await storeSessionToken(authToken);
				}
				if (ctx.data?.user && ctx.data?.session) {
					await setStoredSessionData({
						user: ctx.data.user,
						session: ctx.data.session,
					});
				}

				if (pathname.includes("sign-in") && ctx.response.ok) {
					authBootstrapPromise = (async () => {
						const newToken = ctx.data?.apiKey;
						const newTokenId = ctx.data?.keyId ?? ctx.data?.id;

						if (
							typeof newToken === "string" &&
							newToken.length > 0 &&
							typeof newTokenId === "string" &&
							newTokenId.length > 0
						) {
							await applyNewApiKey(newToken, newTokenId);
							scheduleRefresh();
						} else {
							await ensureSessionApiKey();
						}

						await getAccess({ force: true });
						await triggerSync();
					})();

					try {
						await authBootstrapPromise;
					} finally {
						authBootstrapPromise = null;
					}
				}

				if (pathname.includes("sign-out")) {
					authBootstrapPromise = null;
					await clearStoredAuth();
				}
			},
		},
	}) as unknown as AuthClientInstance;

	// On startup, persisted auth is validated when possible. If the backend is not
	// reachable, the client keeps the cached auth state for offline use instead of
	// eagerly logging the user out.
	async function restoreAuthState() {
		const hasStoredAuth = Boolean(currentSessionToken || currentApiKey);
		if (!hasStoredAuth) return;

		if (hasStaticConfiguredAuth) {
			syncManager?.setAuthReady(true);
			if (isOnline && (offline?.syncOnConnect ?? true)) {
				await triggerSync();
			}
			return;
		}

		if (offline && !isOnline) {
			logger.info("Restored stored auth for offline operation");
			syncManager?.setAuthReady(Boolean(currentSessionToken || currentApiKey));
			return;
		}

		try {
			const session = await getAuthenticatedSession();
			if (!session) throw new Error("Stored session expired");

			await setStoredSessionData(session);
			await ensureSessionApiKey();
			if (currentApiKey) {
				scheduleRefresh();
			}

			await triggerSync();
		} catch (error) {
			const cachedSession = await getStoredSessionData();
			if (cachedSession && (!isOnline || isOfflineLikeError(error))) {
				logger.info("Keeping stored auth while backend is unreachable");
				syncManager?.setAuthReady(
					Boolean(currentSessionToken || currentApiKey),
				);
				return;
			}
			logger.warn("Failed to validate stored auth online", error);
			await clearStoredAuth();
		}
	}

	await restoreAuthState();

	if (!(currentSessionToken || currentApiKey) && syncManager && isOnline) {
		try {
			const activeAuthClient = requireAuthClient();
			const result = await activeAuthClient.getSession();
			if (result.data) {
				await ensureSessionApiKey();
				if (offline?.syncOnConnect ?? true) {
					await triggerSync();
				}
			}
		} catch {
			/* ignore missing/expired browser sessions */
		}
	}

	const baseAuthClient = requireAuthClient();
	const seamlessAuthClient = new Proxy(baseAuthClient as object, {
		get(target, prop, receiver) {
			if (prop === "waitForAuthReady") {
				return async () => {
					if (authBootstrapPromise) {
						await authBootstrapPromise;
						return;
					}

					if (!currentSessionToken && !currentApiKey) {
						return;
					}

					if (!currentApiKey && currentSessionToken) {
						await ensureSessionApiKey();
					}

					if (currentSessionToken || currentApiKey) {
						await getAccess({ force: true });
					}
				};
			}
			if (prop === "getSession") {
				// Public callers should see the same cached-session fallback behavior as
				// the internal auth helpers, not the raw Better Auth network call.
				return async (..._args: any[]) => {
					try {
						const data = await getAuthenticatedSession();
						return { data, error: null };
					} catch (error) {
						return {
							data: null,
							error: error instanceof Error ? error : new Error(String(error)),
						};
					}
				};
			}
			if (prop === "signOut") {
				// Clearing local auth in `finally` keeps the local/offline state honest even
				// if the remote sign-out request fails or the browser closes mid-request.
				return async (...args: any[]) => {
					try {
						return await baseAuthClient.signOut(...args);
					} finally {
						await clearStoredAuth();
					}
				};
			}
			return Reflect.get(target, prop, receiver);
		},
	}) as typeof baseAuthClient;

	// The raw SQL helper adds one FFDB-specific capability on top of Kysely's `sql`
	// tag: callers can ask the backend and the offline driver to bypass cached query
	// answers for one request.
	const sqlHelper = Object.assign(
		((...args: Parameters<typeof sql>) => sql(...args)) as typeof sql,
		{
			execute: async <
				TRow extends Record<string, unknown> = Record<string, unknown>,
				TValues extends readonly unknown[] = readonly unknown[],
			>(
				input: SqlQueryInput<TValues>,
			) => {
				const rawQuery = CompiledQuery.raw(
					input.sql,
					Array.from(input.values ?? []),
				);
				const compiledQuery = {
					...rawQuery,
					ffdb: {
						bypassCache: input.bypassCache === true,
					},
				} as CompiledQuery & {
					ffdb?: { bypassCache?: boolean };
				};
				const result = await dbClient.executeQuery<TRow>(compiledQuery);
				const meta =
					"meta" in result && result.meta && typeof result.meta === "object"
						? (result.meta as QueryMeta)
						: {};
				return {
					rows: result.rows ?? [],
					meta,
				};
			},
			query: async <
				TRow extends Record<string, unknown> = Record<string, unknown>,
				TValues extends readonly unknown[] = readonly unknown[],
			>(
				input: SqlQueryInput<TValues>,
			) => {
				const result = await sqlHelper.execute<TRow, TValues>(input);
				return result.rows;
			},
			first: async <
				TRow extends Record<string, unknown> = Record<string, unknown>,
				TValues extends readonly unknown[] = readonly unknown[],
			>(
				input: SqlQueryInput<TValues>,
			) => {
				const rows = await sqlHelper.query<TRow, TValues>(input);
				return rows[0];
			},
		},
	) as SqlHelper;

	const request = async <T = unknown>(
		url: string,
		options: ClientRequestOptions = {},
	): Promise<T> => {
		const {
			retries = 3,
			retryDelay = 500,
			retryOn = [429, 500, 502, 503, 504],
			parse = "json",
			useCache = true,
			headers,
			method: rawMethod,
			body,
			...rest
		} = options;
		const method = String(rawMethod ?? "GET").toUpperCase();
		const canUseCache = Boolean(
			offline?.adapter && useCache && parse === "json" && method === "GET",
		);
		const cacheKey = `${method}:${url}`;
		const cached = canUseCache ? await readCachedHttpValue<T>(cacheKey) : null;

		// When offline, policy-query POSTs are rerouted through the local SQL helper so
		// existing consumers can keep using the same request interface instead of
		// branching on connectivity themselves.
		if (
			offline?.adapter &&
			!isOnline &&
			method === "POST" &&
			(url === resolvedConfig.endpoint || url.endsWith("/api/policies/query"))
		) {
			const parsedBody =
				typeof body === "string"
					? (JSON.parse(body) as {
							sql?: string;
							values?: unknown[];
							bypassCache?: boolean;
						})
					: ((body ?? {}) as {
							sql?: string;
							values?: unknown[];
							bypassCache?: boolean;
						});
			if (parsedBody.sql) {
				const result = await sqlHelper.execute({
					sql: parsedBody.sql,
					values: Array.isArray(parsedBody.values) ? parsedBody.values : [],
					bypassCache: parsedBody.bypassCache === true,
				});
				return {
					status: "ok",
					data: {
						rows: result.rows,
						meta: {
							...result.meta,
							cached: true,
						},
					},
				} as T;
			}
		}

		if (cached !== null && !isOnline) {
			return cached;
		}

		let attempt = 0;
		let lastError: unknown = null;

		let hasRetriedUnauthorized = false;

		while (attempt <= retries) {
			try {
				const requestHeaders = new Headers(headers ?? undefined);
				requestHeaders.set(
					"X-Timezone",
					Intl.DateTimeFormat().resolvedOptions().timeZone,
				);

				let requestBody = body as
					| BodyInit
					| Record<string, unknown>
					| unknown[]
					| null
					| undefined;
				if (
					requestBody &&
					typeof requestBody === "object" &&
					!(requestBody instanceof FormData) &&
					!(requestBody instanceof Blob) &&
					!(requestBody instanceof URLSearchParams) &&
					!(requestBody instanceof ArrayBuffer)
				) {
					if (!requestHeaders.has("Content-Type")) {
						requestHeaders.set("Content-Type", "application/json");
					}
					requestBody = JSON.stringify(requestBody);
				} else if (
					typeof requestBody === "string" &&
					!requestHeaders.has("Content-Type")
				) {
					requestHeaders.set("Content-Type", "application/json");
				}

				const response = await fetch(
					url.startsWith("http://") || url.startsWith("https://")
						? url
						: `${resolvedConfig.apiUrl}${url}`,
					{
						...rest,
						method,
						body: requestBody as BodyInit | null | undefined,
						headers: requestHeaders,
						credentials: "include",
					},
				);

				if (
					(response.status === 401 || response.status === 403) &&
					!hasRetriedUnauthorized
				) {
					const recovered = await recoverFromUnauthorized();
					if (recovered) {
						hasRetriedUnauthorized = true;
						continue;
					}
				}

				if (retryOn.includes(response.status) && attempt < retries) {
					const delay = retryDelay * 2 ** attempt;
					await new Promise((resolve) => setTimeout(resolve, delay));
					attempt += 1;
					continue;
				}

				if (response.status === 401 || response.status === 403) {
					// An unrecoverable auth failure means the local session snapshot is no
					// longer trustworthy either, so clear it before surfacing the error.
					await seamlessAuthClient.signOut().catch(async () => {
						await clearStoredAuth();
					});
					throw await buildResponseError(response);
				}

				if (!response.ok) {
					throw await buildResponseError(response);
				}

				let data: T;
				switch (parse) {
					case "text":
						data = (await response.text()) as T;
						break;
					case "blob":
						data = (await response.blob()) as T;
						break;
					default:
						data = (await response.json()) as T;
						break;
				}

				await markOnline();
				if (canUseCache) {
					await writeCachedHttpValue(cacheKey, data);
				}
				return data;
			} catch (error) {
				lastError = error;
				if (isOfflineLikeError(error)) {
					await markOffline(error);
					if (cached !== null) {
						return cached;
					}
				}
				if (attempt < retries) {
					const delay = retryDelay * 2 ** attempt;
					await new Promise((resolve) => setTimeout(resolve, delay));
					attempt += 1;
					continue;
				}
				throw lastError;
			}
		}

		throw (lastError ?? new Error("Request failed")) as Error;
	};

	const sync: SyncHandle | null = syncManager
		? {
				run: () => syncManager.sync(),
				sync: () => syncManager.sync(),
				pull: () => syncManager.pull(),
				push: () => syncManager.push(),
				waitForIdle: (timeoutMs?: number) => syncManager.waitForIdle(timeoutMs),
				get status(): Readonly<SyncStatus> {
					return syncManager.status;
				},
				subscribe: (listener: () => void) => syncManager.subscribe(listener),
			}
		: null;

	function subscribe(listener: () => void): () => void {
		// Data subscriptions are only meaningful in offline mode where background sync
		// and read-through refresh can change local query results under the UI.
		if (!syncManager) return () => {};
		return syncManager.subscribeData(listener);
	}

	return {
		db,
		sql: sqlHelper,
		auth: seamlessAuthClient,
		request,
		destroy,
		sync,
		getAccess,
		subscribe,
	};
}
