import { apiKeyClient } from "@better-auth/api-key/client";
import { passkeyClient } from "@better-auth/passkey/client";
import {
	adminClient,
	emailOTPClient,
	genericOAuthClient,
	magicLinkClient,
	organizationClient,
	twoFactorClient,
	usernameClient,
} from "better-auth/client/plugins";
import { createAuthClient as createReactAuthClient } from "better-auth/react";
import type { Kysely } from "kysely";
import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useEffect,
	useRef,
	useState,
} from "react";
import {
	type AuthClientInstance,
	type FFDBClient as CoreFFDBClient,
	type CreateClientOptions,
	createClient,
	createFFDBAuthPlugins,
} from "../client.ts";
import { defaultConfig } from "../config.ts";
import { memoryStorage } from "../storage.ts";
import type { Database as BasicDatabase } from "../types.ts";

type ClientResult = CoreFFDBClient<unknown>;

// This helper exists only to capture the inferred Better Auth React client type
// with the full FFDB plugin set attached. The hard-coded URLs are never used at
// runtime; they just give TypeScript a concrete call site to infer from.
function inferFFDBReactAuthClient() {
	return createReactAuthClient({
		baseURL: "http://localhost:3000",
		fetchOptions: {
			credentials: "include",
			headers: {
				origin: "http://localhost:3000",
			},
		},
		plugins: [
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
		] as const,
	});
}

// Build the React-facing auth client with the same plugin surface as the core
// FFDB auth client so hooks and imperative methods stay aligned.
function createFFDBReactAuthClient(
	baseURL: string,
	origin: string,
): ReturnType<typeof inferFFDBReactAuthClient> {
	return createReactAuthClient({
		baseURL,
		fetchOptions: {
			credentials: "include",
			headers: {
				origin,
			},
		},
		plugins: createFFDBAuthPlugins(),
	}) as ReturnType<typeof inferFFDBReactAuthClient>;
}

type ReactAuthClientInstance = ReturnType<typeof inferFFDBReactAuthClient>;

type SessionHookResult = {
	data: AuthClientInstance["$Infer"]["Session"] | null;
	error: unknown | null;
	isPending: boolean;
	isRefetching: boolean;
	refetch: () => Promise<void>;
};

export type FFDBReactAuthClient = Omit<
	ReactAuthClientInstance,
	"useSession" | "$Infer"
> &
	AuthClientInstance & {
		useSession: (...args: any[]) => SessionHookResult;
	};

type FFDBClient<DB> = Omit<ClientResult, "db" | "auth"> & {
	db: Kysely<DB>;
	auth: FFDBReactAuthClient;
};

type FFDBContextValue<DB> = {
	client: FFDBClient<DB> | null;
	auth: FFDBReactAuthClient | null;
	isLoading: boolean;
	error: Error | null;
	clientVersion: number;
};

type UseFFDBResult<DB> = {
	client: FFDBClient<DB> | null;
	db: Kysely<DB> | null;
	sql: ClientResult["sql"] | null;
	auth: FFDBReactAuthClient | null;
	destroy: ClientResult["destroy"] | null;
	sync: ClientResult["sync"] | null;
	getAccess: ClientResult["getAccess"] | null;
	subscribe: ClientResult["subscribe"] | null;
	isLoading: boolean;
	error: Error | null;
	clientVersion: number;
	isReady: boolean;
};

let sharedAuthClient: FFDBReactAuthClient | null = null;
let sharedClient: FFDBClient<any> | null = null;

// The shared context is the React entry point, while the shared module-level
// getters below support non-hook consumers such as legacy utilities and fetch
// wrappers that need access outside a React render tree.
export const FFDBContext = createContext<FFDBContextValue<any> | null>(null);

export type FFDBProviderProps = {
	children: ReactNode;
	options: CreateClientOptions;
	fallback?: ReactNode;
	onError?: (error: Error) => void;
};

function createSeamlessReactAuth(
	options: CreateClientOptions,
	baseAuth: AuthClientInstance,
): FFDBReactAuthClient {
	const resolvedConfig = { ...defaultConfig, ...(options.config ?? {}) };
	const reactAuth = createFFDBReactAuthClient(
		resolvedConfig.apiUrl,
		resolvedConfig.origin,
	);
	type SessionData = SessionHookResult["data"];
	// The raw Better Auth React client does not expose the same imperative
	// passkey namespace shape as the proxied FFDB client. The proxy below
	// resolves that mismatch by sourcing imperative namespaces from baseAuth.
	const mergedAuth = reactAuth as unknown as FFDBReactAuthClient;
	const storage = options.storage ?? memoryStorage();
	const sessionCacheKey = `ffdb:session-data:${resolvedConfig.apiUrl}`;
	type SharedSessionState = {
		data: SessionData | null;
		error: SessionHookResult["error"];
		isPending: boolean;
	};
	let sharedSessionState: SharedSessionState = {
		data: null,
		error: null,
		isPending: true,
	};
	const sessionListeners = new Set<() => void>();
	let hasHydratedCachedSession = false;
	let hasLoadedLiveSession = false;
	let sessionRequest: Promise<void> | null = null;

	const emitSessionState = () => {
		for (const listener of sessionListeners) {
			listener();
		}
	};

	const updateSharedSessionState = (nextState: Partial<SharedSessionState>) => {
		sharedSessionState = {
			...sharedSessionState,
			...nextState,
		};
		emitSessionState();
	};

	// React auth consumers should keep working during transient offline periods,
	// so session reads are wrapped with a small persisted cache keyed per API URL.
	const readCachedSession = async () => {
		const cached = await storage.get(sessionCacheKey);
		if (!cached) return null;
		try {
			return JSON.parse(cached);
		} catch {
			return null;
		}
	};

	const writeCachedSession = async (session: unknown) => {
		await storage.set(sessionCacheKey, JSON.stringify(session));
	};

	const hydrateCachedSession = async () => {
		if (hasHydratedCachedSession) return sharedSessionState.data;
		hasHydratedCachedSession = true;

		const cached = await readCachedSession();
		if (cached) {
			updateSharedSessionState({
				data: cached,
				error: null,
			});
		}

		return cached;
	};

	// Better Auth and fetch failures vary by runtime, so session fallback uses a
	// message-based offline heuristic instead of relying on one specific error
	// type.
	const isOfflineLikeError = (error: unknown) => {
		const message =
			error instanceof Error ? error.message : String(error ?? "");
		return /failed to fetch|network|err_connection_refused|load failed/i.test(
			message,
		);
	};

	// Bridge the core auth client's session lookup into a React-friendly version
	// that persists successful sessions and can recover cached state when the
	// network drops.
	const getSession = async (...args: any[]) => {
		try {
			const result = await baseAuth.getSession(...args);
			if (result?.data) {
				await writeCachedSession(result.data);
				hasHydratedCachedSession = true;
				updateSharedSessionState({
					data: result.data,
					error: null,
					isPending: false,
				});
				return result;
			}

			if (result?.error) {
				const cached = await readCachedSession();
				if (cached && isOfflineLikeError(result.error)) {
					updateSharedSessionState({
						data: cached,
						error: null,
						isPending: false,
					});
					return { data: cached, error: null };
				}

				updateSharedSessionState({
					data: null,
					error: result.error,
					isPending: false,
				});
			}

			return result;
		} catch (error) {
			const cached = await readCachedSession();
			if (cached && isOfflineLikeError(error)) {
				updateSharedSessionState({
					data: cached,
					error: null,
					isPending: false,
				});
				return { data: cached, error: null };
			}
			updateSharedSessionState({
				data: null,
				error: error instanceof Error ? error : new Error(String(error)),
				isPending: false,
			});
			throw error;
		}
	};

	// Clearing the cached session before delegating sign-out keeps the React layer
	// from briefly replaying stale user state after the caller has explicitly
	// requested logout.
	const signOut = async (...args: any[]) => {
		await storage.remove(sessionCacheKey);
		hasHydratedCachedSession = true;
		hasLoadedLiveSession = true;
		updateSharedSessionState({
			data: null,
			error: null,
			isPending: false,
		});
		return await baseAuth.signOut(...args);
	};

	const loadSharedSession = async (
		args: any[],
		options?: { force?: boolean },
	) => {
		if (sessionRequest && !options?.force) {
			return await sessionRequest;
		}

		if (hasLoadedLiveSession && !options?.force) {
			return;
		}

		updateSharedSessionState({
			isPending: true,
			error: sharedSessionState.data ? null : sharedSessionState.error,
		});

		let currentRequest: Promise<void> | null = null;
		currentRequest = (async () => {
			const cached = await hydrateCachedSession();

			try {
				const result = await getSession(...args);
				hasLoadedLiveSession = true;
				updateSharedSessionState({
					data: result?.data ?? cached ?? null,
					error: (result?.error ?? null) as SessionHookResult["error"],
					isPending: false,
				});
			} catch (error) {
				const fallback = cached ?? (await readCachedSession());
				if (fallback) {
					updateSharedSessionState({
						data: fallback,
						error: null,
						isPending: false,
					});
				} else {
					updateSharedSessionState({
						error: (error instanceof Error
							? error
							: new Error(String(error))) as SessionHookResult["error"],
						isPending: false,
					});
				}
			} finally {
				if (currentRequest && sessionRequest === currentRequest) {
					sessionRequest = null;
				}
			}
		})();

		sessionRequest = currentRequest;
		return await currentRequest;
	};

	// Expose a light `useSession` hook even though the core client is framework-
	// agnostic. This lets React consumers share the same auth source of truth as
	// the imperative client without needing a second auth provider.
	const useSession = (...args: any[]): SessionHookResult => {
		const [snapshot, setSnapshot] = useState(sharedSessionState);
		const argsRef = useRef(args);
		argsRef.current = args;

		const refetch = useCallback(async () => {
			hasLoadedLiveSession = false;
			await loadSharedSession(argsRef.current, { force: true });
		}, []);

		useEffect(() => {
			const syncSnapshot = () => {
				setSnapshot(sharedSessionState);
			};

			sessionListeners.add(syncSnapshot);
			syncSnapshot();
			void loadSharedSession(argsRef.current);

			return () => {
				sessionListeners.delete(syncSnapshot);
			};
		}, []);

		return {
			data: snapshot.data,
			error: snapshot.data ? null : snapshot.error,
			isPending: snapshot.isPending,
			isRefetching: Boolean(snapshot.data) && snapshot.isPending,
			refetch,
		};
	};

	return new Proxy(mergedAuth as object, {
		// Prefer React-specific helpers when they exist, but fall back to the core
		// auth client for the rest of the imperative Better Auth surface.
		get(target, prop, receiver) {
			if (prop === "getSession") return getSession;
			if (prop === "signOut") return signOut;
			if (prop === "useSession") return useSession;

			const baseValue = Reflect.get(baseAuth as object, prop, baseAuth);
			const reactValue = Reflect.get(target, prop, receiver);

			// The React client adds hooks, but the core client remains the source of
			// truth for imperative namespaces like passkey/apiKey/session actions.
			// Returning the base namespace first avoids leaking stale React wrapper
			// method maps when Better Auth plugin routes change.
			if (baseValue !== undefined) {
				return baseValue;
			}

			if (reactValue !== undefined) {
				return reactValue;
			}

			return undefined;
		},
	}) as FFDBReactAuthClient;
}

export function FFDBProvider<ExtraDatabase>({
	children,
	options,
	fallback = null,
	onError,
}: FFDBProviderProps) {
	type DB = BasicDatabase & ExtraDatabase;

	const [client, setClient] = useState<FFDBClient<DB> | null>(null);
	const [isLoading, setIsLoading] = useState(true);
	const [error, setError] = useState<Error | null>(null);
	const [clientVersion, setClientVersion] = useState(0);
	const destroyRef = useRef<(() => Promise<void>) | null>(null);
	const authRef = useRef<FFDBReactAuthClient | null>(null);

	useEffect(() => {
		let cancelled = false;
		// Clear exposed state before booting a replacement client so callers do not
		// keep using a stale instance while a new option set is initializing.
		setClient(null);
		setIsLoading(true);
		setError(null);
		authRef.current = null;
		sharedAuthClient = null;
		sharedClient = null;

		async function init() {
			try {
				const result = await createClient<ExtraDatabase>(options);

				if (cancelled) {
					await result.destroy();
					return;
				}

				const seamlessClient = {
					...result,
					auth: createSeamlessReactAuth(options, result.auth),
				};

				authRef.current = seamlessClient.auth;
				sharedAuthClient = seamlessClient.auth;
				sharedClient = seamlessClient as unknown as FFDBClient<any>;
				destroyRef.current = seamlessClient.destroy;
				setClient(seamlessClient as unknown as FFDBClient<DB>);
				setClientVersion((value) => value + 1);
				setError(null);
			} catch (err) {
				if (!cancelled) {
					const e = err instanceof Error ? err : new Error(String(err));
					authRef.current = null;
					sharedAuthClient = null;
					sharedClient = null;
					setError(e);
					onError?.(e);
				}
			} finally {
				if (!cancelled) setIsLoading(false);
			}
		}

		init();

		return () => {
			cancelled = true;
			authRef.current = null;
			sharedAuthClient = null;
			sharedClient = null;
			destroyRef.current?.().catch(() => {
				/* ignore teardown errors */
			});
			destroyRef.current = null;
		};
	}, [options, onError]);

	return (
		<FFDBContext.Provider
			value={{ client, auth: authRef.current, isLoading, error, clientVersion }}
		>
			{error ? (
				<pre style={{ color: "red", whiteSpace: "pre-wrap" }}>
					{error.stack ?? error.message}
				</pre>
			) : isLoading || !client ? (
				fallback
			) : (
				children
			)}
		</FFDBContext.Provider>
	);
}

function useFFDBContext<DB>(): FFDBContextValue<DB> {
	const ctx = useContext(FFDBContext);
	if (!ctx) {
		// All React convenience hooks depend on the provider owning one live FFDB
		// instance. Throwing early keeps misuse obvious instead of failing later with
		// null access errors.
		throw new Error("useDB/useAuth must be used within an <FFDBProvider>");
	}
	return ctx as FFDBContextValue<DB>;
}

export function useFFDB<ExtraDatabase>(): UseFFDBResult<
	BasicDatabase & ExtraDatabase
> {
	type DB = BasicDatabase & ExtraDatabase;
	const { client, auth, isLoading, error, clientVersion } =
		useFFDBContext<DB>();

	// This is the ergonomic "give me everything" hook for app-level consumers that
	// want one object describing both readiness and the active client surface.
	return {
		...(client ?? {
			db: null,
			sql: null,
			auth,
			destroy: null,
			sync: null,
			getAccess: null,
			subscribe: null,
		}),
		auth,
		client,
		isLoading,
		error,
		clientVersion,
		isReady: Boolean(client && !isLoading && !error),
	};
}

export function useFFDBClient<ExtraDatabase>() {
	type DB = BasicDatabase & ExtraDatabase;
	const { client, error } = useFFDBContext<DB>();

	if (!client) {
		if (error) throw error;
		return null as unknown as FFDBClient<DB>;
	}

	return client;
}

export function useDB<ExtraDatabase>() {
	type DB = BasicDatabase & ExtraDatabase;
	const { client, error } = useFFDBContext<DB>();

	if (!client?.db) {
		if (error) throw error;
		return null as unknown as Kysely<DB>;
	}

	return client.db;
}

export function useAuth() {
	const { auth, error } = useFFDBContext<any>();

	if (!auth) {
		if (error) throw error;
		return null as unknown as FFDBReactAuthClient;
	}

	return auth;
}

export function useFFDBStatus() {
	const { isLoading, error } = useFFDBContext<any>();
	return { isLoading, error };
}

export function getFFDBAuth() {
	return sharedAuthClient;
}

export function getFFDBClient() {
	return sharedClient;
}
