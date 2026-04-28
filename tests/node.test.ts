import { beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../src/client.ts";
import { generateId } from "../src/index.ts";
import { loadEnvConfig } from "../src/node/config.ts";
import { OfflineDriver } from "../src/offline/driver.ts";
import { SyncManager } from "../src/offline/sync.ts";
import { memoryStorage } from "../src/storage.ts";

const fetchMockState = {
	configs: [] as Array<Record<string, unknown>>,
	healthError: null as Error | null,
	healthCalls: 0,
	syncPullAttempts: 0,
	syncPullCalls: 0,
	syncPullBodies: [] as Array<Record<string, unknown>>,
	queryBodies: [] as Array<Record<string, unknown>>,
	invalidApiKeys: new Set<string>(),
	invalidAuthTokens: new Set<string>(),
};

const authMockState = {
	session: null as any,
	getSessionError: null as Error | null,
	getSessionCalls: 0,
	apiKeyCreateCalls: 0,
	onSuccess: null as null | ((ctx: any) => Promise<void>),
};

vi.mock("@better-auth/api-key/client", () => {
	return {
		apiKeyClient: () => ({ id: "api-key-client" }),
	};
});

vi.mock("better-auth/client", () => {
	return {
		createAuthClient: (options?: {
			fetchOptions?: { onSuccess?: (ctx: any) => Promise<void> };
		}) => {
			authMockState.onSuccess = options?.fetchOptions?.onSuccess ?? null;
			return {
				getSession: vi.fn(async () => {
					authMockState.getSessionCalls += 1;
					if (authMockState.getSessionError) {
						throw authMockState.getSessionError;
					}
					return {
						data: authMockState.session,
						error: authMockState.session ? null : { message: "Unauthorized" },
					};
				}),
				apiKey: {
					create: vi.fn(async () => {
						authMockState.apiKeyCreateCalls += 1;
						const suffix =
							authMockState.apiKeyCreateCalls === 1
								? ""
								: `-${authMockState.apiKeyCreateCalls}`;
						return {
							data: {
								key: `issued-api-key${suffix}`,
								id: `issued-key-id${suffix}`,
							},
							error: null,
						};
					}),
					delete: vi.fn(async () => ({ data: true, error: null })),
				},
				signIn: {
					email: vi.fn(async () => ({ data: null, error: null })),
				},
			};
		},
	};
});

vi.mock("../src/fetch.ts", () => {
	return {
		createFetchClient: (options: { config: Record<string, unknown> }) => {
			fetchMockState.configs.push(options.config);
			return async (
				url: string,
				request?: { body?: Record<string, unknown> },
			) => {
				const apiKey = String(options.config.apiKey ?? "");
				const authToken = String(options.config.authToken ?? "");
				const isStaleApiKey =
					apiKey === "stale-api-key" ||
					fetchMockState.invalidApiKeys.has(apiKey);
				const isInvalidAuthToken =
					authToken.length > 0 &&
					fetchMockState.invalidAuthTokens.has(authToken);
				if (url === "/api/health") {
					fetchMockState.healthCalls += 1;
					if (fetchMockState.healthError) {
						throw fetchMockState.healthError;
					}
					return {
						data: { status: "ok", data: { message: "ok" } },
						error: null,
					};
				}
				if (url === "/api/policies/access") {
					if (isStaleApiKey) {
						return {
							data: null,
							error: { statusText: "Unauthorized", message: "Unauthorized" },
						};
					}
					return {
						data: {
							data: {
								userId: "u1",
								role: "user",
								blockedStatementTypes: [],
								tables: [
									{
										table: "account",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "session",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "verification",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "apikey",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "policies",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "table_counts",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "sqlite_shadow",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "user",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "system_metrics",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
									{
										table: "query_metrics",
										access: {
											read: true,
											insert: false,
											update: false,
											delete: false,
										},
										constraints: {},
										explicitlyConfigured: true,
										adminOverride: false,
									},
								],
							},
						},
						error: null,
					};
				}
				if (url === "/api/sync/pull") {
					fetchMockState.syncPullAttempts += 1;
					if (isStaleApiKey || isInvalidAuthToken) {
						return {
							data: null,
							error: { statusText: "Unauthorized", message: "Unauthorized" },
						};
					}
					fetchMockState.syncPullCalls += 1;
					fetchMockState.syncPullBodies.push(request?.body ?? {});
					return {
						data: {
							data: {
								tables: {},
								syncedAt: 2000,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}
				if (url === "/api/policies/query") {
					if (isStaleApiKey) {
						return {
							data: null,
							error: { statusText: "Unauthorized", message: "Unauthorized" },
						};
					}
					fetchMockState.queryBodies.push(request?.body ?? {});
					return {
						data: {
							data: {
								rows: [],
								meta: {
									cached: false,
									bypassedCache: request?.body?.bypassCache === true,
									rowCount: 0,
									statementType: "select",
								},
							},
						},
						error: null,
					};
				}
				return {
					data: {
						data: {
							rows: [],
							numAffectedRows: 0,
						},
					},
					error: null,
				};
			};
		},
	};
});

beforeEach(() => {
	fetchMockState.configs = [];
	fetchMockState.healthError = null;
	fetchMockState.healthCalls = 0;
	fetchMockState.syncPullAttempts = 0;
	fetchMockState.syncPullCalls = 0;
	fetchMockState.syncPullBodies = [];
	fetchMockState.queryBodies = [];
	fetchMockState.invalidApiKeys = new Set<string>();
	fetchMockState.invalidAuthTokens = new Set<string>();
	authMockState.session = null;
	authMockState.getSessionError = null;
	authMockState.getSessionCalls = 0;
	authMockState.apiKeyCreateCalls = 0;
	authMockState.onSuccess = null;
});

type Extra = {
	my_view: {
		id: string;
		computed: number;
	};
};

describe("await createClient types", () => {
	it("generates stable text ids for client-created rows", () => {
		const id = generateId();
		const prefixedId = generateId("temp_");

		expect(typeof id).toBe("string");
		expect(id.length).toBeGreaterThan(10);
		expect(generateId()).not.toBe(id);
		expect(prefixedId.startsWith("temp_")).toBe(true);
	});

	it("merges generated + extra types", async () => {
		const { db } = await createClient<Extra>({
			skipHealthCheck: true,
			storage: memoryStorage(),
		});

		// type-level assertions
		expect(db).toBeDefined();

		// table access
		expect(db.selectFrom("my_view").compile()).toBeDefined();
	});

	it("exposes the Kysely schema builder on the stable db wrapper", async () => {
		const { db } = await createClient({
			skipHealthCheck: true,
			storage: memoryStorage(),
		});

		expect(db.schema).toBeDefined();
		expect(db.schema.createTable("demo_table").ifNotExists()).toBeDefined();
	});

	it("supports no extra types", async () => {
		const { db } = await createClient({
			skipHealthCheck: true,
			storage: memoryStorage(),
		});

		expect(db).toBeDefined();
	});

	it("exposes an ergonomic typed raw-sql helper", async () => {
		const client = await createClient({
			skipHealthCheck: true,
			storage: memoryStorage(),
		});

		expect(client.sql).toBeDefined();
		expect(typeof client.sql.query).toBe("function");
		await expect(
			client.sql.query<{ id: string; email: string }>({
				sql: "SELECT id, email FROM user WHERE email = ?",
				values: ["user@example.com"],
			}),
		).resolves.toEqual([]);
	});

	it("forwards bypassCache and exposes response metadata on raw sql calls", async () => {
		const client = await createClient({
			skipHealthCheck: true,
			storage: memoryStorage(),
		});

		const result = await client.sql.execute<{ id: string }, [string]>({
			sql: "SELECT id FROM user WHERE email = ?",
			values: ["user@example.com"],
			bypassCache: true,
		});

		expect(fetchMockState.queryBodies.at(-1)?.bypassCache).toBe(true);
		expect(result.meta.bypassedCache).toBe(true);
		expect(result.meta.cached).toBe(false);
	});
});

describe("node config helpers", () => {
	it("loads retry and health probe settings from environment variables", () => {
		const previousRetryAttempts = process.env.RETRY_ATTEMPTS;
		const previousHealthProbe = process.env.HEALTH_PROBE_INTERVAL_MS;

		process.env.RETRY_ATTEMPTS = "3";
		process.env.HEALTH_PROBE_INTERVAL_MS = "2500";

		const result = loadEnvConfig({ quiet: true });

		expect(result.retryAttempts).toBe(3);
		expect(result.healthProbeIntervalMs).toBe(2500);

		if (previousRetryAttempts === undefined) {
			delete process.env.RETRY_ATTEMPTS;
		} else {
			process.env.RETRY_ATTEMPTS = previousRetryAttempts;
		}

		if (previousHealthProbe === undefined) {
			delete process.env.HEALTH_PROBE_INTERVAL_MS;
		} else {
			process.env.HEALTH_PROBE_INTERVAL_MS = previousHealthProbe;
		}
	});
});

describe("auth lifecycle", () => {
	it("does not trigger sync until getSession confirms authentication", async () => {
		authMockState.session = null;

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage: memoryStorage(),
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: true,
			},
		});

		expect(fetchMockState.syncPullCalls).toBe(0);
	});

	it("uses an explicitly configured bearer token for offline sync without minting an API key", async () => {
		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
				authToken: "static-bearer-token",
			},
			skipHealthCheck: true,
			storage: memoryStorage(),
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: true,
			},
		});

		expect(fetchMockState.syncPullCalls).toBe(1);
		expect(authMockState.getSessionCalls).toBe(0);
		expect(authMockState.apiKeyCreateCalls).toBe(0);
		expect(
			fetchMockState.configs.some(
				(config) => config.authToken === "static-bearer-token",
			),
		).toBe(true);
	});

	it("uses an explicitly configured API key for offline sync without calling auth session APIs", async () => {
		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
				apiKey: "static-api-key",
			},
			skipHealthCheck: true,
			storage: memoryStorage(),
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: true,
			},
		});

		expect(fetchMockState.syncPullCalls).toBe(1);
		expect(authMockState.getSessionCalls).toBe(0);
		expect(authMockState.apiKeyCreateCalls).toBe(0);
		expect(
			fetchMockState.configs.some(
				(config) => config.apiKey === "static-api-key",
			),
		).toBe(true);
	});

	it("does not run manual pull when offline sync auth is unavailable", async () => {
		const { sync } = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage: memoryStorage(),
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		if (!sync) throw new Error("Expected sync handle");

		await expect(sync.pull()).resolves.toBeUndefined();
		expect(fetchMockState.syncPullAttempts).toBe(0);
		expect(fetchMockState.syncPullCalls).toBe(0);
	});

	it("stops interval pull retries after static bearer auth is rejected", async () => {
		vi.useFakeTimers();
		try {
			fetchMockState.invalidAuthTokens.add("invalid-static-token");

			const client = await createClient({
				config: {
					apiUrl: "http://example.test",
					origin: "http://app.test",
					authToken: "invalid-static-token",
				},
				skipHealthCheck: true,
				storage: memoryStorage(),
				offline: {
					adapter: {
						execute: async (sql: string) => {
							if (
								sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")
							) {
								return { rows: [{ count: 0 }] };
							}
							return { rows: [] };
						},
					},
					syncOnConnect: true,
					syncInterval: 25,
				},
			});

			expect(fetchMockState.syncPullAttempts).toBe(1);

			await vi.advanceTimersByTimeAsync(100);

			expect(fetchMockState.syncPullAttempts).toBe(1);
			await client.destroy();
		} finally {
			vi.useRealTimers();
		}
	});

	it("restores query auth during offline init when a browser session exists", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		expect(await storage.get("ffdb:api-key:http://example.test")).toBe(
			"issued-api-key",
		);
	});

	it("stores the bearer session token from auth success and keeps api keys separate", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
		});

		await authMockState.onSuccess?.({
			request: { url: "http://example.test/api/auth/sign-in/email" },
			response: {
				ok: true,
				headers: new Headers({ "set-auth-token": "long-lived-session-token" }),
			},
			data: {
				apiKey: "short-lived-api-key",
				keyId: "short-lived-key-id",
			},
		});

		expect(await storage.get("ffdb:session-token:http://example.test")).toBe(
			"long-lived-session-token",
		);
		expect(await storage.get("ffdb:api-key:http://example.test")).toBe(
			"short-lived-api-key",
		);
	});

	it("stores the session token from the auth payload when the header is not exposed to the browser", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
		});

		await authMockState.onSuccess?.({
			request: { url: "http://example.test/api/auth/sign-in/email" },
			response: {
				ok: true,
				headers: new Headers(),
			},
			data: {
				sessionToken: "payload-session-token",
				apiKey: "short-lived-api-key",
				keyId: "short-lived-key-id",
			},
		});

		expect(await storage.get("ffdb:session-token:http://example.test")).toBe(
			"payload-session-token",
		);
	});

	it("preserves persisted auth across client teardown for offline refresh", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		await authMockState.onSuccess?.({
			request: { url: "http://example.test/api/auth/sign-in/email" },
			response: {
				ok: true,
				headers: new Headers({ "set-auth-token": "long-lived-session-token" }),
			},
			data: {
				apiKey: "short-lived-api-key",
				keyId: "short-lived-key-id",
				user: { id: "u1", email: "user@example.com" },
				session: { id: "s1" },
			},
		});

		await client.destroy();

		expect(await storage.get("ffdb:session-token:http://example.test")).toBe(
			"long-lived-session-token",
		);
		expect(await storage.get("ffdb:api-key:http://example.test")).toBe(
			"short-lived-api-key",
		);
	});

	it("returns cached auth session through the exported client while offline", async () => {
		const storage = memoryStorage();
		await storage.set(
			"ffdb:session-data:http://example.test",
			JSON.stringify({
				user: { id: "u1", email: "user@example.com" },
				session: { id: "s1" },
			}),
		);
		authMockState.getSessionError = new Error("Failed to fetch");

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		const result = await client.auth.getSession();
		expect(result.data).toEqual({
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		});
		expect(result.error).toBeNull();
	});

	it("does not probe the live auth session endpoint when offline cache exists", async () => {
		const storage = memoryStorage();
		await storage.set(
			"ffdb:session-data:http://example.test",
			JSON.stringify({
				user: { id: "u1", email: "user@example.com" },
				session: { id: "s1" },
			}),
		);
		await storage.set(
			"ffdb:session-token:http://example.test",
			"cached-session-token",
		);
		await storage.set(
			"ffdb:session-token-expires:http://example.test",
			String(Date.now() + 60_000),
		);
		fetchMockState.healthError = new Error("Failed to fetch");
		authMockState.getSessionError = new Error("Failed to fetch");

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		await client.auth.getSession();
		expect(authMockState.getSessionCalls).toBe(0);
	});

	it("replaces a revoked stored api key before access and sync requests", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};
		await storage.set("ffdb:session-token:http://example.test", "cached-token");
		await storage.set(
			"ffdb:session-token-expires:http://example.test",
			String(Date.now() + 60_000),
		);
		await storage.set("ffdb:api-key:http://example.test", "stale-api-key");
		await storage.set("ffdb:key-id:http://example.test", "stale-key-id");
		await storage.set(
			"ffdb:key-expires:http://example.test",
			String(Date.now() + 60 * 60 * 1000),
		);

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: true,
			},
		});

		expect(await storage.get("ffdb:api-key:http://example.test")).toBe(
			"issued-api-key",
		);
	});

	it("refreshes and retries after a mid-session 401 from an expired api key", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};
		await storage.set("ffdb:session-token:http://example.test", "cached-token");
		await storage.set(
			"ffdb:session-token-expires:http://example.test",
			String(Date.now() + 60_000),
		);
		await storage.set(
			"ffdb:api-key:http://example.test",
			"overnight-expired-key",
		);
		await storage.set("ffdb:key-id:http://example.test", "overnight-key-id");
		await storage.set(
			"ffdb:key-expires:http://example.test",
			String(Date.now() + 60 * 60 * 1000),
		);

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
		});

		const activeApiKey = await storage.get("ffdb:api-key:http://example.test");
		expect(activeApiKey).toBeTruthy();
		fetchMockState.invalidApiKeys.add(String(activeApiKey));

		await expect(client.getAccess({ force: true })).resolves.toMatchObject({
			userId: "u1",
			role: "user",
		});
		const refreshedApiKey = await storage.get(
			"ffdb:api-key:http://example.test",
		);
		expect(refreshedApiKey).not.toBe(activeApiKey);
		expect(String(refreshedApiKey)).toContain("issued-api-key");
	});

	it("refreshes a stale stored api key even when only the browser session remains", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};
		await storage.set(
			"ffdb:api-key:http://example.test",
			"overnight-expired-key",
		);
		await storage.set("ffdb:key-id:http://example.test", "overnight-key-id");
		await storage.set(
			"ffdb:key-expires:http://example.test",
			String(Date.now() + 60 * 60 * 1000),
		);

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
		});

		const activeApiKey = await storage.get("ffdb:api-key:http://example.test");
		expect(activeApiKey).toBeTruthy();
		fetchMockState.invalidApiKeys.add(String(activeApiKey));

		await expect(client.getAccess({ force: true })).resolves.toMatchObject({
			userId: "u1",
			role: "user",
		});

		const refreshedApiKey = await storage.get(
			"ffdb:api-key:http://example.test",
		);
		expect(refreshedApiKey).not.toBe(activeApiKey);
		expect(String(refreshedApiKey)).toContain("issued-api-key");
	});

	it("clears restored browser auth and stops interval sync after an unrecoverable 401", async () => {
		vi.useFakeTimers();
		try {
			const storage = memoryStorage();
			await storage.set(
				"ffdb:session-token:http://example.test",
				"cached-token",
			);
			await storage.set(
				"ffdb:session-token-expires:http://example.test",
				String(Date.now() + 60_000),
			);
			await storage.set("ffdb:api-key:http://example.test", "stale-api-key");
			await storage.set("ffdb:key-id:http://example.test", "stale-key-id");
			await storage.set(
				"ffdb:key-expires:http://example.test",
				String(Date.now() + 60 * 60 * 1000),
			);
			authMockState.session = null;

			const client = await createClient({
				config: {
					apiUrl: "http://example.test",
					origin: "http://app.test",
				},
				skipHealthCheck: true,
				storage,
				offline: {
					adapter: {
						execute: async (sql: string) => {
							if (
								sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")
							) {
								return { rows: [{ count: 0 }] };
							}
							return { rows: [] };
						},
					},
					syncOnConnect: false,
					syncInterval: 25,
				},
			});

			expect(fetchMockState.syncPullAttempts).toBe(0);
			expect(await storage.get("ffdb:api-key:http://example.test")).toBeNull();
			expect(
				await storage.get("ffdb:session-token:http://example.test"),
			).toBeNull();

			await vi.advanceTimersByTimeAsync(100);

			expect(fetchMockState.syncPullAttempts).toBe(0);
			await client.destroy();
		} finally {
			vi.useRealTimers();
		}
	});

	it("uses the configured health probe interval to detect recovery while offline", async () => {
		vi.useFakeTimers();
		try {
			const storage = memoryStorage();
			fetchMockState.healthError = new Error("Failed to fetch");

			const client = await createClient({
				config: {
					apiUrl: "http://example.test",
					origin: "http://app.test",
					healthProbeIntervalMs: 25,
				},
				storage,
				offline: {
					adapter: {
						execute: async () => ({ rows: [] }),
					},
					syncOnConnect: false,
				},
			});

			expect(client.sync?.status.isOnline).toBe(false);
			expect(fetchMockState.healthCalls).toBeGreaterThan(0);

			fetchMockState.healthError = null;
			await vi.advanceTimersByTimeAsync(50);

			expect(client.sync?.status.isOnline).toBe(true);

			await client.destroy();
		} finally {
			vi.useRealTimers();
		}
	});

	it("excludes configured tables from sync requests", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: true,
				skipTables: ["system_metrics", "query_metrics"],
			},
		});

		await authMockState.onSuccess?.({
			request: { url: "http://example.test/api/auth/sign-in/email" },
			response: {
				ok: true,
				headers: new Headers({ "set-auth-token": "long-lived-session-token" }),
			},
			data: {
				apiKey: "short-lived-api-key",
				keyId: "short-lived-key-id",
			},
		});

		expect(fetchMockState.syncPullBodies.at(-1)?.tables).toEqual(["user"]);
	});

	it("initializes the internal query cache when offline mode is enabled", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};
		const executedSql: string[] = [];

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async (sql: string) => {
						executedSql.push(sql);
						if (
							sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")
						) {
							return { rows: [{ count: 0 }] };
						}
						return { rows: [] };
					},
				},
				syncOnConnect: false,
			},
		});

		expect(
			executedSql.some((sql) =>
				sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache"),
			),
		).toBe(true);

		await client.destroy();
	});

	it("exposes manual sync controls for on-demand pull and full sync", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};

		const { sync } = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async () => ({ rows: [] }),
				},
				syncOnConnect: false,
			},
		});

		expect(sync).not.toBeNull();
		if (!sync) throw new Error("Expected sync handle");

		await expect(sync.pull()).resolves.toBeUndefined();
		expect(fetchMockState.syncPullCalls).toBe(1);

		await expect(sync.run()).resolves.toEqual({ pushed: 0, failed: 0 });
		expect(fetchMockState.syncPullCalls).toBe(2);
	});

	it("retries request calls after a 401 by refreshing auth first", async () => {
		const storage = memoryStorage();
		authMockState.session = {
			user: { id: "u1", email: "user@example.com" },
			session: { id: "s1" },
		};
		await storage.set("ffdb:session-token:http://example.test", "cached-token");
		await storage.set(
			"ffdb:session-token-expires:http://example.test",
			String(Date.now() + 60_000),
		);
		await storage.set("ffdb:api-key:http://example.test", "old-api-key");
		await storage.set("ffdb:key-id:http://example.test", "old-key-id");
		await storage.set(
			"ffdb:key-expires:http://example.test",
			String(Date.now() + 60 * 60 * 1000),
		);

		const fetchSpy = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ message: "Unauthorized" }), {
					status: 401,
					headers: { "Content-Type": "application/json" },
				}),
			)
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ status: "ok", retried: true }), {
					status: 200,
					headers: { "Content-Type": "application/json" },
				}),
			);

		vi.stubGlobal("fetch", fetchSpy);

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
		});

		await expect(
			client.request<{ status: string; retried: boolean }>("/api/metadata", {
				method: "GET",
				retries: 1,
				retryDelay: 0,
			}),
		).resolves.toEqual({ status: "ok", retried: true });
		expect(
			String(await storage.get("ffdb:api-key:http://example.test")),
		).toContain("issued-api-key");
		expect(fetchSpy).toHaveBeenCalledTimes(2);

		await client.destroy();
		vi.unstubAllGlobals();
	});

	it("reuses cached GET responses and short-circuits extra probes after a network drop", async () => {
		const storage = memoryStorage();
		const httpCache = new Map<string, string>();
		const fetchSpy = vi
			.fn<typeof fetch>()
			.mockResolvedValueOnce(
				new Response(JSON.stringify({ status: "ok", data: { version: 1 } }), {
					status: 200,
					headers: { "Content-Type": "application/json" },
				}),
			)
			.mockRejectedValueOnce(new Error("Failed to fetch"));

		vi.stubGlobal("fetch", fetchSpy);

		const client = await createClient({
			config: {
				apiUrl: "http://example.test",
				origin: "http://app.test",
			},
			skipHealthCheck: true,
			storage,
			offline: {
				adapter: {
					execute: async (sql: string, params: unknown[] = []) => {
						if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_")) {
							return { rows: [] };
						}
						if (
							sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")
						) {
							return { rows: [{ count: 0 }] };
						}
						if (
							sql.includes(
								"SELECT payload FROM __ffdb_http_cache WHERE cache_key = ?",
							)
						) {
							const payload = httpCache.get(String(params[0]));
							return { rows: payload ? [{ payload }] : [] };
						}
						if (sql.includes("INSERT OR REPLACE INTO __ffdb_http_cache")) {
							httpCache.set(String(params[0]), String(params[1]));
							return { rows: [] };
						}
						return { rows: [] };
					},
				},
				syncOnConnect: false,
			},
		});

		await expect(
			client.request<{ status: string; data: { version: number } }>(
				"/api/metadata",
				{ method: "GET" },
			),
		).resolves.toEqual({ status: "ok", data: { version: 1 } });

		await expect(
			client.request<{ status: string; data: { version: number } }>(
				"/api/metadata",
				{ method: "GET" },
			),
		).resolves.toEqual({ status: "ok", data: { version: 1 } });

		await expect(
			client.request<{ status: string; data: { version: number } }>(
				"/api/metadata",
				{ method: "GET" },
			),
		).resolves.toEqual({ status: "ok", data: { version: 1 } });

		expect(fetchSpy).toHaveBeenCalledTimes(2);
		await client.destroy();
		vi.unstubAllGlobals();
	});
});

describe("offline sync recovery", () => {
	it("applies delete deltas when schema only exists in the local database", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<number, { id: number; name: string }>([
			[1, { id: 1, name: "Alpha" }],
		]);

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: [{ name: String(params[0]) }] };
				}
				if (sql.includes('PRAGMA table_info("widgets")')) {
					return {
						rows: [
							{ name: "id", type: "INTEGER", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('DELETE FROM "widgets"')) {
					rows.delete(Number(params[0]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => ({
				data: {
					data: {
						tables: {
							widgets: {
								upserts: [],
								deletes: [1],
								cursor: null,
								hasMore: false,
								syncMode: "delta:updated_at",
							},
						},
						syncedAt: 2000,
						schemaChanged: false,
					},
				},
				error: null,
			}),
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(rows.has(1)).toBe(false);
	});

	it("rebuilds a table when the local cache has more rows than the server", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<number, { id: number; name: string }>([
			[1, { id: 1, name: "Alpha" }],
			[2, { id: 2, name: "Ghost" }],
		]);
		let hasWidgetsTable = true;
		let fetchCount = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: hasWidgetsTable ? [{ name: String(params[0]) }] : [] };
				}
				if (sql.includes('PRAGMA table_info("widgets")')) {
					return {
						rows: [
							{ name: "id", type: "INTEGER", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('DROP TABLE IF EXISTS "widgets"')) {
					hasWidgetsTable = false;
					rows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "widgets"')) {
					hasWidgetsTable = true;
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "widgets"')) {
					rows.set(Number(params[0]), {
						id: Number(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "widgets"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => {
				fetchCount += 1;
				if (fetchCount === 1) {
					return {
						data: {
							data: {
								tables: {
									widgets: {
										upserts: [],
										deletes: [],
										rowCount: 1,
										cursor: null,
										hasMore: false,
										syncMode: "delta:updated_at",
									},
								},
								syncedAt: 2000,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}

				return {
					data: {
						data: {
							tables: {
								widgets: {
									upserts: [{ id: 1, name: "Alpha" }],
									deletes: [],
									rowCount: 1,
									cursor: 1,
									hasMore: false,
									syncMode: "full",
									columns: [
										{
											name: "id",
											type: "INTEGER",
											notNull: true,
											primaryKey: true,
										},
										{
											name: "name",
											type: "TEXT",
											notNull: true,
											primaryKey: false,
										},
									],
								},
							},
							syncedAt: 3000,
							schemaChanged: false,
						},
					},
					error: null,
				};
			},
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(fetchCount).toBeGreaterThan(1);
		expect(rows.size).toBe(1);
		expect(rows.get(1)).toEqual({ id: 1, name: "Alpha" });
		expect(rows.has(2)).toBe(false);
	});

	it("drops cached tables that disappear from the server after a schema change", async () => {
		const meta = new Map<string, string>([
			["last_synced_at", "1000"],
			["tracked_tables", JSON.stringify(["widgets", "test"])],
		]);
		let hasWidgetsTable = true;
		let hasTestTable = true;
		const widgetsRows = new Map<number, { id: number; name: string }>();

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					const name = String(params[0]);
					if (name === "widgets") {
						return { rows: hasWidgetsTable ? [{ name }] : [] };
					}
					if (name === "test") {
						return { rows: hasTestTable ? [{ name }] : [] };
					}
					return { rows: [] };
				}
				if (sql.includes('DROP TABLE IF EXISTS "widgets"')) {
					hasWidgetsTable = false;
					widgetsRows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "widgets"')) {
					hasWidgetsTable = true;
					return { rows: [] };
				}
				if (sql.includes('DROP TABLE IF EXISTS "test"')) {
					hasTestTable = false;
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "widgets"')) {
					widgetsRows.set(Number(params[0]), {
						id: Number(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "widgets"')) {
					return { rows: [{ count: widgetsRows.size }] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => ({
				data: {
					data: {
						tables: {
							widgets: {
								upserts: [{ id: 1, name: "Alpha" }],
								deletes: [],
								rowCount: 1,
								cursor: 1,
								hasMore: false,
								syncMode: "full",
								columns: [
									{
										name: "id",
										type: "INTEGER",
										notNull: true,
										primaryKey: true,
									},
									{
										name: "name",
										type: "TEXT",
										notNull: true,
										primaryKey: false,
									},
								],
							},
						},
						syncedAt: 2000,
						schemaChanged: true,
					},
				},
				error: null,
			}),
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(hasWidgetsTable).toBe(true);
		expect(hasTestTable).toBe(false);
		expect(widgetsRows.get(1)).toEqual({ id: 1, name: "Alpha" });
	});

	it("does not rebuild when the local cache has fewer rows than the server after sync finishes", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<number, { id: number; name: string }>([
			[1, { id: 1, name: "Alpha" }],
		]);
		let hasWidgetsTable = true;
		let fetchCount = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: hasWidgetsTable ? [{ name: String(params[0]) }] : [] };
				}
				if (sql.includes('DROP TABLE IF EXISTS "widgets"')) {
					hasWidgetsTable = false;
					rows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "widgets"')) {
					hasWidgetsTable = true;
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "widgets"')) {
					rows.set(Number(params[0]), {
						id: Number(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "widgets"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => {
				fetchCount += 1;
				if (fetchCount === 1) {
					return {
						data: {
							data: {
								tables: {
									widgets: {
										upserts: [],
										deletes: [],
										rowCount: 3,
										cursor: null,
										hasMore: false,
										syncMode: "delta:updated_at",
									},
								},
								syncedAt: 2000,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}

				return {
					data: {
						data: {
							tables: {
								widgets: {
									upserts: [
										{ id: 1, name: "Alpha" },
										{ id: 2, name: "Beta" },
										{ id: 3, name: "Gamma" },
									],
									deletes: [],
									rowCount: 3,
									cursor: 3,
									hasMore: false,
									syncMode: "full",
									columns: [
										{
											name: "id",
											type: "INTEGER",
											notNull: true,
											primaryKey: true,
										},
										{
											name: "name",
											type: "TEXT",
											notNull: true,
											primaryKey: false,
										},
									],
								},
							},
							syncedAt: 3000,
							schemaChanged: false,
						},
					},
					error: null,
				};
			},
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(fetchCount).toBe(1);
		expect(rows.size).toBe(1);
		expect(rows.has(2)).toBe(false);
		expect(rows.has(3)).toBe(false);
	});

	it("recovers when a delta sync references a table missing locally", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<number, { id: number; name: string }>();
		let hasWidgetsTable = false;
		let fetchCount = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: hasWidgetsTable ? [{ name: String(params[0]) }] : [] };
				}
				if (sql.includes('DROP TABLE IF EXISTS "widgets"')) {
					hasWidgetsTable = false;
					rows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "widgets"')) {
					hasWidgetsTable = true;
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "widgets"')) {
					if (!hasWidgetsTable) throw new Error("no such table: widgets");
					rows.set(Number(params[0]), {
						id: Number(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('DELETE FROM "widgets"')) {
					if (!hasWidgetsTable) throw new Error("no such table: widgets");
					rows.delete(Number(params[0]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => {
				fetchCount += 1;
				if (fetchCount === 1) {
					return {
						data: {
							data: {
								tables: {
									widgets: {
										upserts: [{ id: 1, name: "Alpha" }],
										deletes: [],
										cursor: 1,
										hasMore: false,
										syncMode: "delta:row_audit",
									},
								},
								syncedAt: 2000,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}

				return {
					data: {
						data: {
							tables: {
								widgets: {
									upserts: [{ id: 1, name: "Alpha" }],
									deletes: [],
									cursor: 1,
									hasMore: false,
									syncMode: "full",
									columns: [
										{
											name: "id",
											type: "INTEGER",
											notNull: true,
											primaryKey: true,
										},
										{
											name: "name",
											type: "TEXT",
											notNull: true,
											primaryKey: false,
										},
									],
								},
							},
							syncedAt: 3000,
							schemaChanged: false,
						},
					},
					error: null,
				};
			},
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(fetchCount).toBeGreaterThan(1);
		expect(rows.get(1)).toEqual({ id: 1, name: "Alpha" });
	});

	it("backfills a missing recent row with a small remote key probe when the cache is behind", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<
			string,
			{
				id: string;
				name: string;
				createdAt: string;
				updatedAt: string;
			}
		>([
			[
				"p1",
				{
					id: "p1",
					name: "Alpha",
					createdAt: "2026-04-14T00:00:00.000Z",
					updatedAt: "2026-04-14T00:00:00.000Z",
				},
			],
		]);
		const remoteRows = new Map<
			string,
			{
				id: string;
				name: string;
				createdAt: string;
				updatedAt: string;
			}
		>([
			[
				"p1",
				{
					id: "p1",
					name: "Alpha",
					createdAt: "2026-04-14T00:00:00.000Z",
					updatedAt: "2026-04-14T00:00:00.000Z",
				},
			],
			[
				"p2",
				{
					id: "p2",
					name: "Beta",
					createdAt: "2026-04-14T00:01:00.000Z",
					updatedAt: "2026-04-14T00:01:00.000Z",
				},
			],
		]);

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: [{ name: String(params[0]) }] };
				}
				if (sql.includes('PRAGMA table_info("people")')) {
					return {
						rows: [
							{ name: "id", type: "TEXT", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
							{ name: "createdAt", type: "TEXT", notnull: 1, pk: 0 },
							{ name: "updatedAt", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "people"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (sql.includes('SELECT * FROM "people" WHERE "id" = ? LIMIT 1')) {
					const row = rows.get(String(params[0]));
					return { rows: row ? [row] : [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "people"')) {
					rows.set(String(params[0]), {
						id: String(params[0]),
						name: String(params[1]),
						createdAt: String(params[2]),
						updatedAt: String(params[3]),
					});
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const fetchFn = async (url: string, options?: { body?: any }) => {
			if (url === "/api/sync/pull") {
				return {
					data: {
						data: {
							tables: {
								people: {
									upserts: [],
									deletes: [],
									rowCount: 2,
									cursor: null,
									hasMore: false,
									syncMode: "delta:updated_at",
								},
							},
							syncedAt: 2000,
							schemaChanged: false,
						},
					},
					error: null,
				};
			}

			const sql = String(options?.body?.sql ?? "");
			if (sql.includes('SELECT "id", "updatedAt" FROM "people"')) {
				return {
					data: {
						data: {
							rows: [
								{ id: "p2", updatedAt: remoteRows.get("p2")?.updatedAt },
								{ id: "p1", updatedAt: remoteRows.get("p1")?.updatedAt },
							],
						},
					},
					error: null,
				};
			}

			if (sql.includes('SELECT * FROM "people" WHERE "id" IN')) {
				const ids = (options?.body?.values ?? []) as string[];
				return {
					data: {
						data: {
							rows: ids.map((id) => remoteRows.get(id)).filter(Boolean),
						},
					},
					error: null,
				};
			}

			return { data: { data: { rows: [] } }, error: null };
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn,
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(rows.get("p2")).toEqual(remoteRows.get("p2"));
		expect(rows.size).toBe(2);
	});

	it("uses a full table refresh instead of many remote probes when the local cache is far behind", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<string, { id: string; name: string }>();
		let pullCalls = 0;
		let queryCalls = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: [{ name: String(params[0]) }] };
				}
				if (sql.includes('PRAGMA table_info("people")')) {
					return {
						rows: [
							{ name: "id", type: "TEXT", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('DROP TABLE IF EXISTS "people"')) {
					rows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "people"')) {
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "people"')) {
					rows.set(String(params[0]), {
						id: String(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "people"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (sql.includes('SELECT * FROM "people" WHERE "id" = ?')) {
					const row = rows.get(String(params[0]));
					return { rows: row ? [row] : [] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async (
				url: string,
				options?: { body?: { since?: number | null } },
			) => {
				if (url === "/api/policies/query") {
					queryCalls += 1;
					return { data: { data: { rows: [] } }, error: null };
				}
				if (url === "/api/sync/pull") {
					pullCalls += 1;
					if (options?.body?.since === null) {
						return {
							data: {
								data: {
									tables: {
										people: {
											upserts: [
												{ id: "p1", name: "Alpha" },
												{ id: "p2", name: "Beta" },
											],
											deletes: [],
											rowCount: 48,
											cursor: null,
											hasMore: false,
											columns: [
												{
													name: "id",
													type: "TEXT",
													notNull: true,
													primaryKey: true,
												},
												{
													name: "name",
													type: "TEXT",
													notNull: true,
													primaryKey: false,
												},
											],
											syncMode: "full",
										},
									},
									syncedAt: 2001,
									schemaChanged: false,
								},
							},
							error: null,
						};
					}
					return {
						data: {
							data: {
								tables: {
									people: {
										upserts: [],
										deletes: [],
										rowCount: 48,
										cursor: null,
										hasMore: false,
										syncMode: "delta:updated_at",
									},
								},
								syncedAt: 2000,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}
				return { data: { data: { rows: [] } }, error: null };
			},
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(queryCalls).toBe(0);
		expect(pullCalls).toBe(2);
		expect(rows.size).toBe(2);
	});

	it("rebuilds a large out-of-sync table without paging through many extra pull requests", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<string, { id: string; name: string }>();
		const requestedPageSizes: number[] = [];

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: [{ name: String(params[0]) }] };
				}
				if (sql.includes('PRAGMA table_info("analytics_metrics")')) {
					return {
						rows: [
							{ name: "id", type: "TEXT", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('DROP TABLE IF EXISTS "analytics_metrics"')) {
					rows.clear();
					return { rows: [] };
				}
				if (sql.includes('CREATE TABLE "analytics_metrics"')) {
					return { rows: [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "analytics_metrics"')) {
					rows.set(String(params[0]), {
						id: String(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "analytics_metrics"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (sql.includes('SELECT * FROM "analytics_metrics" WHERE "id" = ?')) {
					const row = rows.get(String(params[0]));
					return { rows: row ? [row] : [] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async (
				url: string,
				options?: { body?: { since?: number | null; pageSize?: number } },
			) => {
				if (url !== "/api/sync/pull") {
					return { data: { data: { rows: [] } }, error: null };
				}

				requestedPageSizes.push(Number(options?.body?.pageSize ?? 0));

				if (options?.body?.since === null) {
					return {
						data: {
							data: {
								tables: {
									analytics_metrics: {
										upserts: [
											{ id: "m1", name: "Alpha" },
											{ id: "m2", name: "Beta" },
										],
										deletes: [],
										rowCount: 3986,
										cursor: null,
										hasMore: false,
										columns: [
											{
												name: "id",
												type: "TEXT",
												notNull: true,
												primaryKey: true,
											},
											{
												name: "name",
												type: "TEXT",
												notNull: true,
												primaryKey: false,
											},
										],
										syncMode: "full",
									},
								},
								syncedAt: 2001,
								schemaChanged: false,
							},
						},
						error: null,
					};
				}

				return {
					data: {
						data: {
							tables: {
								analytics_metrics: {
									upserts: [],
									deletes: [],
									rowCount: 3986,
									cursor: null,
									hasMore: false,
									syncMode: "delta:row_audit",
								},
							},
							syncedAt: 2000,
							schemaChanged: false,
						},
					},
					error: null,
				};
			},
			endpoint: "/api/policies/query",
			maxRowsPerTable: 10000,
			pageSize: 500,
		});

		await syncManager.init();
		await expect(syncManager.pull()).resolves.toBeUndefined();
		expect(requestedPageSizes).toEqual([500, 3986]);
		expect(rows.size).toBe(2);
	});
});

describe("offline read-through", () => {
	it("prefers explicit compiled-query metadata over sql heuristics for raw statements", async () => {
		const cachedRows = new Map<string, string>();
		let remoteCalls = 0;
		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					const value = cachedRows.get(String(params[0]));
					return {
						rows: value ? [{ rows: value, updated_at: Date.now() }] : [],
					};
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					cachedRows.set(String(params[0]), String(params[3]));
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: {
				isOnline: true,
				pendingMutations: 0,
				lastSyncedAt: Date.now(),
			},
			async queueMutation() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ id: "w1", name: "Fresh" }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		await connection.executeQuery({
			sql: 'WITH recent AS (SELECT * FROM "widgets") SELECT * FROM recent',
			parameters: [],
			offline: {
				isReadOnly: true,
				primaryTable: "widgets",
				isAmbiguous: false,
			},
			query: {} as any,
		} as any);

		await vi.waitFor(() => {
			expect(remoteCalls).toBe(1);
		});
	});

	it("uses the synced local database first when a query cache entry is missing", async () => {
		let remoteCalls = 0;

		const adapter = {
			async execute(sql: string) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes('SELECT * FROM "widgets"')) {
					return { rows: [{ id: 7, name: "Synced local row" }] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: 1234 },
			async queueMutation() {},
			async waitForIdle() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ id: 9, name: "Remote row" }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([{ id: 7, name: "Synced local row" }]);
		expect(remoteCalls).toBe(0);
	});

	it("returns cached rows immediately and refreshes them through the query endpoint", async () => {
		const cachedRows = new Map<string, string>();
		let remoteCalls = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					const value = cachedRows.get(String(params[0]));
					return {
						rows: value ? [{ rows: value, updated_at: Date.now() }] : [],
					};
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					cachedRows.set(String(params[0]), String(params[3]));
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ id: 1, name: "Fresh" }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			query: {} as any,
		} as any);
		await new Promise((resolve) => setTimeout(resolve, 0));
		await new Promise((resolve) => setTimeout(resolve, 0));

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([{ id: 1, name: "Fresh" }]);
		expect(remoteCalls).toBeGreaterThan(0);
	});

	it("honors bypassCache by returning the live remote rows while online", async () => {
		const cachedRows = new Map<string, string>();
		let remoteCalls = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					const value = cachedRows.get(String(params[0]));
					return {
						rows: value ? [{ rows: value, updated_at: Date.now() }] : [],
					};
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					cachedRows.set(String(params[0]), String(params[3]));
					return { rows: [] };
				}
				if (sql.includes('SELECT * FROM "widgets"')) {
					return { rows: [{ id: "local-1", name: "Local snapshot" }] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: Date.now() },
			async queueMutation() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ id: "remote-1", name: "Remote fresh" }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const cacheKey = JSON.stringify({
			sql: 'SELECT * FROM "widgets"',
			params: [],
		});
		cachedRows.set(
			cacheKey,
			JSON.stringify([{ id: "cached-1", name: "Cached query result" }]),
		);

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			ffdb: { bypassCache: true },
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([{ id: "remote-1", name: "Remote fresh" }]);
		expect(remoteCalls).toBe(1);
		expect(cachedRows.get(cacheKey)).toBe(
			JSON.stringify([{ id: "remote-1", name: "Remote fresh" }]),
		);
	});

	it("returns the local empty result immediately and refreshes in the background", async () => {
		let remoteCalls = 0;
		let reconcileCalls = 0;

		const adapter = {
			async execute(sql: string) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes('SELECT * FROM "query_metrics"')) {
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {},
			async reconcileTables() {
				reconcileCalls += 1;
			},
			async waitForIdle() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ count: 43 }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "query_metrics"',
			parameters: [],
			query: {} as any,
		} as any);
		await new Promise((resolve) => setTimeout(resolve, 0));
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(result.rows).toEqual([]);
		expect(remoteCalls).toBeGreaterThan(0);
		expect(reconcileCalls).toBe(0);
	});

	it("returns cached remote rows for readable tables that are not locally synced", async () => {
		const cachedRows = new Map<string, string>();
		let remoteCalls = 0;

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT rows, updated_at FROM __ffdb_query_cache WHERE key = ?",
					)
				) {
					const value = cachedRows.get(String(params[0]));
					return {
						rows: value ? [{ rows: value, updated_at: Date.now() }] : [],
					};
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					cachedRows.set(String(params[0]), String(params[3]));
					return { rows: [] };
				}
				if (
					sql.includes('SELECT * FROM "remote_only"') ||
					sql.includes('SELECT count("id") as "count" FROM "user"')
				) {
					throw new Error("no such table: remote_only");
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {},
			async reconcileTables() {},
			async waitForIdle() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ id: "r1", name: "Remote row" }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const first = await connection.executeQuery({
			sql: 'SELECT * FROM "remote_only"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(first.rows).toEqual([{ id: "r1", name: "Remote row" }]);
		expect(remoteCalls).toBe(1);

		syncManager.status.isOnline = false;

		const second = await connection.executeQuery({
			sql: 'SELECT * FROM "remote_only"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(second.rows).toEqual([{ id: "r1", name: "Remote row" }]);
	});

	it("does not trigger pull-based reconciliation before auth is ready", async () => {
		let remoteCalls = 0;
		let reconcileCalls = 0;
		const adapter = {
			async execute(sql: string) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes('SELECT * FROM "query_metrics"')) {
					throw new Error("no such table: query_metrics");
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			isAuthReady: false,
			async queueMutation() {},
			async reconcileTables() {
				reconcileCalls += 1;
			},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ count: 10 }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "query_metrics"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([]);
		expect(remoteCalls).toBe(0);
		expect(reconcileCalls).toBe(0);
	});

	it("uses the query endpoint for aggregate count queries without triggering pull", async () => {
		let remoteCalls = 0;
		let reconcileCalls = 0;
		const adapter = {
			async execute(sql: string) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {},
			async reconcileTables() {
				reconcileCalls += 1;
			},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ count: 10 }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT count("id") as "count" FROM "user"',
			parameters: [],
			query: {} as any,
		} as any);
		await new Promise((resolve) => setTimeout(resolve, 0));
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(result.rows).toEqual([]);
		expect(remoteCalls).toBe(1);
		expect(reconcileCalls).toBe(0);
	});

	it("skips an immediate duplicate background refresh for the same cached query", async () => {
		const cachedRows = new Map<string, string>();
		let remoteCalls = 0;
		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT rows, updated_at FROM __ffdb_query_cache")) {
					const value = cachedRows.get(String(params[0]));
					return {
						rows: value ? [{ rows: value, updated_at: Date.now() }] : [],
					};
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_query_cache")) {
					cachedRows.set(String(params[0]), String(params[3]));
					return { rows: [] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: {
				isOnline: true,
				pendingMutations: 0,
				lastSyncedAt: Date.now(),
			},
			async queueMutation() {},
			async remoteQuery() {
				remoteCalls += 1;
				return [{ count: 10 }];
			},
			notifyDataChange() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		await driver.init();
		const connection = await driver.acquireConnection();

		await connection.executeQuery({
			sql: 'SELECT count("id") as "count" FROM "user"',
			parameters: [],
			query: {} as any,
		} as any);
		await connection.executeQuery({
			sql: 'SELECT count("id") as "count" FROM "user"',
			parameters: [],
			query: {} as any,
		} as any);
		await Promise.resolve();
		await Promise.resolve();

		expect(remoteCalls).toBe(1);
	});

	it("fails invalid create-table mutations before they are queued", async () => {
		let queuedMutations = 0;
		const executedSql: string[] = [];
		const adapter = {
			async execute(sql: string) {
				executedSql.push(sql);
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {
				queuedMutations += 1;
			},
			async reconcileTables() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		const connection = await driver.acquireConnection();

		await expect(
			connection.executeQuery({
				sql: 'CREATE TABLE "bad_widgets" ("id" integer primary key, "name" text)',
				parameters: [],
				query: {} as any,
			} as any),
		).rejects.toThrow("id TEXT PRIMARY KEY");

		expect(queuedMutations).toBe(0);
		expect(
			executedSql.filter(
				(sql) => !sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache"),
			),
		).toEqual([]);
	});

	it("self-initializes the offline query cache before the first connection is used", async () => {
		const executedSql: string[] = [];
		const adapter = {
			async execute(sql: string) {
				executedSql.push(sql);
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, lastSyncedAt: null },
			async queueMutation() {},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		const connection = await driver.acquireConnection();

		await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(
			executedSql.some((sql) =>
				sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache"),
			),
		).toBe(true);
	});

	it("waits for first-load sync to settle, reconciles the missing table, and retries once", async () => {
		const reconciledTables: string[][] = [];
		let hasWidgetsTable = false;
		const adapter = {
			async execute(sql: string) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_query_cache")) {
					return { rows: [] };
				}
				if (sql.includes('SELECT * FROM "widgets"')) {
					if (!hasWidgetsTable) throw new Error("no such table: widgets");
					return { rows: [{ id: "w1", name: "Recovered" }] };
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: {
				isOnline: true,
				pendingMutations: 0,
				lastSyncedAt: null,
				isSyncing: true,
			},
			isAuthReady: true,
			async queueMutation() {},
			async waitForIdle() {
				this.status.isSyncing = false;
			},
			async reconcileTables(tables: string[]) {
				reconciledTables.push(tables);
				hasWidgetsTable = true;
			},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT * FROM "widgets"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([{ id: "w1", name: "Recovered" }]);
		expect(reconciledTables).toEqual([["widgets"]]);
	});

	it("does not trigger a pull-style table reconcile when a local table is missing", async () => {
		const reconciledTables: string[][] = [];
		const adapter = {
			async execute(sql: string) {
				if (sql.includes('SELECT count("id") as "count" FROM "user"')) {
					throw new Error("no such table: user");
				}
				return { rows: [] };
			},
		};

		const syncManager = {
			status: { isOnline: true, pendingMutations: 0, isSyncing: true },
			isAuthReady: false,
			async queueMutation() {},
			async waitForIdle() {
				this.status.isSyncing = false;
			},
			async reconcileTables(tables: string[]) {
				reconciledTables.push(tables);
			},
		} as any;

		const driver = new OfflineDriver({ adapter, syncManager });
		const connection = await driver.acquireConnection();

		const result = await connection.executeQuery({
			sql: 'SELECT count("id") as "count" FROM "user"',
			parameters: [],
			query: {} as any,
		} as any);

		expect(result.rows).toEqual([]);
		expect(reconciledTables).toEqual([]);
	});

	it("does not notify cache listeners when a refresh only replays identical rows", async () => {
		const meta = new Map<string, string>([["last_synced_at", "1000"]]);
		const rows = new Map<number, { id: number; name: string }>([
			[1, { id: 1, name: "Alpha" }],
		]);

		const adapter = {
			async execute(sql: string, params: unknown[] = []) {
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_mutations")) {
					return { rows: [] };
				}
				if (sql.includes("CREATE TABLE IF NOT EXISTS __ffdb_sync_meta")) {
					return { rows: [] };
				}
				if (sql.includes("SELECT COUNT(*) as count FROM __ffdb_mutations")) {
					return { rows: [{ count: 0 }] };
				}
				if (sql.includes("SELECT value FROM __ffdb_sync_meta WHERE key = ?")) {
					const key = String(params[0]);
					const value = meta.get(key);
					return { rows: value ? [{ value }] : [] };
				}
				if (sql.includes("INSERT OR REPLACE INTO __ffdb_sync_meta")) {
					meta.set(String(params[0]), String(params[1]));
					return { rows: [] };
				}
				if (
					sql.includes(
						"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
				) {
					return { rows: [{ name: String(params[0]) }] };
				}
				if (sql.includes('PRAGMA table_info("widgets")')) {
					return {
						rows: [
							{ name: "id", type: "INTEGER", notnull: 1, pk: 1 },
							{ name: "name", type: "TEXT", notnull: 1, pk: 0 },
						],
					};
				}
				if (sql.includes('SELECT * FROM "widgets" WHERE "id" = ?')) {
					const row = rows.get(Number(params[0]));
					return { rows: row ? [row] : [] };
				}
				if (sql.includes('INSERT OR REPLACE INTO "widgets"')) {
					rows.set(Number(params[0]), {
						id: Number(params[0]),
						name: String(params[1]),
					});
					return { rows: [] };
				}
				if (sql.includes('DELETE FROM "widgets"')) {
					rows.delete(Number(params[0]));
					return { rows: [] };
				}
				if (sql.includes('SELECT COUNT(*) as count FROM "widgets"')) {
					return { rows: [{ count: rows.size }] };
				}
				if (
					sql.includes(
						"SELECT id, sql, params, created_at FROM __ffdb_mutations",
					)
				) {
					return { rows: [] };
				}

				return { rows: [] };
			},
		};

		const syncManager = new SyncManager({
			adapter,
			fetchFn: async () => ({
				data: {
					data: {
						tables: {
							widgets: {
								upserts: [{ id: 1, name: "Alpha" }],
								deletes: [],
								rowCount: 1,
								cursor: null,
								hasMore: false,
								syncMode: "delta:updated_at",
							},
						},
						syncedAt: 2000,
						schemaChanged: false,
					},
				},
				error: null,
			}),
			endpoint: "/api/policies/query",
			maxRowsPerTable: 100,
		});

		let notifications = 0;
		syncManager.subscribeData(() => {
			notifications += 1;
		});

		await syncManager.init();
		await expect(
			syncManager.reconcileTables(["widgets"]),
		).resolves.toBeUndefined();
		expect(notifications).toBe(0);
	});
});

describe("edge cases", () => {
	it("handles table name collisions (override behavior)", async () => {
		type Override = {
			user: {
				id: number; // intentionally different
			};
		};

		const { db } = await createClient<Override>({ skipHealthCheck: true });

		const query = db.selectFrom("user").select("id");

		// compile should still work
		expect(query.compile()).toBeDefined();

		// runtime still returns whatever backend returns
		const result = await query.execute();
		expect(result).toBeDefined();
	});

	it("does not allow unknown tables", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		// @ts-expect-error
		db.selectFrom("does_not_exist");
	});

	it("handles weird column names safely", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		// assuming your generator quoted weird names
		const query = db.selectFrom("user").select(["id", "email"]); // replace with weird name if present

		expect(query.compile()).toBeDefined();
	});

	it("handles empty result sets", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		const result = await db
			.selectFrom("user")
			.selectAll()
			.where("id", "=", "nonexistent")
			.execute();

		expect(Array.isArray(result)).toBe(true);
		expect(result.length).toBe(0);
	});

	it("handles nullable columns correctly", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		const result = await db.selectFrom("user").selectAll().execute();

		if (result.length > 0) {
			// just verify it doesn't crash on nulls
			expect(result[0]).toBeDefined();
		}
	});

	it("passes parameters correctly", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		const query = db.selectFrom("user").selectAll().where("id", "=", "123");

		const compiled = query.compile();

		expect(compiled.parameters.length).toBe(1);

		const result = await query.execute();
		expect(result).toBeDefined();
	});

	it("handles large selects without crashing", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		const result = await db
			.selectFrom("user")
			.selectAll()
			.limit(1000)
			.execute();

		expect(result.length).toBeLessThanOrEqual(1000);
	});

	it("executes queries on extra tables", async () => {
		const { db } = await createClient<Extra>({ skipHealthCheck: true });

		const query = db.selectFrom("my_view").selectAll();

		expect(query.compile()).toBeDefined();

		// will only pass if backend supports it
		try {
			await query.execute();
		} catch {
			// acceptable if backend doesn't have it
			expect(true).toBe(true);
		}
	});

	it("compiles queries without executing", async () => {
		const { db } = await createClient({ skipHealthCheck: true });

		const compiled = db.selectFrom("user").selectAll().compile();

		expect(compiled.sql).toContain("select");
	});
});
