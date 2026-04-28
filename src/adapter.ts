import type {
	CompiledQuery,
	DatabaseConnection,
	Driver,
	QueryResult,
} from "kysely";

export type FetchFn = (
	url: string,
	options?: Record<string, unknown>,
) => Promise<{ data: unknown; error: unknown }>;

// The HTTP driver is the thin transport layer used when queries go straight to
// the backend instead of through the offline SQLite driver. It only needs the
// policy-query endpoint and a fetch implementation that already knows how to
// attach auth headers, retries, and runtime-specific behavior.
type HttpDriverConfig = {
	endpoint: string;
	fetch: FetchFn;
	getApiKey?: () => string | null;
};

export class HttpDriver implements Driver {
	#endpoint: string;
	#fetch: FetchFn;
	#getApiKey: () => string | null;

	constructor(config: HttpDriverConfig) {
		this.#endpoint = config.endpoint;
		this.#fetch = config.fetch;
		this.#getApiKey = config.getApiKey ?? (() => null);
	}

	// The remote SQL path is stateless from the driver's point of view, so Kysely's
	// lifecycle hooks mostly collapse to no-ops. The backend owns actual query
	// execution and transaction boundaries.
	async init(): Promise<void> {}

	async acquireConnection(): Promise<DatabaseConnection> {
		return new HttpConnection(this.#endpoint, this.#fetch, this.#getApiKey);
	}

	async beginTransaction(): Promise<void> {}
	async commitTransaction(): Promise<void> {}
	async rollbackTransaction(): Promise<void> {}
	async releaseConnection(): Promise<void> {}
	async destroy(): Promise<void> {}
}

export type QueryMeta = {
	cached?: boolean;
	bypassedCache?: boolean;
	rowCount?: number;
	durationMs?: number;
	statementType?: string;
	table?: string | null;
	invalidatedCache?: boolean;
	changes?: number | bigint;
	lastInsertRowid?: number | bigint;
	audited?: boolean;
	success?: boolean;
	[key: string]: unknown;
};

// The backend wraps SQL responses in the same `{ data: ... }` envelope used by
// the rest of the API, so the adapter peels that layer off before handing rows
// back to Kysely callers.
type SqlResponse = {
	data: {
		rows: unknown[];
		meta: QueryMeta;
	};
};

class HttpConnection implements DatabaseConnection {
	#endpoint: string;
	#fetch: FetchFn;
	#getApiKey: () => string | null;

	constructor(
		endpoint: string,
		fetchFn: FetchFn,
		getApiKey: () => string | null,
	) {
		this.#endpoint = endpoint;
		this.#fetch = fetchFn;
		this.#getApiKey = getApiKey;
	}

	async executeQuery<O>(
		compiledQuery: CompiledQuery,
	): Promise<QueryResult<O> & { meta: QueryMeta }> {
		const apiKey = this.#getApiKey();

		// FFDB-specific query options ride along on the compiled query object so raw
		// SQL helpers and generated query builders can both influence transport
		// behavior without changing Kysely's public types.
		const ffdbOptions = (
			compiledQuery as CompiledQuery & {
				ffdb?: { bypassCache?: boolean };
			}
		).ffdb;

		// The backend expects both the structured Kysely query and the compiled SQL.
		// SQL is what powers policy evaluation and raw execution, while the original
		// query object stays available for any server-side tooling that wants the more
		// structured representation.
		const { data, error } = (await this.#fetch(this.#endpoint, {
			method: "POST",
			headers: apiKey ? { "x-api-key": apiKey } : undefined,
			body: {
				query: compiledQuery.query,
				sql: compiledQuery.sql,
				values: compiledQuery.parameters,
				// `bypassCache` must be forwarded explicitly so callers can force the
				// backend to answer from the live database instead of its policy-query
				// cache when they need a current snapshot.
				bypassCache: ffdbOptions?.bypassCache === true,
			},
		})) as { data: SqlResponse | null; error: { statusText?: string } | null };

		if (error) {
			throw new Error(error.statusText ?? "Query failed");
		}

		return {
			rows: (data?.data?.rows ?? []) as O[],
			meta: data?.data?.meta ?? {},
		};
	}

	async *streamQuery<O>(
		compiledQuery: CompiledQuery,
	): AsyncIterableIterator<QueryResult<O>> {
		// The HTTP transport does not support server-side cursors yet, so the
		// streaming contract is satisfied by yielding the full query result once.
		const result = await this.executeQuery<O>(compiledQuery);
		yield result;
	}
}
