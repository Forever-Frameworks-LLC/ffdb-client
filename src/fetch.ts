import { createFetch } from "@better-fetch/fetch";
import { logger } from "@better-fetch/logger";

import type { FetchFn } from "./adapter.ts";
import type { FFDB_Config } from "./config.ts";

export type CreateFetchClientOptions = {
	config: FFDB_Config;
};

export const createFetchClient = (options: CreateFetchClientOptions) => {
	const { config } = options;
	const bearerToken = config.authToken?.trim() ?? "";
	const apiKey = config.apiKey?.trim() ?? "";

	return createFetch({
		baseURL: config.apiUrl,
		credentials: "include",
		headers: apiKey
			? {
					"x-api-key": apiKey,
				}
			: undefined,
		auth: bearerToken
			? {
					type: "Bearer",
					token: bearerToken,
				}
			: undefined,
		retry: {
			type: "linear",
			attempts: config.retryAttempts || 0,
			delay: 1000,
		},
		plugins: [
			logger({
				enabled: config.logEnabled,
				verbose: config.logLevel === "verbose",
			}),
		],
	});
};

export type FetchClient = FetchFn;
