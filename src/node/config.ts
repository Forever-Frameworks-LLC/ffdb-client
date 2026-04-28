import { config as dotenvConfig } from "dotenv";
import { defaultConfig, type FFDB_Config } from "../config.ts";

export type { FFDB_Config } from "../config.ts";

// Node env files are string-only, so numeric options need an explicit parse step
// before they can be merged with the strongly typed client config.
function parseOptionalNumber(value: string | undefined, fallback?: number) {
	if (value === undefined || value.trim() === "") {
		return fallback;
	}

	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : fallback;
}

// Load `.env` values into the same config shape used by the runtime client.
// That keeps CLIs, Node services, and test helpers aligned with browser/client
// config semantics instead of maintaining a separate Node-only config format.
export function loadEnvConfig(
	options: { path?: string; quiet?: boolean } = {},
): FFDB_Config {
	// `dotenv` is called at read time instead of module init so callers can point
	// specific commands at alternate env files such as staging or production.
	dotenvConfig({ path: options.path, quiet: options.quiet ?? true });

	return {
		// Environment variables override package defaults, but everything still
		// resolves to a full config object so downstream code can avoid repeated
		// undefined checks.
		origin: process.env.ORIGIN || defaultConfig.origin,
		apiUrl: process.env.API_URL || defaultConfig.apiUrl,
		endpoint: process.env.ENDPOINT || defaultConfig.endpoint,
		// Node workflows can authenticate either with the documented developer
		// bearer token or a direct API key, so both are surfaced here and the caller
		// decides which path to use.
		authToken: process.env.FFDB_AUTH_TOKEN || defaultConfig.authToken,
		apiKey: process.env.FFDB_API_KEY || defaultConfig.apiKey,
		retryAttempts: parseOptionalNumber(
			process.env.RETRY_ATTEMPTS,
			defaultConfig.retryAttempts,
		),
		healthProbeIntervalMs: parseOptionalNumber(
			process.env.HEALTH_PROBE_INTERVAL_MS,
			defaultConfig.healthProbeIntervalMs,
		),
		logLevel:
			(process.env.LOG_LEVEL as "info" | "verbose") || defaultConfig.logLevel,
		logEnabled:
			process.env.LOG_ENABLED === undefined
				? defaultConfig.logEnabled
				: process.env.LOG_ENABLED === "true",
	};
}

// Most small Node scripts want a ready-to-use config as soon as the module is
// imported, so we expose an eagerly loaded default alongside `loadEnvConfig()`
// for callers that need custom env-file selection.
export const config: FFDB_Config = loadEnvConfig();
