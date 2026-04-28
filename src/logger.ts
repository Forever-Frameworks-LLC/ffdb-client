import type { FFDB_Config } from "./config.ts";

export type FFDBLogger = {
	enabled: boolean;
	verbose: boolean;
	info: (message: string, meta?: unknown) => void;
	warn: (message: string, meta?: unknown) => void;
	error: (message: string, meta?: unknown) => void;
	debug: (message: string, meta?: unknown) => void;
};

// All client logging goes through one tiny formatter so the rest of the package
// can log structured metadata without each call site worrying about console
// availability, prefixes, or whether logging is currently enabled.
function emit(
	method: "info" | "warn" | "error" | "debug",
	enabled: boolean,
	message: string,
	meta?: unknown,
) {
	if (!enabled) return;

	const prefix = `[ffdb] ${message}`;
	const writer = console[method] as (...args: unknown[]) => void;

	if (meta === undefined) {
		writer(prefix);
		return;
	}

	writer(prefix, meta);
}

export function createClientLogger(
	config: Pick<FFDB_Config, "logEnabled" | "logLevel">,
): FFDBLogger {
	const enabled = config.logEnabled === true;
	// `verbose` intentionally gates only debug noise. Info/warn/error remain tied to
	// the top-level enabled flag so operational logs still appear when logging is on
	// without forcing every consumer into high-volume trace output.
	const verbose = enabled && config.logLevel === "verbose";

	return {
		enabled,
		verbose,
		info: (message, meta) => emit("info", enabled, message, meta),
		warn: (message, meta) => emit("warn", enabled, message, meta),
		error: (message, meta) => emit("error", enabled, message, meta),
		debug: (message, meta) => emit("debug", verbose, message, meta),
	};
}

// Some code paths need a logger object before real config is available. Exporting
// a shape-compatible noop logger lets those paths stay simple without sprinkling
// optional chaining through the codebase.
export const noopLogger: FFDBLogger = {
	enabled: false,
	verbose: false,
	info: () => {},
	warn: () => {},
	error: () => {},
	debug: () => {},
};
