export type FFDB_Config = {
	origin: string;
	apiUrl: string;
	endpoint: string;
	authToken: string;
	apiKey: string;
	retryAttempts?: number;
	/** Interval in ms for backend health recovery probes while offline. Set to 0 to disable. */
	healthProbeIntervalMs?: number;
	logLevel: "info" | "verbose";
	logEnabled: boolean;
};

export const defaultConfig: FFDB_Config = {
	origin: "",
	apiUrl: "",
	endpoint: "/api/policies/query",
	authToken: "",
	apiKey: "",
	retryAttempts: 0,
	healthProbeIntervalMs: 1_000,
	logLevel: "info",
	logEnabled: false,
};
