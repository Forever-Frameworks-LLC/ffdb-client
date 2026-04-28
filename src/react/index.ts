export type { CreateClientOptions, LifecycleHooks } from "../client.ts";
// Re-export core types consumers commonly need
export type { FFDB_Config } from "../config.ts";
export type { Database } from "../types.ts";
export { type AuthDataOf, unwrapAuthResult } from "./auth-utils.ts";
export {
	FFDBProvider,
	type FFDBProviderProps,
	type FFDBReactAuthClient,
	getFFDBAuth,
	getFFDBClient,
	useAuth,
	useDB,
	useFFDB,
	useFFDBClient,
	useFFDBStatus,
} from "./context.tsx";
export {
	type UseQueryOptions,
	type UseQueryResult,
	type UseRawQueryInput,
	useQuery,
	useRawQuery,
} from "./use-query.ts";
