export type QueryCacheRecord = {
	data: unknown[];
	updatedAt: number;
};

// This is the lightweight in-memory cache used by the React query hooks. It is
// distinct from the offline driver's SQLite-backed query cache: this one only
// exists for the current JS runtime and is meant to smooth repeated renders, not
// survive reloads or offline restarts.
export const queryCache = new Map<string, QueryCacheRecord>();

// Matching queries that start in the same render window share one promise so the
// UI does not stampede the database/driver with identical work before the first
// request resolves.
export const inflightQueries = new Map<string, Promise<unknown[]>>();

export function clearQueryCache(): void {
	// Clearing both maps keeps hook-level state aligned after major auth or sync
	// transitions. Dropping only settled data while leaving in-flight promises
	// behind would let old work repopulate the cache immediately.
	queryCache.clear();
	inflightQueries.clear();
}
