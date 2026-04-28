import type { FetchClient } from "./fetch.ts";

/** Per-table access summary returned by `/api/policies/access`. */
export type TableAccess = {
	table: string;
	access: {
		read: boolean;
		insert: boolean;
		update: boolean;
		delete: boolean;
	};
	constraints: {
		allowedReadColumns: string[] | undefined;
		deniedReadColumns: string[] | undefined;
		writableColumns: string[] | undefined;
		requiredPredicateColumns: string[] | undefined;
		maxLimit: number | undefined;
		maxOffset: number | undefined;
		allowOffset: boolean | undefined;
	};
	/** Whether a policy row was explicitly configured for this table. */
	explicitlyConfigured: boolean;
	/** Whether the user is an admin and the policy is overridden. */
	adminOverride: boolean;
};

/** Full response from `/api/policies/access`. */
export type AccessInfo = {
	userId: string;
	role: string;
	/** Statement types the user is not allowed to execute. */
	blockedStatementTypes: string[];
	tables: TableAccess[];
};

/**
 * Fetch the authenticated user's access permissions.
 *
 * Returns which tables they can read/write, column constraints,
 * limits, and whether they are an admin.
 */
export async function fetchAccess($fetch: FetchClient): Promise<AccessInfo> {
	const { data, error } = (await $fetch("/api/policies/access")) as {
		data: { data: AccessInfo } | null;
		error: { statusText?: string; message?: string } | null;
	};

	if (error || !data?.data) {
		throw new Error(
			`Failed to fetch access info: ${error?.message ?? error?.statusText ?? "Unknown error"}`,
		);
	}

	return data.data;
}
