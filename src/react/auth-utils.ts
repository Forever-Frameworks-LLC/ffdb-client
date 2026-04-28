// Better Auth client actions commonly resolve to a `{ data, error }` envelope
// instead of throwing on failure. Capturing that shape here lets React-facing
// helpers infer the useful payload type without repeating the envelope contract
// at each call site.
type AuthEnvelope<T> = {
	data?: T | null;
	error?: unknown | null;
};

// Extract the successful `data` payload type from an async auth function so
// hooks and helpers can expose the domain shape callers actually care about.
export type AuthDataOf<TFn extends (...args: any[]) => Promise<any>> =
	Awaited<ReturnType<TFn>> extends AuthEnvelope<infer TData> ? TData : never;

// Normalize Better Auth's envelope-style responses into the more familiar
// promise contract used by the rest of the React utilities: reject on error,
// otherwise return the data payload or `null` when the auth layer intentionally
// reports an empty result.
export async function unwrapAuthResult<T>(
	request: Promise<AuthEnvelope<T>>,
): Promise<T | null> {
	const result = await request;

	if (result.error) {
		throw result.error;
	}

	return result.data ?? null;
}
