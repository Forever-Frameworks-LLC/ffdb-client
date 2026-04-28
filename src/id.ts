const FALLBACK_ALPHABET =
	"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

function randomString(size: number) {
	const cryptoApi = globalThis.crypto;

	if (cryptoApi?.getRandomValues) {
		const bytes = new Uint8Array(size);
		cryptoApi.getRandomValues(bytes);
		return Array.from(
			bytes,
			(byte) => FALLBACK_ALPHABET[byte % FALLBACK_ALPHABET.length],
		).join("");
	}

	let value = "";
	for (let index = 0; index < size; index += 1) {
		value +=
			FALLBACK_ALPHABET[Math.floor(Math.random() * FALLBACK_ALPHABET.length)];
	}
	return value;
}

/**
 * Generate a stable text ID suitable for offline-first inserts.
 *
 * This intentionally uses built-in platform primitives with a safe fallback
 * so it works across Node.js, browsers, Expo, and React Native without
 * requiring extra crypto polyfills.
 *
 * Create the ID once on the client and send it to the server unchanged.
 * This avoids key swaps and unnecessary re-renders in reactive UIs.
 */
export function generateId(prefix = "") {
	const cryptoApi = globalThis.crypto;
	if (typeof cryptoApi?.randomUUID === "function") {
		return `${prefix}${cryptoApi.randomUUID()}`;
	}
	return `${prefix}${randomString(21)}`;
}
