/**
 * Small persistence abstraction used by auth/session helpers.
 *
 * The client uses this for things like session snapshots, bearer tokens, and
 * short-lived API keys. Keeping it abstract lets the same package run in the
 * browser, desktop shells, Node services, and mobile runtimes without hard-
 * wiring one storage API into the core client.
 *
 * Implement this for your environment:
 * - Browser: use `browserStorage()` (localStorage/sessionStorage)
 * - React Native: wrap AsyncStorage or expo-secure-store
 * - Node.js: wrap fs, keytar, or any key-value store
 *
 * All methods may be sync or async.
 */
export type StorageAdapter = {
	get(key: string): Promise<string | null> | string | null;
	set(key: string, value: string): Promise<void> | void;
	remove(key: string): Promise<void> | void;
};

/**
 * Process-local fallback storage.
 *
 * This keeps the client usable even when the caller does not provide any real
 * persistence, but auth and cache state disappears on reload or process exit.
 */
export function memoryStorage(): StorageAdapter {
	const store = new Map<string, string>();
	return {
		get: (key) => store.get(key) ?? null,
		set: (key, value) => {
			store.set(key, value);
		},
		remove: (key) => {
			store.delete(key);
		},
	};
}

type StorageType = "local" | "session" | "indexeddb";

/**
 * Browser storage adapter with interchangeable backends.
 *
 * Different apps want different durability tradeoffs: `localStorage` survives
 * tab reloads, `sessionStorage` is intentionally tab-scoped, and IndexedDB is
 * the most flexible option when larger or more async-friendly storage semantics
 * are needed.
 *
 * Supports:
 * - localStorage = "local"
 * - sessionStorage = "session"
 * - IndexedDB = "indexeddb"
 */
export function browserStorage(type: StorageType = "local"): StorageAdapter {
	// IndexedDB is offered as the "fully async" browser path so environments that
	// avoid synchronous storage APIs can still satisfy the same adapter contract.
	if (type === "indexeddb") {
		const DB_NAME = "ffdb-storage";
		const STORE_NAME = "keyval";
		const DB_VERSION = 1;

		const openDB = (): Promise<IDBDatabase> =>
			new Promise((resolve, reject) => {
				const request = indexedDB.open(DB_NAME, DB_VERSION);

				request.onupgradeneeded = () => {
					const db = request.result;
					if (!db.objectStoreNames.contains(STORE_NAME)) {
						db.createObjectStore(STORE_NAME);
					}
				};

				request.onsuccess = () => resolve(request.result);
				request.onerror = () => reject(request.error);
			});

		const withStore = async <T>(
			mode: IDBTransactionMode,
			callback: (store: IDBObjectStore) => IDBRequest<T>,
		): Promise<T> => {
			// Open-per-operation keeps this helper small and predictable. FFDB only uses
			// storage for tiny auth/cache records, so the extra connection churn is a
			// reasonable tradeoff for avoiding a long-lived IndexedDB wrapper here.
			const db = await openDB();
			return new Promise((resolve, reject) => {
				const tx = db.transaction(STORE_NAME, mode);
				const store = tx.objectStore(STORE_NAME);
				const request = callback(store);

				request.onsuccess = () => resolve(request.result);
				request.onerror = () => reject(request.error);
			});
		};

		return {
			get: (key) => withStore("readonly", (s) => s.get(key)),
			set: (key, value) =>
				withStore("readwrite", (s) => s.put(value, key)).then(() => {}),
			remove: (key) =>
				withStore("readwrite", (s) => s.delete(key)).then(() => {}),
		};
	}

	// The synchronous Web Storage APIs are wrapped in async-compatible functions so
	// callers can treat every storage backend uniformly.
	const store = type === "local" ? localStorage : sessionStorage;

	return {
		get: async (key) => store.getItem(key),
		set: async (key, value) => {
			store.setItem(key, value);
		},
		remove: async (key) => {
			store.removeItem(key);
		},
	};
}
