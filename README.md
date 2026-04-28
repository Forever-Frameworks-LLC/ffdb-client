# ffdb-client

A framework-agnostic client for FFDB. It ships with built-in user authentication via better-auth, a type-safe query builder via Kysely, raw SQL helpers, and optional offline sync.

Works in Node.js, browsers, React Native, and Expo.

## How It Works

FFDB has a two-tier authentication model:

1. You, the developer, connect with an admin token from the FFDB portal. That token lets you make admin requests.
2. Your users sign in through the auth client methods. After sign-in, the client manages short-lived API keys for scoped access.

The client exposes a `db` object for type-safe queries, a raw `sql` helper, and an `auth` client for user authentication. It also exposes `request`, `sync`, `getAccess`, and `subscribe` for lower-level control.

## Install

```bash
npm install ffdb-client
```

## Quick Start

### The recommended way to start

- Follow the prompts and select a quick start template.
```bash
npx ffdb init
```

### Any JavaScript environment

```ts
import { createClient } from "ffdb-client";

const { db, auth, destroy } = await createClient({
  config: {
    apiUrl: "https://your-ffdb-server.com",
  },
});

await auth.signIn.email({
  email: "user@example.com",
  password: "password",
});

const users = await db.selectFrom("user").selectAll().execute();

await destroy();
```

### React / Next.js

```tsx
import { FFDBProvider, useFFDB, useRawQuery } from "ffdb-client/react";

function App() {
  return (
    <FFDBProvider
      options={{
        config: {
          apiUrl: import.meta.env.VITE_API_URL, // Vite
        },
      }}
    >
      <Users />
    </FFDBProvider>
  );
}

function Users() {
  const { db, auth, isLoading, error } = useFFDB();

  if (isLoading) return <p>Connecting...</p>;
  if (error) return <p>Connection failed: {error.message}</p>;

  const loadUsers = async () => {
    const rows = await db!.selectFrom("user").selectAll();
    console.log(rows);
  };

  return <button onClick={loadUsers}>Load users</button>;
}
```

`useQuery()` is the reactive Kysely hook. `useRawQuery()` is the raw SQL version, which is useful for searchable pickers and other query-driven UI.

```tsx
const { data, isLoading } = useQuery((db) => db.selectFrom("user").selectAll().limit(20));

// -- OR something like this --

const { data, isLoading } = useRawQuery<{ id: string; name: string }>(
  {
    sql: "SELECT id, name FROM user WHERE name LIKE ? ORDER BY name LIMIT 20",
    values: [`%${term}%`],
  },
  { deps: [term], enabled: term.length > 1 },
);
```

### Node.js

```ts
import { createClient, createNodeLifecycle } from "ffdb-client/node";

const { db, auth } = await createClient({
  config: {
    apiUrl: process.env.API_URL,
  },
  lifecycle: createNodeLifecycle(),
});
```

## API Surface

### `createClient(options)`

Creates a new FFDB client.

```ts
type CreateClientOptions = {
  config?: Partial<FFDB_Config>;
  lifecycle?: LifecycleHooks;
  storage?: StorageAdapter; // Very useful for expo (secure store)
  offline?: OfflineConfig;
  skipHealthCheck?: boolean;
};
```

Returns:

```ts
{
  db,
  sql,
  auth,
  request,
  destroy,
  sync,
  getAccess,
  subscribe,
}
```

`sync` includes `run()`, `sync()`, `pull()`, `push()`, `waitForIdle()`, `status`, and `subscribe()`.

### `createFetchClient(options)`

Creates the underlying HTTP client if you need direct API access.

### React hooks

`useFFDB()` returns the current client plus readiness state:

```ts
{
  client,
  db,
  sql,
  auth,
  destroy,
  sync,
  getAccess,
  subscribe,
  isLoading,
  error,
  clientVersion,
  isReady,
}
```

`useDB()` returns the `Kysely` instance directly. `useAuth()` returns the auth client directly. `useFFDBStatus()` returns `{ isLoading, error }`.

`useQuery()` accepts a Kysely query factory and supports `enabled`, `deps`, `refetchOnFocus`, `refetchOnReconnect`, and `refetchInterval`.

`useRawQuery()` accepts `{ sql, values, bypassCache }` and the same hook options.

### Other utilities

`generateId()` creates a stable text ID for app data and is the recommended default for offline-safe inserts.

`memoryStorage()` is the default built in storage adapter. `browserStorage()` supports `"local"`, `"session"`, and `"indexeddb"` options.

## Configuration

`createClient()` accepts a partial config and merges it with defaults.

```ts
type FFDB_Config = {
  origin: string; // Your window.location.origin or scheme://
  apiUrl: string; // Your FFDB instance url
  endpoint: string; // The endpoint for querying, NOT RECOMMENDED TO CHANGE
  authToken: string; // DO NOT USE IN CLIENT CODE
  apiKey: string; // DO NOT USE IN CLIENT CODE - also not necessary if you have authToken
  retryAttempts?: number; // Number of times to retry when encountering 400+ errors
  healthProbeIntervalMs?: number; // How often to poll when offline
  logLevel: "info" | "verbose";
  logEnabled: boolean;
};
```

In practice you usually only need to provide `apiUrl` and `origin` if your code is not running in a browser.

## Session Persistence

If you need persisted sessions outside of memory / cookie storage, pass a storage adapter. Without one, the client uses `memoryStorage()`, which is process-local and resets on reload.

```ts
import { createClient, browserStorage } from "ffdb-client";

const { db, auth } = await createClient({
  config: { apiUrl: "...", origin: "..." },
  storage: browserStorage(),
});
```

`browserStorage("local")` survives reloads, `browserStorage("session")` is tab-scoped, and `browserStorage("indexeddb")` is the fully async browser option.

## Querying

The `db` object is a Kysely instance. All queries are type-safe against your database schema.

```ts
await db.selectFrom("user").select(["id", "email", "name"]);
await db.insertInto("user").values({ id: "abc", name: "Alice", email: "alice@example.com" });
await db.updateTable("user").set({ name: "Bob" }).where("id", "=", "abc");
await db.deleteFrom("user").where("id", "=", "abc");
```

If you prefer SQL strings, use `client.sql.query()`, `client.sql.first()`, or `client.sql`.

## Authentication

The `auth` object is a better-auth client with the FFDB API key plugin pre-configured. After sign-in, the client automatically rotates API keys and keeps the session refreshed.

## Access & Permissions

`getAccess()` fetches the authenticated user's permissions, including table access, column constraints, and blocked statement types.

## Offline-First

If you enable `offline`, the client can keep a local SQLite cache, queue mutations, and sync with the server using `POST /api/sync/pull`.

## Lifecycle & Cleanup

Always call `destroy()` when you are done with the client. For Node.js services, `createNodeLifecycle()` registers cleanup handlers for `SIGINT`, `SIGTERM`, and `beforeExit`.

## Entry Points

| Import Path | Environment | Includes |
|---|---|---|
| `ffdb-client` | Universal | `createClient`, `createFetchClient`, `generateId`, storage helpers, types |
| `ffdb-client/react` | React | `FFDBProvider`, `useQuery`, `useRawQuery`, `useDB`, `useAuth`, `useFFDB`, `useFFDBStatus` |
| `ffdb-client/node` | Node.js | Universal + `config`, `loadEnvConfig`, `createNodeLifecycle` |

## License

ISC
