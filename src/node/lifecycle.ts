import type { LifecycleHooks } from "../client.ts";

/**
 * Create lifecycle hooks for long-lived Node processes.
 *
 * Browser callers do not need this, but CLIs, replicas, and background workers
 * often keep timers, sockets, or local SQLite handles open. Wiring shutdown
 * through the client lifecycle gives those runtimes one place to release
 * resources before the process exits.
 */
export function createNodeLifecycle(): LifecycleHooks {
	let exitHandler: (() => void) | null = null;

	return {
		onCreated(destroy) {
			// Reuse the same handler for signals and normal process wind-down so the
			// client's destroy path stays consistent regardless of how shutdown begins.
			exitHandler = () => {
				destroy().catch(() => {
					// Shutdown should remain best-effort here: once Node is already exiting,
					// there is usually no useful recovery path for teardown failures.
					/* ignore teardown errors */
				});
			};
			// `SIGINT` covers Ctrl+C, `SIGTERM` covers orchestrated shutdown, and
			// `beforeExit` catches the quieter case where the event loop is draining but
			// we still want the client to release its own resources.
			process.once("SIGINT", exitHandler);
			process.once("SIGTERM", exitHandler);
			process.once("beforeExit", exitHandler);
		},
		onDestroyed() {
			if (exitHandler) {
				// Remove handlers after explicit destroy so repeated client creation does
				// not accumulate duplicate process listeners in the same Node runtime.
				process.off("SIGINT", exitHandler);
				process.off("SIGTERM", exitHandler);
				process.off("beforeExit", exitHandler);
				exitHandler = null;
			}
		},
	};
}
