# Migrate from 1.x to 2.0

[README](../README.md) · [Lifecycle guide](lifecycle.md) · [Changelog](../CHANGELOG.md)

Version 2 is a breaking release. Every engine has a required complete-catalog loader and explicit startup. Query runtime replacement is coordinated by the package instead of an application assembling independent add/update/remove calls.

This guide covers upgrading from published 1.0.0 and from the unpublished 1.1.0 preparation. The 1.1.0-only editing helpers are identified below; applications using 1.0.0 do not need to adopt them as an intermediate step.

## Replace imperative configuration

Before:

```ts
const sync = await HederaEventSync.create({ restClient });
sync.registry.registerLog("Transfer", transferHandler);
sync.setHooks(hooks);
await sync.addQuery(query);
```

After:

```ts
let queries = [query];
const sync = await HederaEventSync.create({
  restClient,
  querySource: () => queries,
});
sync.registry.registerLog("Transfer", transferHandler);
sync.setHooks(hooks);
await sync.start();
```

`create()` does not call the source or start work. The default process-local singleton still applies: a second guarded `create()` returns the existing instance and does not replace its loader/options. Configure one owner at startup, or explicitly use `singletonGuard: false` for independent engines.

| 1.x pattern | 2.0 replacement |
| --- | --- |
| `addQuery(query)` starts immediately | Include the query in `querySource`; call `start()` initially or `reloadQueries()` afterwards |
| `updateQuery(fullConfig)` | Update the application-owned catalog and call `reloadQueries()` |
| `removeQuery(id)` | Omit the ID from the complete loader result and call `reloadQueries()` |
| `removeQuery(id, { returnCheckpoint: true })` (unpublished 1.1.0) | Persist old final checkpoints in the awaited `beforeReplace` callback |
| Initial state passed to add/update (unpublished 1.1.0) | Put `initialState` on each new `QueryConfig` |
| Loop over queries to pause/resume | Use `pauseAll({ scope })` / `resumeAll({ scope })` with `"read"`, `"process"`, or `"both"` (default) to preserve independent pause ownership |
| Recreate the engine to refresh database queries | Use `reloadQueries()`; handlers, hooks, and application transports stay registered |
| Rebuild from previously accepted configuration | Use `restartQueries()` without calling the source |
| `skipOnDuplication` and equivalent-config deduplication | Removed. Every distinct ID in the authoritative catalog is retained; duplicate IDs reject the whole candidate |

The removed mutation methods are absent from the public runtime and declarations. Migrate application wrappers, HTTP routes, scripts, mocks, and tests together. Route authorization and database writes stay in your application; do not expose an unrestricted arbitrary-catalog replacement endpoint.

## Preserve safe checkpoints and ownership

During in-process restart/reload, existing logical IDs inherit their final **processed** checkpoints. Loader cursors cannot rewind them or skip unprocessed events. Source identity changes require a new ID. Restart may replay records and resets in-memory history; keep handler effects idempotent and keep durable operational history outside the engine.

Transaction queries retain their existing type selection when `params.transactionType` is omitted. Opting into one category, changing it, or removing it later changes source identity, just like changing `params.result`: use a new query ID and deliberately select its initial checkpoint. When broadening a filter, choose a checkpoint that includes the previously excluded history your application needs. See [transaction source filters](queries.md#transactions) for handler-policy and balance-accounting implications.

Global read and processing holds are independent. The new `pauseAll()` / `resumeAll()` methods affect both sides by default; pass `{ scope: "read" }` or `{ scope: "process" }` to change one side. After `pauseAll()`, `resumeAll({ scope: "read" })` leaves processing held. Read pause lets queued handlers drain; processing pause lets readers continue until queue capacity applies backpressure. A pause waits for active work on its selected side without forcibly terminating a handler.

`resumeAll` does not resume a manually paused query, erase a failure pause, or reset its failure budget. Successful restart/reload preserves each administrative hold, and new queries inherit them. Existing per-query controls remain for deliberate operator actions. Display `globalReadPaused` and `globalProcessPaused` separately from individual query statuses. `globalPaused` means both effective holds are active, so checking it alone misses a single-side pause. `maintenanceHeld` identifies the lifecycle coordinator's both-side hold; the two effective fields include this hold. TypeScript consumers can import `GlobalPauseScope` and `GlobalPauseOptions`.

For a storage-backed consumer, add this construction option using your own persistence adapter:

```ts
beforeReplace: async ({ checkpoints, signal }) => {
  signal.throwIfAborted();
  await saveFinalProcessedCheckpoints(checkpoints, signal);
  signal.throwIfAborted();
}
```

This callback must reject if persistence fails or cannot be confirmed. It is awaited before old queues are discarded and is independent of `hookFailurePolicy`. Continue saving ordinary `onProcessedCheckpoint` updates for crash recovery. The package does not supply a database transaction, a distributed worker lock, an email service, or an HTTP endpoint.

## Loader and operation errors

- A source error must reject; do not catch it and return `[]`. An empty array deliberately removes all queries.
- Use stable IDs across reloads and return a consistent complete source snapshot.
- A rejected loader or invalid catalog leaves the accepted generation and independent global holds unchanged. Failures after entering replacement hold both sides for recovery. A failure before old disposal can be resumed deliberately after the operation settles; partial replacement blocks every resume scope until a successful restart/reload clears `recoveryRequired`.
- Lifecycle methods can take `{ timeoutMs }`. Timeout does not force-stop user callbacks. The gate remains occupied until they settle, preventing overlapping generations.
- A guarded `create()` during shutdown waits for actual shutdown completion, including after a caller-visible timeout. It returns one replacement after disposal, or the existing held instance if shutdown fails before disposal. Inspect lifecycle state before assuming a returned instance is newly created.
- Lifecycle controls from the engine's own active callbacks reject; run follow-up management work outside those callbacks.
- Source and persistence callbacks should use bounded external I/O and honor `signal`.
- Shutdown now drains active work before checkpoint persistence/disposal. A handler that waits indefinitely for shutdown cancellation can block it; migrate such handlers to bounded operations. A timeout reports the blockage without overlapping another generation.

See [the lifecycle guide](lifecycle.md) for complete static, database, and file examples and the exact recovery behavior. Test the migration with active processing, a paused failure, a loader rejection, a persistence rejection, and an empty catalog before deploying it.
