# Instance lifecycle and query sources

[README](../README.md) · [API reference](reference.md) · [Migration from 1.x](migration-v2.md)

Version 2 has one authoritative configuration interface: `querySource`. It returns the complete intended catalog, synchronously or asynchronously. The engine coordinates readers, processing, checkpoints, and replacement; your application owns configuration storage, authorization, persistence, and transports.

## Lifecycle operations

| Operation | Behavior |
| --- | --- |
| `create(options)` | Construct the engine, or return the active guarded singleton; do not invoke the loader or start queries. Register handlers and hooks next. |
| `start(options?)` | Invoke the loader with `reason: "start"`, validate the complete result, and start its queries. Calling it again after successful startup is a no-op. |
| `pauseAll(options?)` | Hold the selected side across all queries and wait for its active work and ordered observers to settle. Accepts `{ scope?: "read" \| "process" \| "both", timeoutMs?: number }`; scope defaults to `"both"`. Queued work is retained. |
| `resumeAll(options?)` | Release the selected global hold(s), with the same options and default scope. Existing manual, failure, predicate, and capacity pauses retain their ownership. |
| `restartQueries(options?)` | Rebuild the last accepted catalog without calling the loader, using final processed checkpoints. |
| `reloadQueries(options?)` | Invoke the loader with `reason: "reload"`, validate its complete result before disruption, then replace the runtime catalog. |
| `shutdown(options?)` | Stop the engine, await required checkpoint persistence, release runtime resources, and release the singleton after completion. |
| `getLifecycleState()` | Inspect `started`, `closed`, `generation`, `globalReadPaused`, `globalProcessPaused`, `globalPaused`, `maintenanceHeld`, `recoveryRequired`, optional `operation: { name, startedAt, timedOut, scope? }`, and `lastError`. `startedAt` is epoch milliseconds; `scope` identifies an active global pause/resume operation. |

These operations preserve the engine object, handler registry, and hook registrations until shutdown. They do not restart the Node.js process or recreate application HTTP/WebSocket servers. Successful restart/reload restores the read and processing global holds independently; newly loaded queries inherit them too. Per-query pause/resume controls remain available when an instance operation is not active.

The default singleton is process-local. Later guarded `create()` calls return the existing instance without applying their options. During shutdown, they wait for the underlying work to settle, even if the shutdown caller has already timed out. Completed disposal permits one replacement instance; a shutdown that fails before disposal leaves the existing held instance available for recovery. `create()` does not repair or restart it automatically. Use `singletonGuard: false` only for deliberately independent engines with separately managed ownership.

## Read and processing holds

`GlobalPauseScope` and `GlobalPauseOptions` are public TypeScript exports. The two administrative holds are independent, and only the selected side changes:

```ts
await sync.pauseAll({ scope: "read", timeoutMs: 5_000 });
// Ingestion is held; existing queued work can continue through handlers.
await sync.resumeAll({ scope: "read" });

await sync.pauseAll({ scope: "process" });
// Readers can fill the queue until its capacity/backpressure limits apply.
await sync.resumeAll({ scope: "process" });

await sync.pauseAll(); // Equivalent to { scope: "both" }.
await sync.resumeAll({ scope: "read" }); // Processing remains held.
await sync.resumeAll(); // Release both administrative holds.
```

A read-only pause cancels and settles active reads without waiting for handlers to finish draining queued work. A processing-only pause prevents another handler from starting while the active handler and its ordered processing observers settle; reads remain eligible. A both-scope pause installs both gates before waiting for either side. Pausing cannot forcibly terminate arbitrary user callbacks; use bounded external I/O and observe cancellation signals where supplied.

Global holds preserve each query's underlying read/process status and pause ownership. Public inspection through `getReadStatus`, `getQueueStatus`, and query snapshots reports an otherwise `RUNNING` side as `PAUSING` while active work settles or `PAUSED` once settled. Hooks and underlying query state retain the independent status. Dashboards should display the public per-query status alongside these lifecycle fields to distinguish global holds from independent pauses:

| Field | Meaning |
| --- | --- |
| `globalReadPaused` | Reading is globally held by the administrative read hold **or** maintenance. |
| `globalProcessPaused` | Processing is globally held by the administrative processing hold **or** maintenance. |
| `globalPaused` | Both effective global holds are active: `globalReadPaused && globalProcessPaused`. It does not mean that either side is held. |
| `maintenanceHeld` | The lifecycle coordinator is holding both sides for maintenance, such as replacement or shutdown. |

Do not persist global holds as manual per-query pauses. `resumeAll` changes administrative holds only; it does not clear independent manual/failure pauses, reset failure counters, or override predicate/capacity restrictions. A lifecycle operation in progress still prevents conflicting controls.

Paginated `listQueries()` can filter these effective states independently with `readStatus` and `processStatus`, alongside configured-title/identifier `search`. The filters inspect the loaded catalog without invoking the loader or changing holds. See [query-list filtering](reference.md#query-list-filtering).

## Automatic failure recovery

Automatic recovery is disabled by default. Opt in at construction:

```ts
const sync = await HederaEventSync.create({
  restClient,
  querySource,
  failureRecovery: { enabled: true, delaySeconds: 3600 },
});
```

`FailureRecoveryOptions` is a public export. `enabled` is a boolean; omitting the entire option or setting it to `false` keeps failure pauses under operator control. `delaySeconds` defaults to 3600 and must be a whole number from 1 to 2,147,483,647. Configuration is per instance; read and processing stages use independent deadlines. There is no environment, database, Redis, or notification dependency.

- Only a pause raised internally by reaching `pauseAfterConsecutiveFailures` qualifies. Initial pauses, manual pauses, fulfilled predicates, backpressure, hook-policy pauses, and terminal `FAILED`/`STOPPED` states are excluded. Existing capacity-based resumption is unchanged.
- `query.read.status.failureRecovery?.retryAt` and `query.process.status.failureRecovery?.retryAt` expose the earliest retry time in epoch milliseconds. The field is also available in stage status inspection and status-change hooks. It disappears on resumption or an intentional takeover of the pause. Do not infer eligibility from the human-readable `info` string.
- A single unreferenced instance timer checks due work roughly once per second. A retry waits for the stage to finish pausing, then uses the ordinary resume path and resets that stage's consecutive-failure budget. It does not clear queued records, change cursors, skip the failing item, or reload configuration. Continued failure reaches the limit again and schedules a new cooldown.
- Global stage holds, maintenance, and active instance operations defer recovery without discarding the deadline. Per-query controls take precedence under the same lifecycle gate. A due retry becomes eligible after the applicable hold or operation ends; the displayed deadline is not a promise of exact execution time.
- Applications can call `const release = sync.holdFailureRecovery()` before their own catalog writes or management work, then call `release()` in `finally`. This synchronous hold prevents admission of automatic retries across the storage-before-reload interval; it does not pause ordinary work or block explicit controls. Holds compose, release is idempotent, and deadlines remain intact. A forgotten release intentionally keeps automatic recovery held.
- Calling `pauseRead(id)` or `pauseProcessing(id)` on a failure-paused stage converts it into an intentional pause and cancels recovery. `{ preserveExistingPause: true }` explicitly retains the previous ownership and deadline. Ordinary per-stage resume is available before the cooldown expires.
- Restart and reload carry the original deadlines and pause ownership for retained logical query IDs. Removed queries are retired; reintroducing an ID later does not inherit its old schedule. Shutdown clears the timer and stops admitting recovery work. Replacement failures keep their existing global/maintenance holds.

Automatic resumption is an attempt, not proof of recovery. Applications should resolve failure incidents on successful reads or processing, and retain their own notification deduplication/rate limits. An unbounded callback cannot be forcibly recovered; it must settle before its stage can retry.

Deadlines are in-memory state. A new Node.js process loads the application's catalog and durable checkpoints, following `initialState` as usual; it does not restore a previous process's cooldown. Applications needing durable pause policy must supply that policy in their source. Keep the process clock synchronized because deadlines use wall-clock epoch time.

## Static catalog

```ts
import { HederaEventSync, HederaRestClient, type QueryConfig } from "@davincigraph/hedera-event-sync";

let catalog: QueryConfig[] = [{
  id: 1, type: "Transactions", title: "Account activity",
  params: { accountId: "0.0.98", timestamp: "1700000000.000000000", index: 0 },
  read: {
    fetch: { network: "testnet", restProvider: "public" },
    pauseAfterConsecutiveFailures: 5,
  },
  process: { pauseAfterConsecutiveFailures: 5, unhandledTransactionPolicy: "skip" },
}];

const sync = await HederaEventSync.create({
  restClient: new HederaRestClient({ defaultProvider: "public", defaultNetwork: "testnet" }),
  querySource: () => catalog,
});
sync.registry.registerTransaction("CRYPTOTRANSFER", {
  handle(transaction) { console.log(transaction.transaction_id); },
});
await sync.start();

catalog = catalog.map(query => ({ ...query, title: "Renamed activity" }));
await sync.reloadQueries();
await sync.shutdown();
```

The loader is called only by startup and explicit reload, never on each poll. Changing the array alone does not change running work. Every array position must contain a valid query; sparse arrays and explicit `undefined` entries reject the complete catalog before existing work is held. Return `[]` only when intentionally removing all queries. Accepted plain configuration data is detached from the caller; callbacks remain function references. Distinct IDs stay distinct even for equivalent configurations; unlike 1.x, there is no configuration deduplication that silently skips an entry.

## Database source

The package has no database dependency. Return configurations through your existing adapter, including durable processed cursors for a fresh process startup:

```ts
import { HederaEventSync, type HederaRestClient, type QueryConfig, type BeforeReplaceContext } from "@davincigraph/hedera-event-sync";

interface CatalogStore {
  loadAssignedQueries(workerId: string, signal: AbortSignal): Promise<readonly QueryConfig[]>;
  saveCheckpoints(entries: BeforeReplaceContext["checkpoints"], signal: AbortSignal): Promise<void>;
}

export async function createWorker(restClient: HederaRestClient, store: CatalogStore, workerId: string) {
  return HederaEventSync.create({
    restClient,
    querySource: ({ signal }) => store.loadAssignedQueries(workerId, signal),
    beforeReplace: ({ checkpoints, signal }) => store.saveCheckpoints(checkpoints, signal),
  });
}
```

`loadAssignedQueries` must return all enabled queries intended for this worker. Let database errors reject. Converting an unavailable database into `[]` would deliberately remove every current query. Read the catalog consistently, serialize management writes/reloads in your application, and enforce assignment/authorization before asking the package to replace work. The [PostgreSQL integration](integrations.md) contains concrete persistence SQL.

## File source

```ts
import { readFile } from "node:fs/promises";
import { HederaEventSync, type HederaRestClient, type QueryConfig } from "@davincigraph/hedera-event-sync";

export function createFileWorker(restClient: HederaRestClient, filename: string) {
  return HederaEventSync.create({
    restClient,
    async querySource({ signal }) {
      const value: unknown = JSON.parse(await readFile(filename, { encoding: "utf8", signal }));
      if (!Array.isArray(value)) throw new Error("Query file must contain a complete array");
      // The engine validates every configuration before changing running work.
      return value as QueryConfig[];
    },
  });
}
```

A missing/malformed file rejects startup or reload. JSON cannot encode callbacks such as `logFilter`, `contractResolver`, or `until`; attach trusted application functions in the loader when needed. The package does not watch files automatically. If your application watches them, write a complete file atomically and debounce changes before requesting reload. Keep checkpoint persistence separate or safely merge it with the configuration; never replace a configuration file with only its cursors.

## Identity, new queries, and removal

Query IDs identify a stable logical source, not a row position. Never generate them from array indexes. Reusing an accepted ID with a different query kind, network, source address/account, topics, transaction result filter, or transaction-type filter rejects the candidate before disruption. Use a new ID and an intentional starting checkpoint for a different logical source. A provider change on the same ledger is allowed; ensure both providers actually represent that ledger.

The package cannot determine the meaning of changed application functions or business handlers. Changes to admission predicates, ABI meaning, or business semantics may require a new identity and deliberate backfill even if configuration validation accepts them. Coordinate those changes in the application.

For an existing logical ID, replacement uses the final processed checkpoint from its current runtime. A stale or advanced cursor returned by the loader cannot override it. To perform an intentional backfill, introduce a new logical ID with the desired checkpoint and remove the prior ID in the same catalog replacement. Do not run both against non-idempotent effects.

New IDs use their supplied `params.timestamp`/`params.index` and optional `initialState`:

```ts
const initialState = {
  read: { name: "PAUSED", info: "Awaiting operator review" },
  process: { name: "PAUSED", info: "Awaiting operator review" },
} as const;
```

Omitted sides start independently `RUNNING`, subject to the instance's global holds. Existing IDs retain their independent pause ownership and failure counters; terminal failed/stopped sides become independent pauses that need an explicit per-side resume after repair. Changing `initialState` in a loader is not a way to override live controls. An omitted ID is removed after old work settles and persistence succeeds. Returning `[]` is intentional removal of the entire catalog. Configuration reload replaces the whole generation, including unchanged queries, and clears per-runtime in-memory history.

## Persistence before replacement

`beforeReplace({ reason, checkpoints, signal })` is awaited for restart, reload, and shutdown. It receives final safe processed checkpoints paired with the old query configurations, including removed queries. It runs after work is held and before old queues are disposed. It is **not an observational hook**: a rejection is propagated regardless of `hookFailurePolicy`.

Applications that require durable replacement must supply this callback and confirm writes before it resolves. Use a transaction when all checkpoints/configuration metadata must commit together, and throw when the outcome cannot be confirmed. Be replay-safe: a timeout can occur after an external database committed, and a later retry may repeat the callback. Give database operations their own deadlines and honor the supplied signal where supported. Swallowing persistence failures opts out of the guarantee.

Without `beforeReplace`, in-process restart still carries safe checkpoints but the package makes no durability promise across process termination. Keep ordinary `onProcessedCheckpoint` storage for ongoing progress; this final boundary does not replace it. Neither boundary is an atomic transaction with arbitrary handler side effects. Idempotent handlers remain required for replay after crashes, overlap, and replacement.

## Failure and timeout recovery

`start`, `pauseAll`, `resumeAll`, `restartQueries`, `reloadQueries`, and `shutdown` accept optional `{ timeoutMs: positiveInteger }` (1 through 2,147,483,647 milliseconds). Omit it to await completion without a package deadline. A timeout rejects with `LifecycleTimeoutError`; it does **not** prove that callbacks have stopped or that external writes did not commit. `create()` and per-query controls do not accept this deadline option.

| Failure point | Result and next action |
| --- | --- |
| Loader, duplicate ID, invalid configuration, changed source identity | Existing generation remains usable and retains its prior independent global holds. Fix the source and retry reload. Failed initial startup may be retried with `start()`. |
| Settling work or `beforeReplace` | Both global holds remain set for recovery; the old catalog and queued records are retained. Resolve the cause and retry the operation, or deliberately resume a selected side after the failed operation has settled. |
| Replacement after old disposal or partial initialization | Both global holds remain set with `recoveryRequired: true`; every `resumeAll` scope rejects until a successful restart/reload completes. Inspect `lastError`, query statuses, and observer failures; retry restart after correcting the cause. There is no claim of transactional rollback after arbitrary application callbacks. |
| Lifecycle gate still occupied after timeout | Wait for the callback to settle and inspect `operation.timedOut`. Other lifecycle changes reject with `LifecycleBusyError`; do not start another copy alongside the blocked one. |

After a replacement failure, a successful restart/reload clears the recovery guard but retains the global holds installed by the failure. Review the resulting generation, then explicitly call `resumeAll()` or resume the intended global scope. Independent per-query pauses still require their own resume actions.

Shutdown, like replacement, waits for active work before discarding queues; a handler that waits indefinitely for cancellation can therefore block graceful completion. Give its external work bounded I/O rather than requiring shutdown to abort it. No JavaScript API can safely force-terminate an arbitrary handler. A permanently hung callback may require operator intervention at the process/deployment layer after confirming the old process is no longer executing. The package deliberately does not overlap another runtime generation with it.

Do not call lifecycle methods from active callbacks belonging to that same engine, including the loader and persistence callback: they reject with `LifecycleReentrancyError` to prevent self-deadlock. Schedule changes after the callback has returned, handle the promise, and serialize them in your application. The package coordinates one instance only; distributed ownership/leases remain the application's responsibility.
