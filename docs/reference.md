# API reference and guarantees

[Back to the README](../README.md) · [Queries and handlers](queries.md) · [Application integrations](integrations.md)

Use this page to look up options and exact behavior after choosing your application's queries, handlers, and observers. Code snippets using `sync` assume an existing synchronizer and run outside its active callbacks.

- [Configuration](#configuration-reference)
- [Runtime management and recovery](#runtime-management)
- [Handler types and registration](#handler-types-and-registration)
- [Processes, attempts, and steps](#processes-attempts-and-steps)
- [Hooks](#hook-reference)
- [Read cursors and processing](#how-synchronization-works)
- [Delivery guarantees](#delivery-guarantees)
- [Failures and automatic pauses](#failures-and-automatic-pauses)
- [Hook guarantees and failure policy](#hook-guarantees-and-failure-policy)
- [Singleton lifecycle](#singleton-lifecycle)
- [Lifecycle safety](#lifecycle-safety)
- [Advanced APIs](#advanced-public-apis)
- [Package exports](#package-exports)
- [Troubleshooting](#troubleshooting)

## Configuration reference

### Engine options

Pass these to `HederaEventSync.create(options)`:

| Path | Required/default | Purpose |
| --- | --- | --- |
| `restClient` | Required | Configured `HederaRestClient` used by queries |
| `singletonGuard` | `true` | Reuse the process-local instance; `false` deliberately creates an isolated engine |
| `hookFailurePolicy` | `"continue"` | `"continue"`, `"pause"`, or `"fail-fast"` |
| `defaults` | Optional | Fallback values listed below |

Set defaults **inside** `defaults`. Query-level values take precedence.

| Engine path | Query override | Default / unit |
| --- | --- | --- |
| `defaults.pollIntervalSeconds` | `read.fetch.pollIntervalSeconds` | `30` seconds between completed cycles |
| `defaults.batchSize` | `read.fetch.batchSize` | `100` records requested per page |
| `defaults.maxPagesPerCycle` | `read.fetch.maxPagesPerCycle` | `10` pages, including replay requests |
| `defaults.maxQueuedProcessesPerQuery` | `process.maxQueuedProcesses` | `10_000` queued and active records |
| `defaults.finalityLagSeconds` | `read.consistency.finalityLagSeconds` | `15` seconds behind the local clock |
| `defaults.overlapSeconds` | `read.consistency.overlapSeconds` | `30` seconds of replay history |
| `defaults.debugRestClient` | None | `false`; enables REST-facade request diagnostics |

### Shared query options

| Path | Required/default | Purpose |
| --- | --- | --- |
| `type` | Required | One of the three [query types](queries.md) |
| `id` | Required | Unique non-negative safe integer in this engine |
| `title` | Required | Non-empty display title |
| `icon` | Optional | Application display metadata |
| `read.fetch.restProvider` | Required | Provider name configured in the REST client |
| `read.fetch.network` | Required | Network name configured in the REST client |
| `read.pauseAfterConsecutiveFailures` | Required | Positive integer; pauses the reader at this failure count |
| `read.fetch.nextDelayMs` | `0` milliseconds | Delay between successive page requests |
| `read.until` | Optional | Synchronous `(cursor) => boolean`; pauses reading after a successful completed sweep when true |
| `process.pauseAfterConsecutiveFailures` | Required | Positive integer; pauses processing after consecutive failures for an item |
| `process.nextDelayMs` | `0` milliseconds | Delay between processing attempts, including retries |
| `process.holdPeriod` | `"daily"` | Retention of successful in-memory process history |
| `process.context` | Optional string | Application-defined label, not injected handler services |
| `params.timestamp` | `"0.000000000"` | Initial/restored exclusive source cursor |
| `params.index` | `0` | Initial/restored cursor tie-breaker |

The query must still supply a `params` object even when all its fields are optional.

### Source-specific options

| Path | Applies to | Required/default |
| --- | --- | --- |
| `abi` | Log queries | Required; describes selected logs unambiguously |
| `contract.address` | Single-contract logs | Required Mirror Node endpoint identifier |
| `contract` | Other queries | Optional metadata |
| `contract.id`, `contract.type`, `contract.version` | All queries | Optional application metadata; type/version select specialized log handlers |
| `params.topics` | Log queries | Required and non-empty for multi-contract queries; optional for single-contract queries |
| `params.contractResolver` | Multi-contract logs | Optional `(address) => metadata`, synchronous or asynchronous |
| `process.skipEventNames` | Log queries | `[]`; exact names excluded after successful decoding |
| `params.accountId` | Transactions | Required account/contract endpoint identifier |
| `params.result` | Transactions | `"success"` (default), `"fail"`, or `null` to remove the result filter |
| `process.skipTransactionTypes` | Transactions | `[]`; exact type names to exclude |
| `process.unhandledTransactionPolicy` | Transactions | `"error"`; `"skip"` deliberately discards types without handlers |

### Timing, capacity, and retention

Polling intervals must be finite and at least one second. Page counts, batch sizes, and capacities must be positive safe integers; use a batch size supported by your Mirror Node endpoint. Delays, finality lag, and overlap must be finite and non-negative. Topic-filtered overlap must be shorter than the usable seven-day window; the engine rejects values that cannot advance.

Read loops have a randomized startup delay and +/- 10% polling jitter. `pollIntervalSeconds` is a cadence target, not an exact timer. `read.until` is not a server-side upper timestamp filter and does not wait for the processing queue to drain.

Retention is in memory: `"none"` removes a successful record immediately, `"daily"` retains it for 24 hours, `"weekly"` for 7 days, and `"monthly"` for 30 days. It does not persist history or control the lifetime of failed/queued records.

`process.maxQueuedProcesses` must be at least `read.fetch.batchSize`, because a prepared page is committed atomically. Retained successful records do not count toward this queued-and-active capacity, but they still consume memory.

At capacity, reading auto-pauses for backpressure and auto-resumes below 80% occupancy. An explicit `pauseRead()` takes ownership and is not undone automatically; only `resumeRead()` releases it. A prepared page that does not fit waits for sufficient room; pages are never split or allowed to exceed the cap.

## Runtime management

Here, `id` is the numeric query ID. Inspection methods are synchronous; lifecycle methods return `Promise<void>`. Call lifecycle methods outside active package callbacks.

```ts
console.log("Reader", sync.getReadStatus(1)?.status);
console.log("Read cursor", sync.getReadStatus(1)?.cursor);
console.log("Processing queue", sync.getQueueStatus(1)?.status);

await sync.pauseRead(1); // Stops ingestion; processing can continue.
await sync.pauseProcessing(1); // Waits for an active handler to settle.

console.log("Recent processes", sync.listProcesses(1, 1, 20, undefined, "desc"));
console.log("Failed process", sync.getFailedProcess(1, { includePayload: true }));

await sync.resumeProcessing(1);
await sync.resumeRead(1);
```

Use this recipe on an active or normally paused query. A terminal `FAILED` or `STOPPED` side cannot be resumed; see [error recovery](#error-channels-and-recovery).

| Call | Result / purpose |
| --- | --- |
| `getQuery(id)` | Resolved query snapshot, or `undefined` |
| `listQueries()` | All resolved query snapshots |
| `listQueries(page, pageSize, options?)` | Query page; options: `type`, `network`, `order` (`"insertion"`, `"id-asc"`, `"id-desc"`) |
| `getReadStatus(id)` | `{ status: { name, info? }, cursor: { timestamp, index } }`, or `undefined` |
| `getQueueStatus(id)` | `{ status: { name, info? } }`, or `undefined` |
| `getLastReadCycle(id)` | Most recent terminal `ReadCycleAudit`, or `undefined` |
| `getProcessedCheckpoint(id)` | Safe restart cursor, or `undefined` |
| `getInFlightProcess(id, options?)` / `getFailedProcess(id, options?)` | Active/latest failed process metadata, or `undefined`; options: `{ includePayload: true }` for payloads |
| `listProcesses(id, page, pageSize, status?, order?)` | Retained-process page; optional process status and `"asc"` (default) or `"desc"` ordering |
| `countProcesses(id, status?)` | Retained-record count; zero for a missing query |
| `getProcessingTimeline(id, size)` | Source-ordered `Process[]`; up to `size` processed records, active/failed center, and up to `size` queued records |
| `getStatistics(id)` | `{ total, queued, processing, processed, failed }`, or `undefined` |
| `getSummary()` | Instance query/process counts and separate read/processing lifecycle totals |
| `pauseRead(id)` / `resumeRead(id)` | Pause/resume ingestion |
| `pauseProcessing(id)` / `resumeProcessing(id)` | Pause/resume handler execution |
| `addQuery(config, skipOnDuplication?)` | Add and start a full query configuration |
| `updateQuery(config, skipOnDuplication?)` | Stop and replace the runtime with the same ID |
| `removeQuery(id)` | Stop and remove a query |
| `shutdown()` | Stop all queries and release runtime state |

Pages contain `{ items, page, pageSize, total, totalPages, hasPrev, hasNext }`. Page numbers start at one and are clamped; an empty result reports one empty page. Statistics describe **currently retained records**, not durable lifetime totals. Retention, replacement, removal, and restart affect the counts.

Returned queries, processes, cursors, audits, and summaries copy arrays, plain objects, and `Date` values. Functions, `Map`, `Set`, `Error`, and custom class instances remain shared references, so treat every inspection result as read-only. Mutating copied plain data does not reconfigure the running engine.

The default return type of `getInFlightProcess()` and `getFailedProcess()` is `ProcessWithoutPayload`; it does not contain `data` or `rawData`. Passing the literal `{ includePayload: true }` returns `Process`, while a runtime boolean correctly produces the union of both possible shapes. Successful read-cycle items expose recursively read-only `SyncReadItem` values, while unhandled-item hooks expose a recursively read-only `SyncTransaction` instead of `unknown`.

`addQuery()` and `updateQuery()` accept an optional `skipOnDuplication` argument, which defaults to `true`. Duplicate detection compares normalized runtime configuration while ignoring `id`, `title`, `icon`, and live statuses; callback values compare by identity. Set it to `false` only when equivalent readers are deliberate. For `updateQuery()`, a duplicate of another query leaves the target unchanged when skipping is enabled.

`getProcessingTimeline(id, size)` can therefore return more than `size` records; `size: 0` returns an empty array. Runtime inspection is not a substitute for a durable audit log.

### Error channels and recovery

| Failure | Where to handle it |
| --- | --- |
| Configuration or lifecycle operation rejected | Catch the returned API promise |
| Reading, decoding, normalization, handler selection, or `until` failed | `onReadFailure`; inspect read status and the cycle audit |
| Processing attempt failed | `onProcessTryFailed`; inspect that attempt and its steps |
| Observer rejected | `onHookError`; runtime response follows `hookFailurePolicy` |

Callback error arguments are `unknown`: JavaScript can throw values other than `Error`. Stored `ProcessTry.error` and read-cycle `error` fields are JSON strings containing error details, not `Error` objects.

After correcting an ordinary threshold failure, resume the affected paused side. Explicit processing resume resets the consecutive-failure budget while retaining attempt history. A terminal `FAILED` or `STOPPED` side requires runtime replacement rather than resume.

`updateQuery()` takes a **full configuration**, not a patch. It stops the old runtime, discards its in-memory queue/history/deduplication state, and starts from the supplied `params.timestamp` and `params.index`. It does not automatically preserve the previous checkpoint. When continuity is intended, capture `getProcessedCheckpoint(id)` before replacement and explicitly include both fields in the replacement configuration. Expect safe replay and use idempotent handlers.

## Handler types and registration

For complete typed event and transaction examples, see [Queries and handlers](queries.md).

`LogHandler<Normalized, DecodedArguments>` types both stages of a log handler. The decoded-argument parameter defaults to `unknown`; registration infers both parameters and checks the normalizer/handler pairing, but cannot prove that string registration keys match an ABI.

`TransactionHandler<"CRYPTOTRANSFER">` receives a recursively read-only `SyncTransaction` with its existing `name` narrowed. It has no normalization method and does not add transaction-specific fields.

### Handler context

| Member | Meaning |
| --- | --- |
| `queryId` | Query ID, or `null` for standalone processing |
| `network`, `provider` | Configured selection |
| `contract` | Resolved contract metadata |
| `restClient` | Underlying configured `HederaRestClient` |
| `raw` | Validated Mirror Node source record |
| `signal` | Optional cooperative abort signal |
| `addStep` | Optional synchronous attempt diagnostic callback |
| `now()` | Runtime clock, Unix milliseconds |

Preparation callbacks never receive `addStep`. Managed execution supplies it to the handler and makes it a no-op after the awaited handler settles. Standalone `LogEventProcessor` callers can supply it through execution options only.

`context.raw`, transaction-handler data, resolved metadata, and standalone resolver inputs are recursively read-only in their public TypeScript contract. This is not runtime freezing: mutation through an unsafe cast remains visible to later application code receiving the same record. Internal source identity is captured independently.

Overlap replay can invoke normalizers and resolvers again, and repeat `onUnhandledItem` for deliberately skipped transactions. Deduplication suppresses duplicate queue insertion and handler execution, not repeated preparation. Keep preparation pure/replay-safe and that observer idempotent.

### Selecting specialized log handlers

Resolution checks these keys in order:

1. `EventName@ContractType:Version`
2. `EventName@ContractType`
3. `EventName`

Set `contract.type` and `contract.version`, or resolve them per address in a global query, to select specializations. These are application labels, not automatic contract-standard detection. Event names, handler keys, and exclusions are exact and case-sensitive.

A queued process retains its selected handler. Replacing a registry entry does not change already queued work.

### Classes and bulk registration

`ArgsLogHandler<Args, Normalized>` supplies synchronous mapping through `mapArgs`; `TxHandler` and `HookBase` support class-based transaction handlers and observers. Implement callbacks as function-valued class fields to preserve strict parameter checking:

```ts
import { TxHandler, type TransactionHandler } from "@davincigraph/hedera-event-sync";

class TransferHandler extends TxHandler<"CRYPTOTRANSFER"> {
  readonly handle: TransactionHandler<"CRYPTOTRANSFER">["handle"] = (transaction, context) => {
    console.log("Crypto transfer", transaction, context.queryId);
  };
}
```

The following uses an existing `sync` and the class above. For typed log payloads, replace the illustrative handler with your event-specific handler:

```ts
import { registerLogs, registerTxs, type LogHandler } from "@davincigraph/hedera-event-sync";

const logHandler: LogHandler = {
  normalize: (args) => args,
  handle: (data) => { console.log(data); },
};
registerLogs(sync.registry, [
  ["Transfer@TOKEN:1", logHandler],
  ["Transfer@TOKEN", logHandler],
]);
registerTxs(sync.registry, [
  ["CRYPTOTRANSFER", new TransferHandler()],
]);
```

## Processes, attempts, and steps

A **process** represents one source record. Each execution adds an **attempt** to `process.tries`; each attempt contains ordered **steps**. A retry creates another attempt, not another process.

| Layer | States |
| --- | --- |
| Source process | `Queued`, `Processing`, `Processed`, `Failed` |
| Execution attempt | `PROCESSING`, `SUCCEEDED`, `FAILED` |
| Reader or processing queue | `RUNNING`, `PAUSING`, `PAUSED`, `RESUMING`, `FAILED`, `STOPPED` |

`onProcessStarted` receives an attempt with no steps yet. Use `onStepCreated` for incremental progress. An `onProcessTryFailed` snapshot can still show the process as `Processing` because the hook runs before the retry/threshold state transition; its attempt outcome is `FAILED`. The processing-threshold hook reports the final `Failed` process.

`context.addStep?.({ title, desc?, type?, data? })` is synchronous. The default severity is `Info`; `Warning`, `Error`, and `Success` are also accepted, including lowercase spellings. Steps are diagnostics, not independently executed tasks. Await the actual work in your handler and add steps where they describe progress. Step timestamps are ISO strings and step durations are seconds.

`process.nextDelayMs` delays **between attempts**, not between the steps inside a handler. A fast handler may start and finish between two UI polling requests; use hooks to observe its transitions without slowing the handler artificially.

## Hook reference

All hooks are optional. Query, process, attempt, and step arguments are read-only snapshots; see [snapshot and ordering guarantees](#hook-guarantees-and-failure-policy).

| Hook | Arguments and purpose |
| --- | --- |
| `onQueryAdded(query)` | Registered, before reader initialization |
| `onQueryRemoved(query)` | Runtime stopped and in-memory state released |
| `onReadInit(query, pollIntervalSeconds)` | Reader initialized with its resolved interval |
| `onReadCycleStarted(query, cursor, batchSize)` | Read cycle begins |
| `onReadPage(query, meta)` | Fetched page before preparation; `meta.kind` and raw `meta.itemCount` |
| `onReadCycleCompleted(query, result)` | Terminal result: `success`, `cursor`, `items`, `startedAt`, `endedAt`, optional `error` and `itemCount` |
| `onReadFailure(query, error, consecutiveFailures)` | Read or `until` predicate failed |
| `onReadConsecutiveFailuresReached(query, consecutiveFailures)` | Reader reached its failure threshold |
| `onUnhandledItem(query, item)` | Transaction has no handler, before its policy is applied; `item` has `kind`, `itemType`, `sourceKey`, and `raw` |
| `onProcessEnqueued(query, process)` | Queued, before processing eligibility |
| `onProcessStarted(query, process, attempt)` | New attempt with an initially empty step list |
| `onStepCreated(query, process, attempt, step)` | New ordered diagnostic |
| `onProcessTryFailed(query, process, attempt, error)` | Attempt failed; the process may retry |
| `onProcessSucceeded(query, process)` | Handler completed successfully |
| `onProcessedCheckpoint(query, checkpoint, process)` | Current restart-safe checkpoint after success; it may remain unchanged |
| `onProcessConsecutiveFailuresReached(query, process, attempts)` | Processing threshold reached; `attempts` is the consecutive-failure count, not necessarily lifetime tries |
| `onQueryStatusChange(query, payload)` | `payload.kind` is `"read"` or `"process"`; `payload.status` has `name` and optional `info` |
| `onHookError(failure)` | Observer failure with `hook`, zero-based `hookIndex`, `scope`, `policy`, query identity, and thrown `error` |

## How synchronization works

For each query, the package runs two independent loops:

1. The read loop fetches and validates Mirror Node pages, performs source-specific preparation, and atomically commits a prepared page to the in-memory queue. For logs, preparation follows this order: strict ABI decoding, explicit exclusion, contract resolution, handler selection, and normalization.
2. The processing loop handles records in committed queue order. Records within each fetched page are source-ordered, while a record indexed late during overlap replay can be appended after a newer record. A failed item is retried at the head of the queue until its configured consecutive-failure threshold is reached.

Each item's `onProcessEnqueued` notification settles before that item becomes eligible for processing.

The read cursor and processed checkpoint intentionally describe different progress:

- The **read cursor** is the highest fully ingested source position or fully drained time-window boundary. It can therefore include records deliberately excluded by query policy and can advance across an empty bounded window.
- The **processed checkpoint** starts at the query's configured cursor and advances conservatively from the successful-processing high-water mark. It deliberately trails that high-water mark by the configured overlap horizon, plus the nanosecond needed for the reader's exclusive starting cursor, so a restart can still discover records indexed late inside the replay window. It may remain unchanged across several successful handlers.

Persist the processed checkpoint when an application needs a durable restart position.

## Delivery guarantees

- Pages are validated, decoded, normalized, and matched to handlers before they are committed to the queue.
- Forward pagination and overlap replay retain separate provider-issued continuations across bounded read cycles. After each paginated forward segment, a fresh sweep of the preceding replay range completes before the next forward segment. The sweep keeps a fixed range and continues across cycles when necessary; every cycle respects `maxPagesPerCycle`.
- Queue capacity is enforced without splitting a page commit.
- Provider-independent source keys suppress duplicate queue insertion and handler execution during overlap replay and provider failover while the runtime remains alive. A log is identified by its consensus timestamp and log index; a transaction is identified by its consensus timestamp.
- `finalityLagSeconds` keeps the request's upper timestamp bound behind the local current time to avoid querying the newest interval.
- `overlapSeconds` re-reads the configured history horizon so records indexed within that horizon can be recovered.
- Restarting from a persisted processed checkpoint provides at-least-once delivery for records visible within the configured overlap horizon. A mirror record indexed later than that horizon is outside this guarantee.

The queue, duplicate keys, queries, and checkpoints are in memory. Follow [Save and restore checkpoints](integrations.md) for durable resume.

Checkpoints also remain behind unfinished replay sweeps and older queued records. Replay can therefore increase the distance between a read cursor and its processed checkpoint. Backfills that require pagination make additional fresh requests for replay; dense replay ranges can span multiple polling cycles before forward pagination continues.

For logs, the cursor tie-breaker is the Mirror Node log `index`. For transactions, the cursor retains `nonce` as its tie-breaker, while canonical transaction source identity is the normalized `consensus_timestamp`. Optional provider metadata such as `transaction_hash` and `transaction_id` is never used for source identity.

Do not persist `onReadCycleCompleted.cursor` as a processing checkpoint: it can be ahead of handlers that have not completed.

Exactly-once external side effects are outside this package's scope. Handlers should use an application-level idempotency key or transactional outbox when replayed side effects must be suppressed.

## Failures and automatic pauses

Read failures increment the reader's consecutive-failure count. Processing failures keep the failed item at the head of the queue so source order is preserved. Each side pauses independently when its configured `pauseAfterConsecutiveFailures` threshold is reached.

Processing attempts expose ordered steps and one of these outcomes:

- `PROCESSING`: the attempt started and has no `endTs`.
- `SUCCEEDED`: the handler completed.
- `FAILED`: the handler or processing pipeline rejected.

`read.until` is a synchronous predicate evaluated after a successful cycle-completion notification, once the pending replay sweep has completed. Returning `true` pauses the reader with the reason `fulfilled`. A predicate failure follows the normal read-failure and pause policy but does not retroactively change the completed cycle into a failed read.

Read and predicate failures share one consecutive-failure counter. During a successful cycle that defers `until`, the counter resets if the latest failure came from reading and is preserved if it came from the predicate. A fully successful cycle or an explicit resume transition clears it; automatic backpressure recovery does not.

## Hook guarantees and failure policy

Hook failures are contained and reported to `onHookError`. `hookFailurePolicy` determines the runtime response:

| Policy | Behavior |
| --- | --- |
| `"continue"` | Report the hook failure and continue. This is the default. |
| `"pause"` | Pause the affected read or processing side; query-scoped hook failures affect both. |
| `"fail-fast"` | Stop the affected query runtime and expose a `FAILED` status. |

The selected `"pause"` or `"fail-fast"` runtime action is initiated before `onHookError` observers are awaited. A slow or non-settling error reporter therefore cannot delay the configured safety policy, although the hook dispatch itself remains pending until that reporter settles.

Query, process, attempt, step, and JSON-like result objects are recursively copied before each hook invocation. Arrays, plain objects, and `Date` values are copied; functions, `Map`, `Set`, `Error`, and custom class instances are passed by reference. Snapshotting copies property descriptors without invoking accessors; a proxy that cannot be inspected safely is treated as an opaque shared value. Hook data is exposed through recursively read-only TypeScript views, but it is not frozen at runtime; opaque values typed as `unknown` remain the consumer's responsibility.

`onReadPage.itemCount` is the fetched page length. On successful cycles, `onReadCycleCompleted.items` contains eligible records from fully handled pages, including explicit exclusions and overlap replays, and `itemCount` equals its length. Failed cycles report an empty `items` array and omit `itemCount`. These values are not newly enqueued-process counts.

Steps are delivered in order before the corresponding success or failure hook. Status transitions preserve their emission order within each read job and each processing queue, and `onQueryAdded` precedes `onReadInit`. Replacing hooks affects the next dispatched event, not an event already being dispatched.

A failure in `onProcessSucceeded` does not retry a handler that already succeeded. Persistence hooks should remain idempotent and observe `onHookError`.

## Singleton lifecycle

`HederaEventSync.create()` uses a process-local singleton by default. If an active singleton already exists, another call returns that instance; the later call's REST client, hook-failure policy, and defaults are not applied. Hooks are replaced separately with `setHooks()` on the returned instance. This prevents separate modules in the same process from accidentally running independent synchronization engines.

The guarded singleton remains reserved until shutdown has completely settled. A guarded `create()` called during shutdown waits for that shutdown and then creates or returns the single replacement instance. After `await sync.shutdown()` resolves, a later guarded `create()` constructs a new instance normally.

For an application that deliberately manages multiple independent engines, pass `singletonGuard: false` to every such call:

```ts
const isolatedSync = await HederaEventSync.create({
  restClient,
  singletonGuard: false,
});
```

## Lifecycle safety

Lifecycle controls must not be called while a package-invoked hook, handler, normalizer, resolver, or `read.until` callback belonging to the same synchronizer is active. The operation could otherwise wait for its own callback. Such calls reject with `LifecycleReentrancyError` even if you omit `await` or use `void`.

If callback-driven control is required, schedule it after the callback returns and handle the resulting promise.

Only short internal state transitions use the global mutation lock. Hooks and runtime start/stop work do not hold it, so operations for different query IDs can progress independently. A conflicting operation for a query already changing state rejects with `LifecycleBusyError`; retry it after the active operation settles.

`shutdown()` and `removeQuery()` abort active Mirror Node pagination, signal active handlers through `context.signal`, wait for loops and ordered hook notifications, and then release in-memory state.

Application-callback cancellation is cooperative: lifecycle methods cannot forcibly terminate user code and wait for active handlers, normalizers, resolvers, predicates, and ordered hooks to settle. Handlers should observe `context.signal`; any awaited callback that never settles can prevent shutdown or removal from completing.

Pause and resume methods fulfill only after reaching their requested state. If a concurrent failure or stop wins the transition, the method rejects with the terminal state and its recorded reason instead of falsely reporting success.

All four pause/resume controls reject once synchronizer shutdown begins, including after shutdown completes. On an active instance, a control call for a missing query ID remains a no-op.

## Advanced public APIs

- `define()` preserves literal query types while normalizing one query or a query array to a readonly array.
- `RestClient` is the package's low-level synchronization facade over `HederaRestClient`; its optional constructor settings use `RestClientOptions`. Most applications should use `HederaEventSync`.
- `LogEventProcessor` supports strict preparation and direct execution outside a managed query runtime.

## Package exports

The root export contains the synchronizer, handlers, hooks, query and domain types, normalized Mirror Node types, errors, `HederaRestClient`, and the package-owned REST facade. Supported subpath exports are:

- `@davincigraph/hedera-event-sync/types/config`
- `@davincigraph/hedera-event-sync/types/domain`
- `@davincigraph/hedera-event-sync/core/logs/LogEventProcessor`

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Nothing appears | Install hooks before `addQuery()`; verify the provider/network, real contract/account, ABI, and topic values. Allow for startup delay, polling cadence, finality lag, and Mirror Node indexing. |
| Old events are missing | A current starting cursor is live-only. Choose an earlier cursor for backfill; the package cannot recover records absent from the selected mirror. |
| Much more history is being read than expected | Omitted timestamp/index default to zero. Supply an explicit starting cursor. |
| Reading pauses | Inspect `getReadStatus(id)?.status.info` and `onReadFailure`. Check capacity/backpressure, failure thresholds, hook policy, `read.until`, and operator pauses. |
| A transaction query fails on an unexpected type | Register its handler, explicitly exclude that type, or deliberately choose the unhandled-type skip policy. Registration alone does not filter responses. |
| A log query fails even though an event is skipped | Exclusion follows decoding. Unknown signatures and malformed logs still fail; supply a matching ABI/filter rather than relying on `skipEventNames`. |
| A UI misses the processing state or steps | Use `onProcessStarted` and `onStepCreated`; a fast attempt can finish between UI polls. `process.nextDelayMs` delays between attempts, not within a handler. |
| Checkpoint stays unchanged after successes | It intentionally retains overlap and may be held back by unfinished replay or older queued records. Do not replace it with the read cursor. |
| History or statistics shrink/disappear | They describe retained in-memory records. Check `holdPeriod`; restart/removal/replacement releases runtime history. |
| A second `create()` ignores options | The default singleton is intentional. Configure the engine at startup; replacing hooks uses `setHooks()`. |
| Lifecycle control fails inside a callback | Invoke it after the callback settles. Removing `await` is not a workaround. |
| Shutdown does not finish | An application callback may still be pending. Return/await handler work, observe `context.signal` where available, and ensure external operations settle. |
| Imports or TypeScript execution fail | Use ESM; run the README's `.mjs` example with Node. TypeScript consumers need 5.4+. Use only the [documented exports](#package-exports). |

For durable observability, forward hooks to storage or your monitoring system. This package does not provide a UI, HTTP server, or durable history database.
