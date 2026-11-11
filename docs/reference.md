# API reference and guarantees

[Back to the README](../README.md) · [Queries and handlers](queries.md) · [Application integrations](integrations.md) · [Changelog](../CHANGELOG.md) · [Release checklist](releasing.md)

Use this page to look up options and exact behavior after choosing your application's queries, handlers, and observers. Code snippets using `sync` assume an existing synchronizer and run outside its active callbacks.

- [Configuration](#configuration-reference)
- [Runtime management and recovery](#runtime-management)
- [Safe query editing](#safe-query-editing)
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
| `querySource` | Required | Sync/async complete catalog loader; called only by `start()` and `reloadQueries()` with `{ signal, reason }` |
| `beforeReplace` | Optional | Awaited persistence boundary with final processed checkpoints before restart/reload/shutdown releases old work; required for durable replacement guarantees |
| `singletonGuard` | `true` | Reuse the process-local instance; `false` deliberately creates an isolated engine |
| `hookFailurePolicy` | `"continue"` | `"continue"`, `"pause"`, or `"fail-fast"` |
| `failureRecovery` | Disabled | `{ enabled: true, delaySeconds?: number }` retries failure-limit pauses independently; interval defaults to 3600 seconds. See [lifecycle behavior and inspection](lifecycle.md#automatic-failure-recovery). |
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

`sync.holdFailureRecovery()` acquires an application recovery hold and returns an
idempotent release function. Use `try/finally` around application-owned management
work that precedes a package lifecycle call. It defers automatic retries only;
see [automatic failure recovery](lifecycle.md#automatic-failure-recovery).

### Shared query options

| Path | Required/default | Purpose |
| --- | --- | --- |
| `type` | Required | One of the three [query types](queries.md) |
| `id` | Required | Unique non-negative safe integer in this engine |
| `title` | Required | Non-empty display title |
| `icon` | Optional | Application display metadata |
| `initialState` | Optional | Initial read/process `RUNNING` or `PAUSED` states for newly introduced IDs; existing runtime pause ownership survives reload |
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
| `params.logFilter` | Multi-contract logs | Optional `(log, signal?) => boolean`, synchronous or asynchronous; frozen snapshot checked before decoding, false skips and errors retry the page |
| `process.skipEventNames` | Log queries | `[]`; exact names excluded after successful decoding |
| `params.accountId` | Transactions | Required account/contract endpoint identifier |
| `params.transactionType` | Transactions | Optional single `TransactionTypes` name; omitted/`undefined` means no Mirror Node transaction-type filter; `null` is invalid |
| `params.result` | Transactions | `"success"` (default), `"fail"`, or `null` to remove the result filter |
| `process.skipTransactionTypes` | Transactions | `[]`; exact type names to exclude |
| `process.unhandledTransactionPolicy` | Transactions | `"error"`; `"skip"` deliberately discards types without handlers |

`params.transactionType` selects the source transaction category and combines with account, timestamp, and result filters. It must be a single nonempty string; normalization trims surrounding whitespace and preserves case. Known names have editor completion, while future/provider names remain accepted strings; the Mirror Node determines which names it supports. Handler registration never infers this filter; local skip rules and missing-handler errors still apply to returned records. A category such as `CRYPTOTRANSFER` is not a complete account balance history: fees and other balance movements can appear in other categories. See the [transaction recipe](queries.md#transactions) before narrowing a query.

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
| `listQueries(page, pageSize, options?)` | Query page; options: `type`, `network`, `contractType`, `readStatus`, `processStatus`, `search`, `order` (`"insertion"`, `"id-asc"`, `"id-desc"`); see [query-list filtering](#query-list-filtering) |
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
| `pauseRead(id, options?)` / `resumeRead(id)` | Pause/resume ingestion; pause accepts `{ preserveExistingPause?: boolean }` |
| `pauseProcessing(id, options?)` / `resumeProcessing(id)` | Pause/resume handler execution; pause accepts `{ preserveExistingPause?: boolean }` |
| `validateQuery(config, initialState?)` | Normalize and validate a detached configuration without registering it |
| `setHooks(hookOrArray)` | Synchronously replace the observer list for subsequent events; `[]` removes observers |
| `registry` | Shared `HandlerRegistry` used by this engine; see [registration methods](#registry-methods) |
| `start(options?)` | Load and validate the initial catalog, then start work |
| `pauseAll(options?)` / `resumeAll(options?)` | Install/release selected global holds with `{ scope?: "read" \| "process" \| "both", timeoutMs?: number }`; default scope is `"both"`, independent of per-query pauses |
| `restartQueries(options?)` | Replace runtimes from the accepted catalog and final processed checkpoints |
| `reloadQueries(options?)` | Load a complete candidate catalog, validate it, then replace runtimes |
| `getLifecycleState()` | Snapshot of `started`, `closed`, `generation`, `globalReadPaused`, `globalProcessPaused`, `globalPaused`, `maintenanceHeld`, `recoveryRequired`, optional current operation and last error |
| `shutdown(options?)` | Settle work, confirm final checkpoint persistence, and release runtime state |

Pages contain `{ items, page, pageSize, total, totalPages, hasPrev, hasNext }`. Page numbers start at one and are clamped; an empty result reports one empty page. Statistics describe **currently retained records**, not durable lifetime totals. Retention, replacement, removal, and restart affect the counts.

`page` and `pageSize` must be positive safe integers. `listProcesses()` orders by source timestamp/index, not process creation time; it returns an empty page for a missing query. `getProcessingTimeline()` accepts a non-negative safe integer and returns `[]` for a missing query. Process lists and timelines include payloads; only the active/failed-process getters omit them by default.

`getSummary()` returns `{ totalQueries, runningQueries, pausedQueries, totalProcesses, queued, processing, processed, failed, read, process }`. A query counts as running only when both sides are `RUNNING`; it counts as paused when either side is `PAUSED` or `PAUSING`. These two counts need not sum to `totalQueries`. Each of `read` and `process` contains `{ running, paused, failed, stopped, transitioning }`, with `PAUSING` and `RESUMING` counted as transitioning.

On the per-query pause methods, `preserveExistingPause: true` retains an existing independent pause reason instead of replacing it with an administrative pause reason. It does not preserve automatic backpressure ownership: explicitly pausing a reader still requires an explicit resume. Global holds are separate from these per-query pause settings. `setHooks()` rejects with `LifecycleBusyError` while an instance-wide lifecycle operation is active.

Returned queries, processes, cursors, audits, and summaries copy arrays, plain objects, and `Date` values. Functions, `Map`, `Set`, `Error`, and custom class instances remain shared references, so treat every inspection result as read-only. Mutating copied plain data does not reconfigure the running engine.

The default return type of `getInFlightProcess()` and `getFailedProcess()` is `ProcessWithoutPayload`; it does not contain `data` or `rawData`. Passing the literal `{ includePayload: true }` returns `Process`, while a runtime boolean correctly produces the union of both possible shapes. Successful read-cycle items expose recursively read-only `SyncReadItem` values, while unhandled-item hooks expose a recursively read-only `SyncTransaction` instead of `unknown`.

The required loader is the only configuration interface. Distinct IDs remain distinct even when their configurations are equivalent; duplicate IDs reject the complete candidate catalog. See [lifecycle coordination](lifecycle.md) and the [v2 migration guide](migration-v2.md).

`getProcessingTimeline(id, size)` can therefore return more than `size` records; `size: 0` returns an empty array. Runtime inspection is not a substitute for a durable audit log.

### Query-list filtering

The paginated overload accepts `ListQueriesOptions`. All filters apply to the currently loaded runtime catalog; they do not load saved queries from your database or call `querySource`. Omitted or explicitly `undefined` options leave that field unfiltered. The no-argument `listQueries()` overload still returns every loaded query.

#### Contract type

`contractType?: string | null | undefined` selects configured contract metadata:

| `contractType` value | Queries included |
| --- | --- |
| Omitted or `undefined` | All contract types; existing behavior is unchanged |
| A string | An exact, case-sensitive match against the query's `contract.type`; strings are not trimmed or normalized |
| `null` | Queries with no `contract`, or an absent, `null`, empty, or whitespace-only `contract.type` |

`contractType` matches the query's configured metadata, not per-log metadata from `contractResolver`. For example, `type: "Single-Contract-Logs"` selects the query kind, while `contractType: "TOKEN"` selects an application-defined contract label.

#### Reader and processor status

`readStatus` and `processStatus` each accept one `QueryStatus["name"]` or a readonly array of those names:

| Value | Queries included |
| --- | --- |
| Omitted or `undefined` | All statuses for that side |
| A status name | An exact match against that side's effective status |
| An array of status names | Any listed status for that side; `[]` matches no queries |

Names are case-sensitive: `"RUNNING"`, `"PAUSING"`, `"PAUSED"`, `"RESUMING"`, `"FAILED"`, or `"STOPPED"`. Invalid names, non-string entries, and non-array objects are rejected, including when the catalog is empty.

The filters use the same effective statuses returned in query snapshots and by `getReadStatus()` / `getQueueStatus()`. A global read hold changes an otherwise running reader to `PAUSING` while it settles, then `PAUSED`; it does not change the processing filter. A global processing hold works independently. Maintenance holds apply to both sides. See [read and processing holds](lifecycle.md#read-and-processing-holds).

Reaching the consecutive-failure limit pauses the affected side as `PAUSED`; it is not a terminal `FAILED` status. A `FAILED` filter alone therefore does not find every query that needs attention. Inspect the returned status's `info` and the relevant read/process history to understand a pause.

#### Search

`search?: string | undefined` is a literal, case-insensitive substring match against any of:

- The configured query title.
- The query ID converted to a string.
- Configured `contract.address` and `contract.id` metadata, when present.
- `params.accountId` for transaction queries.

Leading and trailing whitespace in the search term is trimmed; an empty or whitespace-only term leaves search unfiltered. A non-string value is rejected. Search is one literal term, with no regular expressions, wildcards, or token splitting. For example, `"12"` can match query IDs `12` and `120`, while `"0.0.98"` can match a configured account ID.

Search does not resolve identifiers through a Mirror Node or inspect encountered logs, payloads, or `contractResolver` results. An ID and its EVM address are both searchable only if both are present in the supported configured fields.

#### Combining filters and pagination

Separate filter fields combine with AND, including `type`, `network`, `contractType`, `readStatus`, `processStatus`, and `search`. Status alternatives within one array use OR; search matches any of its supported fields.

```ts
// Existing calls keep their behavior.
const allQueries = sync.listQueries();
const allTypes = sync.listQueries(1, 20, { contractType: undefined });

const tokens = sync.listQueries(1, 20, {
  network: "mainnet",
  type: "Single-Contract-Logs",
  contractType: "TOKEN",
  order: "id-asc",
});

// Find Station readers that are paused or settling while processing stays enabled.
const pausedReaders = sync.listQueries(1, 25, {
  network: "testnet",
  readStatus: ["PAUSED", "PAUSING"],
  processStatus: "RUNNING",
  search: "station",
  order: "id-asc",
});

// Match configured contract/account identifiers without a Mirror Node lookup.
const accountQueries = sync.listQueries(1, 25, { search: "0.0.98" });

// A page beyond the filtered result is clamped to its last available page.
const unspecified = sync.listQueries(999, 20, {
  contractType: null,
  order: "id-desc",
});
console.log(unspecified.page, unspecified.total, unspecified.totalPages);
```

Filtering happens before pagination. `total`, `totalPages`, `hasPrev`, and `hasNext` describe the filtered result, and an out-of-range page is reclamped to that result's last page. No matches produce page 1 with an empty `items` array and `totalPages: 1`. These rules apply to all three order modes: `"insertion"` (the default), `"id-asc"`, and `"id-desc"`.

These are inspection-only filters: they do not change ingestion, checkpoints, ABI decoding, handler selection, or processing. Applications can expose the options through their own API and UI; the package performs the matching and pagination. To admit or reject source logs before decoding, use the separate [`params.logFilter` option](queries.md#multiple-contracts-by-topic).

### Error channels and recovery

| Failure | Where to handle it |
| --- | --- |
| Configuration or lifecycle operation rejected | Catch the returned API promise |
| Reading, decoding, normalization, handler selection, or `until` failed | `onReadFailure`; inspect read status and the cycle audit |
| Processing attempt failed | `onProcessTryFailed`; inspect that attempt and its steps |
| Observer rejected | `onHookError`; runtime response follows `hookFailurePolicy` |

Callback error arguments are `unknown`: JavaScript can throw values other than `Error`. Stored `ProcessTry.error` and read-cycle `error` fields are JSON strings containing error details, not `Error` objects.

After correcting an ordinary threshold failure, resume the affected paused side. Explicit processing resume resets the consecutive-failure budget while retaining attempt history. A terminal `FAILED` or `STOPPED` side requires runtime replacement rather than resume. Replacement carries it forward as an independent pause with its reason; explicitly resume the affected side after correcting the cause.

Use `restartQueries()` for runtime recovery with the accepted configuration, or `reloadQueries()` after changing the loader source. Both preserve final processed checkpoints for unchanged logical IDs rather than the reader's potentially advanced cursor. Expect replay and use idempotent handlers. Source identity changes require a fresh ID; the package will not silently carry a checkpoint to a different source. For transactions, adding, removing, or changing `params.transactionType` changes source identity, as does changing `params.result`. Choose the new ID's initial checkpoint deliberately so a broader filter can include previously excluded history.

## Handler types and registration

For complete typed event and transaction examples, see [Queries and handlers](queries.md).

`LogHandler<Normalized, DecodedArguments>` types both stages of a log handler. The decoded-argument parameter defaults to `unknown`; registration infers both parameters and checks the normalizer/handler pairing, but cannot prove that string registration keys match an ABI.

`TransactionHandler<"CRYPTOTRANSFER">` receives a recursively read-only `SyncTransaction` with its existing `name` narrowed. It has no normalization method and does not add transaction-specific fields.

### Registry methods

Use `sync.registry`, or construct `new HandlerRegistry()` for standalone processing:

| Method | Behavior |
| --- | --- |
| `registerLog(key, handler)` | Register or replace a log handler under an exact `HandlerKey` |
| `unregisterLog(key)` | Remove that registration; a missing key is a no-op |
| `resolveLog(eventName, contractType?, version?)` | Return the most specific registered log handler, or `undefined` |
| `registerTransaction(type, handler)` | Register or replace a transaction handler under its exact name |
| `unregisterTransaction(type)` | Remove that registration; a missing name is a no-op |
| `resolveTransaction(type)` | Return the exact transaction handler, or `undefined` |

These methods are synchronous. `registerLogs(registry, entries)` and `registerTxs(registry, entries)` register `[key, handler]` pairs in order; later duplicate keys replace earlier ones. The registry retains each handler object, including its class instance identity. Registration is a TypeScript boundary, not runtime ABI validation.

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

`params.logFilter` and standalone `LogPrepareOptions.logFilter` receive a separate frozen source snapshot with a frozen topics array. Their inputs cannot mutate the raw record used for subsequent decoding and callbacks. Returning false performs no handler work or durable checkpoint update. Filters must not persist cursors or application state: earlier accepted logs on that page are not committed yet.

Overlap replay can invoke filters, normalizers and resolvers again, and repeat `onUnhandledItem` for deliberately skipped transactions. Deduplication suppresses duplicate queue insertion and handler execution, not repeated preparation. Keep preparation pure/replay-safe and that observer idempotent. Reattach callback functions when restoring JSON-persisted query settings; engine snapshots retain functions, but JSON does not.

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

Install one `SyncHooks` object or an array with `sync.setHooks(...)`. Hooks may return `void` or `Promise<void>`. Observers run serially in array order for each event, but separate events and queries can overlap. A rejected observer is reported and does not prevent the remaining observers from receiving that event. Errors in `onHookError` are contained without recursive reporting.

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

1. The read loop fetches and validates Mirror Node pages, performs source-specific preparation, and atomically commits a prepared page to the in-memory queue. For logs, preparation follows this order: optional log-filter admission, strict ABI decoding, explicit exclusion, contract resolution, handler selection, and normalization.
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

Direct consumers can construct `new CompositeHooks(observers, { policy?, onFailure? })` and replace its observer array with `setHooks(observers)`. The separate `onFailure` control callback is invoked immediately and may return `void` or `Promise<void>`. Its promise is not awaited; synchronous throws and asynchronous rejections are contained, and reporting of the original hook failure continues. A standalone dispatcher only reports the selected policy; it cannot pause an engine without a corresponding control callback. The synchronizer supplies a synchronous implementation that applies the selected runtime policy.

Query, process, attempt, step, and JSON-like result objects are recursively copied before each hook invocation. Arrays, plain objects, and `Date` values are copied; functions, `Map`, `Set`, `Error`, and custom class instances are passed by reference. Snapshotting copies property descriptors without invoking accessors; a proxy that cannot be inspected safely is treated as an opaque shared value. Hook data is exposed through recursively read-only TypeScript views, but it is not frozen at runtime; opaque values typed as `unknown` remain the consumer's responsibility.

`onReadPage.itemCount` is the fetched page length. On successful cycles, `onReadCycleCompleted.items` contains eligible records from fully handled pages, including explicit exclusions and overlap replays, and `itemCount` equals its length. Failed cycles report an empty `items` array and omit `itemCount`. These values are not newly enqueued-process counts.

Steps are delivered in order before the corresponding success or failure hook. Status transitions preserve their emission order within each read job and each processing queue, and `onQueryAdded` precedes `onReadInit`. Replacing hooks affects the next dispatched event, not an event already being dispatched.

A failure in `onProcessSucceeded` does not retry a handler that already succeeded. Persistence hooks should remain idempotent and observe `onHookError`.

## Singleton lifecycle

`HederaEventSync.create()` uses a process-local singleton by default. If an active singleton already exists, another call returns that instance; the later call's loader, persistence callback, REST client, hook-failure policy, and defaults are not applied. Hooks are replaced separately with `setHooks()` on the returned instance. This prevents separate modules in the same process from accidentally running independent synchronization engines.

The guarded singleton remains reserved until shutdown has completely settled, even if its caller has already received `LifecycleTimeoutError`. A guarded `create()` called during shutdown waits for the underlying work. If disposal completes, concurrent callers create or return the single replacement instance. If shutdown aborts before disposal (for example, a checkpoint callback finishes after its deadline), they receive the retained instance once the operation settles; inspect its lifecycle state and explicitly recover or retry shutdown. After `await sync.shutdown()` resolves, a later guarded `create()` constructs a new instance normally.

For an application that deliberately manages multiple independent engines, pass `singletonGuard: false` to every such call:

```ts
const isolatedSync = await HederaEventSync.create({
  restClient,
  querySource: () => queries,
  singletonGuard: false,
});
```

## Lifecycle safety

Lifecycle controls must not be called from an active package-invoked loader, persistence callback, hook, handler, normalizer, resolver, log filter, or `read.until` callback belonging to the same synchronizer, including asynchronous work still executing in that callback's context. The operation could otherwise wait for its own callback. Such calls reject with `LifecycleReentrancyError` even if you omit `await` or use `void`. External callers can request lifecycle control while callbacks are running; the operation waits for the required work to settle.

If callback-driven control is required, schedule it after the callback returns and handle the resulting promise.

Instance-wide lifecycle work is serialized by a gate. Conflicting lifecycle or per-query control calls reject with `LifecycleBusyError`; retry only after the active operation settles. A timed-out callback retains ownership until it settles, so timeout cannot create overlapping generations.

Replacement holds new work and lets active reads, handlers, and ordered hooks settle before collecting processed checkpoints and invoking `beforeReplace`. Queues are discarded only after that boundary succeeds. Successful restart/reload preserves the administrative read and processing holds independently, including for new queries. A failure after entering replacement holds both sides for recovery; partial replacement also sets `recoveryRequired`, which blocks every global resume scope until a successful restart/reload.

`pauseAll` and `resumeAll` accept the exported `GlobalPauseOptions`, whose `scope` uses `GlobalPauseScope`: `"read"`, `"process"`, or `"both"` (default). Read-only pause cancels and settles active reads while queued processing may drain. Processing-only pause waits for the active handler and ordered processing observers; readers remain eligible until capacity/backpressure applies. Both-scope pause installs both gates before waiting. Each administrative hold is independent: `pauseAll(); resumeAll({ scope: "read" })` leaves processing held. Global resume does not clear independent manual/failure pauses, failure counters, predicate pauses, or capacity restrictions.

`LifecycleState.globalReadPaused` and `.globalProcessPaused` report effective global holds: the corresponding administrative hold **or** the lifecycle maintenance hold. `globalPaused` is their conjunction, so it is false during a single-side pause; `maintenanceHeld` exposes the maintenance hold separately. These fields do not replace per-query statuses. An active pause/resume `operation` includes its resolved `scope` alongside `name`, epoch-millisecond `startedAt`, and `timedOut`. See [lifecycle coordination](lifecycle.md#read-and-processing-holds) for examples and recovery rules.

Application-callback cancellation is cooperative. `shutdown()` first holds new work and waits for active handlers and observers to settle before disposal; it does not force-cancel a handler to meet the caller's deadline. Use `timeoutMs` (1 through 2,147,483,647 milliseconds) for caller-visible lifecycle deadlines, observe provided abort signals, and give external services their own deadlines. A hung handler may keep the lifecycle gate blocked after the caller times out; inspect `getLifecycleState()` and the query statuses. Do not launch another in-process owner for that work.

Pause and resume methods fulfill only after reaching their requested state. If a concurrent failure or stop wins the transition, the method rejects with the terminal state and its recorded reason instead of falsely reporting success.

All four per-query pause/resume controls reject while shutdown is in progress and after the synchronizer has closed. If shutdown fails before disposal, wait for the active operation to settle and inspect the retained instance's lifecycle state before recovery. On an active instance, a control call for a missing query ID remains a no-op.

## Advanced public APIs

### Query definition helper

`define(query)` returns a readonly one-element tuple, and `define(queries)` preserves a readonly array's inferred query types. It only normalizes the container shape: it does not validate, copy, freeze, register, or start the queries. Validation happens through `validateQuery()`, `start()`, or `reloadQueries()`.

### Standalone log processing

`new LogEventProcessor(runtime)` requires a `LogProcessorRuntime`: `{ registry, restClient, network, provider }`, with optional `queryId` and `now`. `restClient` is the configured `HederaRestClient`, not the package's `RestClient` facade. An omitted `queryId` becomes `null` in handler context; `now` defaults to `Date.now`.

| Method | Result |
| --- | --- |
| `prepare(log, options)` | `Promise<PreparedLogProcess \| null>`; decode, resolve and normalize without executing the handler |
| `execute(prepared, options?)` | `Promise<void>`; invoke the handler captured by preparation |
| `prepareAndExecute(log, prepareOptions, executionOptions?)` | Prepare and execute immediately; return the prepared record or `null` |

`log` must already be a normalized `SyncContractLog`; the [REST facade](#low-level-rest-facade) supplies this shape. Preparation options (`LogPrepareOptions`) require `abi` and accept `contract`, `skipEventNames`, `logFilter`, `contractResolver`, and `signal`. Unlike a managed query's address-only resolver, this standalone `contractResolver(log)` receives the complete read-only normalized log. Its result overlays the base `contract` metadata; an absent address falls back to the source log's address. A `false` result from `logFilter` or exclusion of a decoded event returns `null`; a thrown/rejected filter error propagates.

Execution options (`LogExecutionOptions`) accept `signal` and `addStep`. A `PreparedLogProcess` contains `{ eventName, data, rawLog, contract, handler }`; it is an in-process object containing a handler reference, not a portable durable queue record.

This function uses the registry and ABI from the [typed transfer example](queries.md#single-contract-events-with-types):

```ts
import { HederaRestClient, LogEventProcessor, type SyncContractLog } from "@davincigraph/hedera-event-sync";

const processor = new LogEventProcessor({
  registry: sync.registry,
  restClient: new HederaRestClient({ defaultProvider: "public", defaultNetwork: "testnet" }),
  provider: "public",
  network: "testnet",
});

async function processLog(log: SyncContractLog, signal: AbortSignal) {
  return processor.prepareAndExecute(
    log,
    { abi: transferAbi, signal },
    { signal, addStep: (step) => { console.log(step.title); } },
  );
}
```

Standalone processing supplies no polling, queue, deduplication, retries, checkpoints, hooks, or lifecycle coordination. The caller owns those responsibilities and catches rejected preparation/execution promises. Cancellation is cooperative; supplying a signal does not forcibly terminate application callbacks.

### Low-level REST facade

`new RestClient(client, options?)` wraps a configured `HederaRestClient`. `RestClientOptions` contains `{ debug?: boolean }` (default `false`); the underlying client remains available as the readonly `client` property. The facade does not own or close it.

| Method | Parameter type and required fields | Result |
| --- | --- | --- |
| `fetchGlobalLogs(params)` | `GlobalLogsParams`: `provider`, `network`, `cursor`, `batchSize` | `Promise<GlobalContractLogsPage>` |
| `fetchContractLogs(params)` | `ContractLogsParams`: the same fields plus `address` | `Promise<SyncContractLogsPage>` |
| `fetchTransactions(params)` | `RestClientTransactionsParams`: `provider`, `network`, `accountId`, `timestampGte`, `pageSize` | `Promise<SyncTransactionsPage>` |

All methods accept optional `timestampLte` and `signal`. Log methods additionally accept `topics` and `timestampGte`, defaulting the lower timestamp to `cursor.timestamp`. Transaction options also accept `result` and `transactionType` with the same endpoint-filter defaults described above. Requests are ascending and use inclusive timestamp bounds. The facade does **not** exclude records at `cursor.index`; managed readers apply exclusive-cursor filtering separately.

Responses contain validated normalized records and the REST client's continuation API. `page.next.url()` returns the next URL or `null`, and `await page.next({ signal })` returns the next normalized page or `null`. A first page may expose `syncWindowEnd` for its bounded time window. Topic-filtered log requests are bounded to the usable seven-day range even when a larger upper range was requested. The facade alone does not advance to the next window or implement overlap replay.

### Error classes

All exported errors extend `Error` and set their corresponding `name`. They can be imported from the package root:

| Class | Meaning / additional fields |
| --- | --- |
| `DecodeError` | A log cannot be decoded under the supported ABI rules |
| `NormalizationError` | A log handler's normalizer threw or rejected |
| `HandlerError` | A suitable concrete handler is missing |
| `LifecycleReentrancyError` | Control was invoked from an active callback; exposes `operation` and `callback: { kind, name, queryId }` |
| `LifecycleBusyError` | Conflicting control is still active; exposes `operation`, `queryId` (`-1` for instance-wide conflicts), and `activeOperation` |
| `LifecycleTimeoutError` | A caller deadline expired; exposes `operation` and `timeoutMs`, while underlying work can remain active |

Not every rejection uses one of these classes. Configuration failures, transport errors, filters/resolvers, and application handlers can throw other values; catch errors as `unknown`. Stored audit errors are serialized diagnostics rather than instances of these classes.

## Package exports

The root export contains the synchronizer, handlers, hooks, query and domain types, normalized Mirror Node types, errors, `HederaRestClient`, and the package-owned REST facade. Supported subpath exports are:

- `@davincigraph/hedera-event-sync/types/config`
- `@davincigraph/hedera-event-sync/types/domain`
- `@davincigraph/hedera-event-sync/core/logs/LogEventProcessor`

Import type-only contracts with `import type`. Useful groups include:

| Area | Exported types |
| --- | --- |
| Construction and lifecycle | `HederaEventSyncOptions`, `QuerySource`, `QuerySourceContext`, `BeforeReplaceContext`, `LifecycleOperationOptions`, `LifecycleOperation`, `LifecycleState`, `GlobalPauseOptions`, `GlobalPauseScope` |
| Query configuration | `QueryConfig`, `RuntimeQueryConfig`, `QueryConfigOf<Kind>`, `RuntimeQueryConfigOf<Kind>`, `QueryStartState`, `QueryKind`, `QueryStatus`, `ReadCommon`, `ReadConsistency`, `ProcessBase`, `ContractMeta`, source-specific parameter types |
| Inspection and history | `ListQueriesOptions`, `Page<T>`, `Cursor`, `ProcessedCheckpoint`, `Process`, `ProcessWithoutPayload`, `ProcessTry`, `ProcessStep`, `ReadCycleAudit` |
| Handler callbacks | `LogHandler`, `TransactionHandler`, `HandlerContext`, `LogHandlerContext`, `TransactionHandlerContext`, `HandlerKey`, `ProcessHandler` |
| Hook callbacks | `SyncHooks`, `SyncHookName`, `HookFailure`, `HookFailurePolicy`, `HookFailureScope`, `QueryHookContext`, `ReadCycleResult`, `ReadonlyCallbackValue<T>` |
| Normalized Mirror Node data | `SyncContractLog`, `SyncContractLogsPage`, `SyncTransaction`, `SyncTransactionsPage`, `SyncReadItem` |

`NetworkName`, `ProviderName`, `TransactionResultFilter`, and `TransactionTypes` are re-exported from the REST dependency. Other REST-client configuration/error types belong to `@davincigraph/hedera-rest-client` and are not all re-exported here. Internal jobs, queues, stores, and utilities have no supported deep-import paths.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Nothing appears | Install hooks and handlers before `start()`; verify the provider/network, real contract/account, ABI, and topic values. Allow for startup delay, polling cadence, finality lag, and Mirror Node indexing. |
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

## Safe query editing

`validateQuery(config, initialState?)` returns a normalized detached configuration without registering it, calling hooks, or making network requests. `QueryConfig.initialState` supplies initial per-side `RUNNING`/`PAUSED` states; transition and terminal states are rejected. Validation does not prove provider reachability or handler registration.

Write a complete candidate configuration to your application's source and call `reloadQueries()`. The engine validates the complete catalog before disruption, preserves per-query pause ownership and safe processed checkpoints, and awaits `beforeReplace` before releasing old queued data. A loader/persistence error must propagate; do not convert it into an empty catalog or swallow it in the persistence adapter.

The package does not transact with application storage. Durable editors must serialize source writes, enforce authorization, and implement transaction/rollback/recovery policy in their source and `beforeReplace` adapter. Persist timestamp strings and indexes without converting fractional timestamps to numbers. Never save `getReadStatus(id).cursor` as the processed checkpoint.

See [lifecycle coordination](lifecycle.md) for static/database/file loaders and failure behavior, and [migration from 1.x](migration-v2.md) for removed APIs.
