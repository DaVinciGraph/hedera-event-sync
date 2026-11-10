# Changelog

## 2.0.0 — 2026-11-10

This major release supersedes the unpublished 1.1.0 preparation below. See the [migration guide](docs/migration-v2.md), [lifecycle guide](docs/lifecycle.md), and [release checklist](docs/releasing.md).

### Breaking changes

- `querySource` is required and is the only public configuration interface. It returns the complete catalog, synchronously or asynchronously, and receives `{ signal, reason: "start" | "reload" }`. Construction does not load or start work; register handlers/hooks and call `start()`.
- Removed public `addQuery`, `updateQuery`, `removeQuery`, and `skipOnDuplication`. Change the application-owned catalog and call `reloadQueries()`. Distinct IDs with equivalent configurations are retained; duplicate IDs reject the entire candidate.
- New-query pauses belong on `QueryConfig.initialState`. Existing logical IDs retain independent pause ownership and failure counters through replacement. Source identity changes under an accepted ID are rejected; use a new ID.
- Restart/reload resume existing IDs from final processed checkpoints rather than loader cursors. In-memory history is reset and replay remains possible.
- Instance shutdown is graceful: it holds new work, waits for active callbacks, and confirms `beforeReplace` before disposal. Handlers must use bounded I/O rather than depend on shutdown aborting them; lifecycle deadlines do not force-stop user code.

### Added

- Optional `params.transactionType?: TransactionTypes` for transaction queries forwards one explicit category to the Mirror Node source filter. Omission preserves all types allowed by the existing account, timestamp, and result filters; the default `result: "success"` is unchanged. Handler registration, local skip rules, and unhandled-transaction errors remain independent. Adding, removing, or changing the filter changes source identity and requires a new query ID with a deliberate starting checkpoint. See the [transaction recipe](docs/queries.md#transactions), including the limits of type filtering for balance accounting.
- Operational filters for paginated `listQueries()`: independent `readStatus` and `processStatus` accept an exact status or readonly array of alternatives, while `search` matches configured titles, query IDs, contract IDs/addresses, and transaction account IDs using trimmed, case-insensitive literal substrings. Status filters reflect effective global holds. Filters combine with existing options before pagination and do not change synchronization; omitted options preserve existing behavior. See [query-list filtering](docs/reference.md#query-list-filtering).
- Instance-wide `pauseAll`, `resumeAll`, `restartQueries`, `reloadQueries`, explicit `start`, and `getLifecycleState`. Global pause/resume accepts independent `"read"`, `"process"`, or `"both"` scopes (default `"both"`) and optional `timeoutMs`, with exported `GlobalPauseScope` and `GlobalPauseOptions` types. Global holds preserve manual/failure/predicate/capacity pause ownership and remain independent across successful restart/reload and newly loaded queries.
- Scoped lifecycle inspection through `globalReadPaused`, `globalProcessPaused`, `maintenanceHeld`, and active pause/resume `operation.scope`. `globalPaused` means both effective sides are held. Read-only pause lets queued processing drain; processing-only pause lets readers continue until capacity backpressure. Both-scope pause installs both gates before waiting. Replacement failures retain the both-side recovery hold and partial-replacement resume guard.
- Awaited `beforeReplace` persistence boundary for final checkpoints before restart/reload/shutdown disposes old queues. Rejections keep work held and are never swallowed by observer policy.
- Lifecycle serialization, callback reentrancy protection, configurable deadlines and `LifecycleTimeoutError`. A timeout does not force-stop user code or permit overlapping generations.
- Static, database, and file loader recipes, migration instructions, and public-runtime/declaration/archive consumer checks. The guides cover effective dashboard statuses, lifecycle recovery, handler and hook policies, exported helpers, and standalone log preparation.
- Includes the contract-type list filtering, pre-decode log filters, callback typings, validation, and pause-reason preservation prepared for 1.1.0. Its imperative add/update/remove extensions are replaced by the lifecycle API above.

### Fixed

- Sparse `querySource` arrays are rejected during catalog validation, before startup or reload can hold existing work. Every position must contain a valid query; rejection preserves the accepted catalog, queued records, checkpoints, and independent global holds.
- Guarded `create()` waits for the underlying shutdown work even after its caller-visible deadline expires. Concurrent callers receive one replacement after completed disposal, or the retained instance after shutdown aborts before disposal; a timeout cannot expose an instance still being shut down.
- Direct `CompositeHooks` consumers can return a promise from `onFailure`. Rejections are contained like synchronous throws without delaying error reporting or subsequent hooks. The existing `continue`, `pause`, and `fail-fast` policies are unchanged.


## 1.1.0 — prepared / unreleased (2026-11-08)

This unpublished preparation is retained as history. Its compatible additions were carried into 2.0.0; its imperative lifecycle extensions were replaced. Follow the current [release checklist](docs/releasing.md) for 2.0.0.

### Added

- `validateQuery(config, initialState?)` validates and normalizes configurations without starting a runtime, invoking hooks, or requesting Mirror Node data. Optional initial `RUNNING`/`PAUSED` read and processing states on `addQuery` and `updateQuery` are applied before initialization; omitted states preserve the existing running defaults.
- `removeQuery(id, { returnCheckpoint: true })` returns the final restart-safe timestamp/index after in-flight work and ordered hooks settle and before queued records are disposed. Existing one-argument calls retain their `Promise<void>` API. Optional `preserveExistingPause` on `pauseRead`/`pauseProcessing` retains already-settling manual/failure pause reasons while preparing a configuration edit. These lifecycle additions were prepared on 2026-11-09; see [safe query editing](docs/reference.md#safe-query-editing).

- Optional `ListQueriesOptions.contractType?: string | null | undefined` for paginated `listQueries()`. Omitted or `undefined` includes all contract types. A string matches `contract.type` exactly and case-sensitively; `null` selects unspecified types (missing contract or type, `null`, empty, or whitespace-only type).
- Contract-type filtering combines with `network` and query-kind `type` using AND. Counts, pages, navigation flags, and page reclamping use the filtered result in insertion, ascending-ID, and descending-ID order. The no-argument overload and existing calls retain their behavior. This only filters inspection results; ingestion and processing are unchanged. See [query-list filtering](docs/reference.md#query-list-filtering).
- Optional `params.logFilter` for multi-contract log queries, plus standalone `LogPrepareOptions.logFilter`, admits source logs before ABI decoding. Return `false` to ignore an unrelated log, `true` to continue strict decoding, or throw to retry the page when admission is uncertain. The callback receives a frozen source snapshot and optional abort signal; accepted malformed logs still fail. Filters must remain read-only and replay-safe. See [queries and handlers](docs/queries.md) for cursor and persistence behavior.
