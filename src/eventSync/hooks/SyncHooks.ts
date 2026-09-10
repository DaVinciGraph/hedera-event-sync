import type { QueryConfig, QueryStatus, RuntimeQueryConfig } from "../types/config";
import type { Cursor, Process, ProcessedCheckpoint, ProcessTry, ProcessStep, ReadonlyCallbackValue } from "../types/domain";
import type { SyncReadItem, SyncTransaction } from "../types/mirror";

type HookQuery = ReadonlyCallbackValue<RuntimeQueryConfig>;
type HookCursor = ReadonlyCallbackValue<Cursor>;
type HookProcess = ReadonlyCallbackValue<Process>;
type HookProcessTry = ReadonlyCallbackValue<ProcessTry>;
type HookProcessStep = ReadonlyCallbackValue<ProcessStep>;

/** Minimal query identity included in a contained `HookFailure`. */
export type QueryHookContext = ReadonlyCallbackValue<{
	id: number;
	title: string;
	type: QueryConfig["type"];
	contract?: {
		address?: string;
		id?: string;
		type?: string;
		version?: string | number;
	};
}>;

/** Runtime response when a user hook rejects. */
export type HookFailurePolicy = "continue" | "pause" | "fail-fast";
/** Runtime subsystem affected by a hook failure. */
export type HookFailureScope = "query" | "read" | "process";
/** Name of a hook that can be reported through `onHookError`. */
export type SyncHookName = Exclude<keyof SyncHooks, "onHookError">;

/** Diagnostic information for a hook error contained by the dispatcher. */
export type HookFailure = ReadonlyCallbackValue<{
	hook: SyncHookName;
	hookIndex: number;
	policy: HookFailurePolicy;
	scope: HookFailureScope;
	query: QueryHookContext;
	error: unknown;
}>;

/** Terminal result delivered once for each read cycle. */
export type ReadCycleResult = ReadonlyCallbackValue<{
	/** On success, eligible normalized records including explicit exclusions and overlap replays; failures report an empty array. */
	items: SyncReadItem[];
	/** Ingestion cursor; this can be ahead of the processed checkpoint. */
	cursor: Cursor;
	startedAt: number;
	endedAt: number;
	success: boolean;
	/** JSON-serialized error name and message when the cycle failed. */
	error?: string;
	/** Present on successful cycles and equal to `items.length`; omitted on failure. */
	itemCount?: number;
}>;

/** Optional observers for query, read, and processing lifecycle events. */
export interface SyncHooks {
	/** Called when any other hook rejects. Errors from this observer are always contained. */
	onHookError?: (failure: HookFailure) => void | Promise<void>;

	/** Called after a query runtime is registered and before its read loop initializes. */
	onQueryAdded?: (query: HookQuery) => void | Promise<void>;
	/** Called after a query runtime has stopped and released its in-memory state. */
	onQueryRemoved?: (query: HookQuery) => void | Promise<void>;

	/** Called once before a query's read loop starts. */
	onReadInit?: (query: HookQuery, pollIntervalSeconds: number) => void | Promise<void>;
	/** Called when a read cycle starts; a fully drained window may require no request. */
	onReadCycleStarted?: (query: HookQuery, cursor: HookCursor, batchSize: number) => void | Promise<void>;
	/** Called for each fetched page with its raw record count, before preparation. */
	onReadPage?: (query: HookQuery, meta: ReadonlyCallbackValue<{ kind: "logs" | "transactions"; itemCount: number }>) => void | Promise<void>;
	/** Called exactly once with the terminal result of a read cycle. */
	onReadCycleCompleted?: (
		query: HookQuery,
		result: ReadCycleResult
	) => void | Promise<void>;
	/** Called when a read cycle or its `until` predicate fails. */
	onReadFailure?: (query: HookQuery, error: unknown, consecutiveFailures: number) => void | Promise<void>;
	/** Called when the configured read-failure threshold is reached. */
	onReadConsecutiveFailuresReached?: (query: HookQuery, consecutiveFailures: number) => void | Promise<void>;
	/** Called with the normalized transaction when no handler is registered, before policy is applied. */
	onUnhandledItem?: (
		query: HookQuery,
		item: ReadonlyCallbackValue<{ kind: "transaction"; itemType: string; sourceKey: string; raw: SyncTransaction }>
	) => void | Promise<void>;

	/** Called after a process enters the in-memory queue and before it becomes eligible for processing. */
	onProcessEnqueued?: (query: HookQuery, process: HookProcess) => void | Promise<void>;
	/** Called when a processing attempt starts. */
	onProcessStarted?: (query: HookQuery, process: HookProcess, _try: HookProcessTry) => void | Promise<void>;
	/** Called after a processing attempt fails. */
	onProcessTryFailed?: (query: HookQuery, process: HookProcess, _try: HookProcessTry, error: unknown) => void | Promise<void>;
	/** Called after a handler completes successfully. */
	onProcessSucceeded?: (query: HookQuery, process: HookProcess) => void | Promise<void>;
	/**
	 * Called after a successful process with the current restart-safe cursor.
	 * The cursor may remain unchanged while processing advances inside the
	 * configured replay horizon.
	 */
	onProcessedCheckpoint?: (query: HookQuery, cursor: ReadonlyCallbackValue<ProcessedCheckpoint>, process: HookProcess) => void | Promise<void>;
	/** Called when an item's configured consecutive-failure threshold is reached. */
	onProcessConsecutiveFailuresReached?: (query: HookQuery, process: HookProcess, attempts: number) => void | Promise<void>;
	/** Called for each ordered diagnostic step added during an attempt. */
	onStepCreated?: (query: HookQuery, process: HookProcess, _try: HookProcessTry, step: HookProcessStep) => void | Promise<void>;

	/** Called for ordered reader and processing-queue status transitions. */
	onQueryStatusChange?: (query: HookQuery, payload: ReadonlyCallbackValue<{ kind: "read" | "process"; status: QueryStatus }>) => void | Promise<void>;
}
