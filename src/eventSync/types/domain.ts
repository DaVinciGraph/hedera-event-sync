import type { ContractMeta } from "./config";
import type { SyncReadItem } from "./mirror";

type OpaqueCallbackObject = Date | RegExp | Error | PromiseLike<unknown> | ArrayBuffer | ArrayBufferView;
type CallbackPrimitive = string | number | bigint | boolean | symbol | null | undefined;

/**
 * Recursively read-only view used for package-owned callback data.
 *
 * Functions retain their exact callable type, keyed collections expose their
 * native read-only interfaces, and opaque platform objects are left intact.
 * Values represented as `unknown` are intentionally not inspected. Keeping
 * this utility scoped to the package's plain data-transfer shapes avoids
 * structurally rewriting client APIs or application class instances.
 */
export type ReadonlyCallbackValue<T> =
	T extends CallbackPrimitive ? T
		: T extends (...args: never[]) => unknown ? T
		: T extends ReadonlyMap<infer K, infer V> ? ReadonlyMap<ReadonlyCallbackValue<K>, ReadonlyCallbackValue<V>>
			: T extends ReadonlySet<infer V> ? ReadonlySet<ReadonlyCallbackValue<V>>
				: T extends OpaqueCallbackObject ? T
					: T extends readonly unknown[] ? { readonly [K in keyof T]: ReadonlyCallbackValue<T[K]> }
						: T extends object ? { readonly [K in keyof T]: ReadonlyCallbackValue<T[K]> }
							: T;

/** Lifecycle state of an individual queued source record. */
export type ProcessStatus = "Queued" | "Processing" | "Processed" | "Failed";

/** In-memory representation of a Mirror Node record and its processing history. */
export type Process = {
	/** Runtime-local process identifier. */
	id: number;
	/** Stable identity of the source mirror record within this query. */
	sourceKey: string;
	queryId: number;
	/** Decoded event name for logs or Mirror Node transaction type for transactions. */
	eventName: string;
	contract?: ContractMeta;
	/** Normalized log data or the normalized transaction record. */
	data: unknown;
	/** Normalized source record supplied to handler context as `raw`. */
	rawData: unknown;
	/** Source timestamp used by checkpoints and ordering. */
	pointerTS: string;
	/** Source-position tie-breaker used with `pointerTS`. */
	pointerIDX: number;
	status: ProcessStatus;
	/** Unix time in milliseconds when the source record was prepared. */
	timeOfEmit: number;
	/** ISO timestamp when the current or latest attempt started. */
	startTs?: string;
	/** Duration of the current or latest completed attempt, in seconds. */
	totalDurationSec?: number;
	/** Unix creation time in milliseconds. */
	createdAt: number;
	/** Unix last-update time in milliseconds. */
	updatedAt: number;
	/** Ordered processing attempts; newly queued processes expose an empty array. */
	tries: ProcessTry[];
};

/** Process metadata returned when payload inclusion is not explicitly requested. */
export type ProcessWithoutPayload = Omit<Process, "data" | "rawData">;

/** State of one handler execution attempt. */
export type ProcessTryOutcome = "PROCESSING" | "FAILED" | "SUCCEEDED";

/** Timing, outcome, error, and ordered diagnostics for a handler attempt. */
export type ProcessTry = {
	id: number;
	processId: number;
	number: number;
	/** ISO timestamp when this attempt started. */
	startTs: string;
	/** Present only after the attempt has reached a terminal outcome. */
	endTs?: string;
	/** Attempt duration in seconds; zero while newly started. */
	durationSec: number;
	outcome: ProcessTryOutcome;
	/** JSON-serialized error name and message for a failed attempt. */
	error?: string;
	/** Ordered diagnostics; newly started attempts expose an empty array. */
	steps: ProcessStep[];
};

/** Severity assigned to a processing diagnostic step. */
export type ProcessStepType = "Info" | "Warning" | "Error" | "Success";
/** Accepted step severity, including lowercase convenience inputs. */
export type ProcessStepInputType = ProcessStepType | Lowercase<ProcessStepType>;

/** Ordered application diagnostic attached to a processing attempt. */
export type ProcessStep = {
	id: number;
	tryId: number;
	orderNo: number;
	title: string;
	desc?: string;
	type: ProcessStepType;
	data?: unknown;
	/** ISO timestamp when this step was created. */
	createdTs: string;
	/** Seconds since the preceding step; the first step reports zero. */
	durationSec: number;
};

/** Canonical Mirror Node position ordered by timestamp and then index. */
export type Cursor = { timestamp: string; index: number };

/**
 * Monotonic, restart-safe cursor derived from successful processing progress.
 * It deliberately retains the configured overlap horizon so late-indexed
 * records remain eligible after a restart.
 */
export type ProcessedCheckpoint = Cursor;

/** Terminal audit record retained for the most recent read cycle. */
export type ReadCycleAudit = {
	queryId: number;
	/** Unix cycle-start time in milliseconds. */
	startedAt: number;
	/** Unix cycle-end time in milliseconds. */
	endedAt: number;
	success: boolean;
	/** JSON-serialized error name and message for a failed cycle. */
	error?: string;
	/** Present on successful cycles and equal to `items.length`. */
	itemCount?: number;
	/** Eligible normalized records from fully handled pages; omitted on failed cycles. */
	items?: ReadonlyCallbackValue<SyncReadItem>[];
};
