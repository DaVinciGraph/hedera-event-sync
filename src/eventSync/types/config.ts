import type { Abi } from "abitype";
import type { NetworkName, ProviderName, TransactionResultFilter } from "@davincigraph/hedera-rest-client";
import type { Cursor } from "./domain";

export type { NetworkName, ProviderName, TransactionResultFilter } from "@davincigraph/hedera-rest-client";

/** Lifecycle states shared by read jobs and processing queues. */
export type QueryQueueAndJobStatus = "PAUSED" | "RUNNING" | "PAUSING" | "RESUMING" | "FAILED" | "STOPPED";
/** Supported Mirror Node source shapes. */
export type QueryKind = "Multi-Contract-Logs" | "Single-Contract-Logs" | "Transactions";
/** Current lifecycle state and an optional operator-facing reason. */
export type QueryStatus = { name: QueryQueueAndJobStatus; info?: string };

/** One or more accepted values for a Mirror Node log-topic position. */
export type ContractLogTopicFilter = string | readonly string[];
/** Mirror Node topic positions; use undefined to leave a position unfiltered. */
export type ContractLogTopicsFilter = readonly [
	(ContractLogTopicFilter | undefined)?,
	(ContractLogTopicFilter | undefined)?,
	(ContractLogTopicFilter | undefined)?,
	(ContractLogTopicFilter | undefined)?,
];

/** Controls the safe and replayable time range used by a read job. */
export type ReadConsistency = {
	/** Keep the request's upper timestamp bound behind the local clock. Defaults to 15 seconds. */
	finalityLagSeconds?: number;
	/**
	 * Re-read this history horizon for records indexed late within it. Defaults
	 * to 30 seconds. Topic-filtered log queries require a value shorter than
	 * their usable seven-day Mirror Node request window.
	 */
	overlapSeconds?: number;
};

/** Settings shared by every Mirror Node reader. */
export type ReadCommon = {
	/** Provider selection, polling cadence, and per-cycle pagination limits. */
	fetch: {
		/** Network name configured in the supplied REST client. */
		network: NetworkName;
		/** Provider name configured in the supplied REST client. */
		restProvider: ProviderName;
		/** Base delay between completed read cycles; runtime jitter is applied. */
		pollIntervalSeconds?: number;
		/** Requested Mirror Node page size. */
		batchSize?: number;
		/** Maximum number of pages consumed in one read cycle. */
		maxPagesPerCycle?: number;
		/** Optional delay between successive pages in a cycle. */
		nextDelayMs?: number;
	};
	consistency?: ReadConsistency;
	/** Pauses this reader after the configured number of consecutive failures. */
	pauseAfterConsecutiveFailures: number;
	/** Synchronous pause predicate evaluated after a successful cycle with no unfinished replay sweep. */
	until?: (cursor: Readonly<Cursor>) => boolean;
};

/** Settings shared by log and transaction processing queues. */
export type ProcessBase = {
	/** Optional application-defined label retained in the resolved query. */
	context?: string;
	/** Pauses processing after this many consecutive failures for an item. */
	pauseAfterConsecutiveFailures: number;
	/** Maximum number of queued or actively processed records for this query. */
	maxQueuedProcesses?: number;
	/** Retention period for successfully processed records. */
	holdPeriod?: ProcessHoldPeriod;
	/** Delay between processing attempts. Defaults to zero. */
	nextDelayMs?: number;
};

/** Retention period for successfully processed in-memory records. */
export type ProcessHoldPeriod = "none" | "daily" | "weekly" | "monthly";

/** Metadata used to select specialized handlers and describe a contract. */
export type ContractMeta = {
	address?: string;
	id?: string;
	type?: string;
	version?: string | number;
};

/** Cursor, topic, and resolver inputs for a global contract-log query. */
export type MultiContractLogsParams = {
	/** At least one topic position must contain a filter. */
	topics: ContractLogTopicsFilter;
	/** Initial or restored source timestamp. Numbers are safe-integer seconds; use a string for fractional seconds. */
	timestamp?: string | number;
	/** Initial or restored tie-breaker for records sharing a timestamp. */
	index?: number;
	/** Resolves address-specific metadata before handler selection. */
	contractResolver?: (address: string) => ContractMeta | undefined | Promise<ContractMeta | undefined>;
};

/** Cursor and topic inputs for a single-contract log query. */
export type SingleContractLogsParams = {
	topics?: ContractLogTopicsFilter;
	/** Numbers are safe-integer seconds; use a string to preserve fractional seconds exactly. */
	timestamp?: string | number;
	index?: number;
};

/** Cursor and endpoint filters for an account or contract transaction query. */
export type TransactionsParams = {
	/** Mirror Node account/contract identifier used by the transactions endpoint. */
	accountId: string;
	/** Numbers are safe-integer seconds; use a string to preserve fractional seconds exactly. */
	timestamp?: string | number;
	/** Tie-breaker used when restoring a processed checkpoint. */
	index?: number;
	/** Defaults to "success". Set null to include both successful and failed transactions. */
	result?: TransactionResultFilter | null;
};

/** Canonical cursor and resolver state exposed for a global log runtime. */
export type RuntimeMultiContractLogsParams = Omit<MultiContractLogsParams, "timestamp" | "index"> & {
	timestamp: string;
	index: number;
};

/** Canonical cursor and filter state exposed for a single-contract log runtime. */
export type RuntimeSingleContractLogsParams = Omit<SingleContractLogsParams, "timestamp" | "index"> & {
	timestamp: string;
	index: number;
};

/** Canonical cursor, endpoint, and result filter exposed for a transaction runtime. */
export type RuntimeTransactionsParams = Omit<TransactionsParams, "timestamp" | "index" | "result"> & {
	timestamp: string;
	index: number;
	result: TransactionResultFilter | null;
};

type WithStatus<T> = T & { status: QueryStatus };

type RuntimeReadCommon = Omit<ReadCommon, "fetch"> & {
	fetch: Omit<ReadCommon["fetch"], "pollIntervalSeconds" | "batchSize" | "maxPagesPerCycle" | "nextDelayMs"> & {
		pollIntervalSeconds: number;
		batchSize: number;
		maxPagesPerCycle: number;
		nextDelayMs: number;
	};
	consistency: Required<ReadConsistency>;
};

type RuntimeProcess<T extends ProcessBase> = Omit<T, "maxQueuedProcesses" | "holdPeriod" | "nextDelayMs"> & {
	maxQueuedProcesses: number;
	holdPeriod: ProcessHoldPeriod;
	nextDelayMs: number;
};

type LogsProcessConfig = ProcessBase & {
	/** Known, successfully decoded events that should not be enqueued. */
	skipEventNames?: readonly string[];
};
/** Policy applied when a transaction type has no registered handler. */
export type UnhandledTransactionPolicy = "error" | "skip";
type TransactionsProcessConfig = ProcessBase & {
	skipTransactionTypes?: readonly string[];
	/** Missing handlers are errors by default; use "skip" only when intentional. */
	unhandledTransactionPolicy?: UnhandledTransactionPolicy;
};

type RuntimeLogsProcessConfig = RuntimeProcess<LogsProcessConfig> & {
	skipEventNames: readonly string[];
};

type RuntimeTransactionsProcessConfig = RuntimeProcess<TransactionsProcessConfig> & {
	skipTransactionTypes: readonly string[];
	unhandledTransactionPolicy: UnhandledTransactionPolicy;
};

type QueryIdentity = {
	id: number;
	title: string;
	icon?: string;
};

/**
 * User-facing discriminated union for all supported synchronization queries.
 * Log-query ABIs must describe the selected logs unambiguously: declarations
 * sharing an event signature must not have conflicting indexed-parameter layouts.
 */
export type QueryConfig =
	| {
			type: "Multi-Contract-Logs";
			read: ReadCommon;
			contract?: ContractMeta;
			params: MultiContractLogsParams;
			abi: Abi;
			process: LogsProcessConfig;
	  } & QueryIdentity
	| ({
			type: "Single-Contract-Logs";
			read: ReadCommon;
			contract: ContractMeta & { address: string };
			params: SingleContractLogsParams;
			abi: Abi;
			process: LogsProcessConfig;
	  } & QueryIdentity)
	| ({
			type: "Transactions";
			read: ReadCommon;
			contract?: ContractMeta;
			params: TransactionsParams;
			process: TransactionsProcessConfig;
	  } & QueryIdentity);

/** Validated query state with defaults, canonical identifiers, and statuses resolved. */
export type RuntimeQueryConfig =
	| {
			type: "Multi-Contract-Logs";
			id: number;
			title: string;
			icon?: string;
			read: WithStatus<RuntimeReadCommon>;
			contract?: ContractMeta;
			params: RuntimeMultiContractLogsParams;
			abi: Abi;
			process: WithStatus<RuntimeLogsProcessConfig>;
	  }
	| {
			type: "Single-Contract-Logs";
			id: number;
			title: string;
			icon?: string;
			read: WithStatus<RuntimeReadCommon>;
			contract: ContractMeta & { address: string };
			params: RuntimeSingleContractLogsParams;
			abi: Abi;
			process: WithStatus<RuntimeLogsProcessConfig>;
	  }
	| {
			type: "Transactions";
			id: number;
			title: string;
			icon?: string;
			read: WithStatus<RuntimeReadCommon>;
			contract?: ContractMeta;
			params: RuntimeTransactionsParams;
			process: WithStatus<RuntimeTransactionsProcessConfig>;
	  };

/** Selects the input configuration for a particular query kind. */
export type QueryConfigOf<T extends QueryConfig["type"]> = Extract<QueryConfig, { type: T }>;
/** Selects the resolved runtime configuration for a particular query kind. */
export type RuntimeQueryConfigOf<T extends RuntimeQueryConfig["type"]> = Extract<RuntimeQueryConfig, { type: T }>;

/** Preserves literal query types while normalizing one query or an array to an array. */
export function define<T extends QueryConfig>(q: T): readonly [T];
export function define<T extends QueryConfig>(q: readonly T[]): readonly T[];
export function define<T extends QueryConfig>(q: T | readonly T[]): readonly T[] {
	return (Array.isArray(q) ? q : [q]) as readonly T[];
}
