import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import type { NetworkName, ProviderName } from "../types/config";
import type { SyncContractLog, SyncTransaction } from "../types/mirror";
import type { ProcessStepInputType, ReadonlyCallbackValue } from "../types/domain";

/** Runtime services and source metadata supplied to a user handler. */
export type HandlerContext<Raw extends SyncContractLog | SyncTransaction = SyncContractLog | SyncTransaction> = {
	/** Query that produced the record, or `null` for standalone processor use. */
	readonly queryId: number | null;
	/** The configured client, available for related Mirror Node requests. */
	readonly restClient: HederaRestClient;
	readonly network: NetworkName;
	readonly provider: ProviderName;
	/** Contract metadata resolved before the process entered the queue. */
	readonly contract: ReadonlyCallbackValue<{ id?: string; address?: string; type?: string; version?: string | number }>;
	/** Runtime clock source; standalone processors may inject it for deterministic tests. */
	readonly now: () => number;
	/** Adds an ordered diagnostic step to the active processing attempt. */
	readonly addStep?: (step: { title: string; desc?: string; type?: ProcessStepInputType; data?: unknown }) => void;
	/** Recursively read-only view of the normalized source record. */
	readonly raw: ReadonlyCallbackValue<Raw>;
	/** May be aborted when lifecycle control or failure policy cancels this callback's work. */
	readonly signal?: AbortSignal;
};

/**
 * Normalizes ABI-decoded event arguments and processes the resulting data.
 * `DecodedArguments` describes the event arguments produced by the query ABI;
 * callers must register a handler whose argument shape matches that ABI.
 */
export interface LogHandler<N = unknown, DecodedArguments = unknown> {
	/** Maps ABI-decoded arguments to the data retained by the queued process. */
	normalize: (decodedArguments: DecodedArguments, ctx: HandlerContext<SyncContractLog>) => N | Promise<N>;
	/** Executes the application side effect for a normalized event. */
	handle: (data: N, ctx: HandlerContext<SyncContractLog>) => void | Promise<void>;
}

type NamedSyncTransaction<Name extends SyncTransaction["name"]> = SyncTransaction & { readonly name: Name };

/** Processes a Mirror Node transaction whose existing record fields require no normalization stage. */
export interface TransactionHandler<Name extends SyncTransaction["name"] = SyncTransaction["name"]> {
	/**
	 * Receives the standard transaction record with its `name` narrowed to the
	 * registration key. The function property preserves strict parameter
	 * variance without permitting handler-specific fields that do not exist at
	 * runtime.
	 */
	handle: (
		data: ReadonlyCallbackValue<NamedSyncTransaction<Name>>,
		ctx: HandlerContext<NamedSyncTransaction<Name>>
	) => void | Promise<void>;
}

export type LogHandlerContext = HandlerContext<SyncContractLog>;
export type TransactionHandlerContext<Name extends SyncTransaction["name"] = SyncTransaction["name"]> =
	HandlerContext<NamedSyncTransaction<Name>>;

export type ProcessHandler = LogHandler | TransactionHandler;

/** Registry key for a general event or a contract-type/version specialization. */
export type HandlerKey =
	| `${string}@${string}`
	| `${string}@${string}:${string | number}`
	| (string & {});
