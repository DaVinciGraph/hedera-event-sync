import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import type { Abi, AbiEvent } from "abitype";
import { decodeEventLog, toEventSelector, type Hex } from "viem";
import type { ContractMeta, NetworkName } from "../../types/config";
import type { HandlerRegistry } from "../../handlers/HandlerRegistry";
import type { HandlerContext, LogHandler, LogHandlerContext } from "../../handlers/types";
import type { SyncContractLog } from "../../types/mirror";
import type { ReadonlyCallbackValue } from "../../types/domain";
import { DecodeError, HandlerError, NormalizationError } from "../../errors";
import { errorDetails } from "../../utils/error";
import { runInUserCallback, userCallbackOwnerOf } from "../../utils/userCallback";

/** Runtime dependencies required to prepare and execute contract logs. */
export type LogProcessorRuntime = {
	queryId?: number | null;
	registry: HandlerRegistry;
	restClient: HederaRestClient;
	network: NetworkName;
	provider: string;
	now?: () => number;
};

/** ABI, metadata, exclusions, and cancellation used while preparing one log. */
export type LogPrepareOptions = {
	/** ABI containing the non-anonymous event declaration for this log. */
	abi: Abi;
	/** Base metadata used during handler selection. */
	contract?: ContractMeta;
	/** Optional metadata resolver receiving a recursively read-only view of the normalized log. */
	contractResolver?: (
		log: ReadonlyCallbackValue<SyncContractLog>
	) => Pick<ContractMeta, "id" | "address" | "type" | "version"> | undefined | Promise<Pick<ContractMeta, "id" | "address" | "type" | "version"> | undefined>;
	/** Exact event names excluded only after successful decoding. */
	skipEventNames?: readonly string[];
	/** Cooperative signal forwarded to normalizer context. */
	signal?: AbortSignal;
};

/** Fully decoded, normalized, and handler-bound log ready for queueing. */
export type PreparedLogProcess = {
	eventName: string;
	data: unknown;
	rawLog: SyncContractLog;
	contract: ContractMeta;
	handler: LogHandler;
};

/** Per-attempt services supplied when executing a prepared log. */
export type LogExecutionOptions = {
	addStep?: HandlerContext["addStep"];
	/** Cooperative signal forwarded to handler context. */
	signal?: AbortSignal;
};

/**
 * Strictly decodes logs, resolves a handler, and normalizes decoded arguments
 * into a prepared process. Preparation performs no handler side effect and can
 * therefore complete for an entire page before any process is committed.
 */
export class LogEventProcessor {
	/** Creates a processor backed by the supplied registry and REST client. */
	constructor(private readonly runtime: LogProcessorRuntime) {}

	/** Returns `null` only for a successfully decoded event explicitly excluded by name. */
	async prepare(log: SyncContractLog, options: LogPrepareOptions): Promise<PreparedLogProcess | null> {
		const decoded = this.decodeWithAbi(log, options.abi);
		const eventName = decoded.eventName;
		if (options.skipEventNames?.includes(eventName)) return null;

		const resolved = options.contractResolver
			? await runInUserCallback(
				{ kind: "resolver", name: "contractResolver", queryId: this.runtime.queryId ?? null },
				() => options.contractResolver!(log),
				userCallbackOwnerOf(this.runtime)
			)
			: undefined;
		const resolvedContract = {
			...(options.contract ?? {}),
			...(resolved ?? {}),
		};
		const contractAddress = resolvedContract.address ?? options.contract?.address ?? this.resolveContractAddress(log);
		const contract = { ...resolvedContract, address: contractAddress };

		const handler = this.runtime.registry.resolveLog(eventName, contract.type, contract.version);
		if (!this.isConcreteHandler(handler)) {
			throw new HandlerError(`No handler registered for ${eventName} (contractType=${contract.type ?? "none"}, version=${contract.version ?? "none"})`);
		}

		let normalized: unknown;
		try {
			normalized = await runInUserCallback(
				{ kind: "normalizer", name: eventName, queryId: this.runtime.queryId ?? null },
				() => handler.normalize(decoded.args, this.contextFor(log, contract, { signal: options.signal })),
				userCallbackOwnerOf(this.runtime)
			);
		} catch (error) {
			throw new NormalizationError(errorDetails(error).message);
		}

		return {
			eventName,
			data: normalized,
			rawLog: log,
			contract,
			handler,
		};
	}

	/** Executes the handler already selected during preparation. */
	async execute(prepared: PreparedLogProcess, options: LogExecutionOptions = {}): Promise<void> {
		await runInUserCallback(
			{ kind: "handler", name: prepared.eventName, queryId: this.runtime.queryId ?? null },
			() => prepared.handler.handle(
				prepared.data,
				this.contextFor(prepared.rawLog, prepared.contract, {
					addStep: options.addStep,
					signal: options.signal,
				})
			),
			userCallbackOwnerOf(this.runtime)
		);
	}

	/** Prepares and immediately executes one log; intended for standalone use. */
	async prepareAndExecute(log: SyncContractLog, prepare: LogPrepareOptions, execute: LogExecutionOptions = {}): Promise<PreparedLogProcess | null> {
		const prepared = await this.prepare(log, prepare);
		if (!prepared) return null;
		await this.execute(prepared, execute);
		return prepared;
	}

	private decodeWithAbi(log: SyncContractLog, abi: Abi): { eventName: string; args: unknown } {
		try {
			const data = toHex(log.data ?? "0x");
			const topics = (log.topics ?? []).map((topic) => toHex(topic)) as [] | [Hex, ...Hex[]];
			const event = this.selectEvent(abi, topics);
			const decoded = decodeEventLog({
				// Decode the same supported declaration whose topic count was checked.
				// An anonymous entry sharing its selector must not shadow it. An empty
				// ABI leaves unknown signatures and missing topics to viem's errors.
				abi: event ? [event] : [],
				data,
				topics,
				// Reject partial argument decoding when required topics or data are
				// missing. Exact topic cardinality is verified separately above.
				strict: true,
			});
			if (!decoded.eventName) throw new Error("Decoded log did not contain an event name");
			return { eventName: decoded.eventName, args: decoded.args };
		} catch (error) {
			throw new DecodeError(errorDetails(error).message);
		}
	}

	/** Selects a supported declaration and verifies its exact indexed-topic count. */
	private selectEvent(abi: Abi, topics: readonly Hex[]): AbiEvent | undefined {
		const selector = topics[0]?.toLowerCase();
		if (!selector) return;
		const event = abi.find((item): item is AbiEvent => (
			item.type === "event"
			&& item.anonymous !== true
			&& toEventSelector(item).toLowerCase() === selector
		));
		if (!event) return;

		const expected = 1 + event.inputs.filter((input) => input.indexed).length;
		if (topics.length !== expected) {
			throw new Error(`Event ${event.name} requires exactly ${expected} topic${expected === 1 ? "" : "s"}; received ${topics.length}`);
		}
		return event;
	}

	private contextFor(log: SyncContractLog, contract: ContractMeta, extra: Pick<HandlerContext, "addStep" | "signal"> = {}): LogHandlerContext {
		return {
			queryId: this.runtime.queryId ?? null,
			restClient: this.runtime.restClient,
			network: this.runtime.network,
			provider: this.runtime.provider,
			contract,
			now: this.runtime.now ?? (() => Date.now()),
			raw: log,
			...extra,
		};
	}

	private resolveContractAddress(log: SyncContractLog): string | undefined {
		return typeof log.address === "string" && log.address ? log.address : undefined;
	}

	private isConcreteHandler(handler: LogHandler | undefined): handler is LogHandler {
		return !!handler && typeof handler.handle === "function" && typeof handler.normalize === "function";
	}
}

function toHex(value: string): Hex {
	return (value.startsWith("0x") ? value.toLowerCase() : `0x${value.toLowerCase()}`) as Hex;
}
