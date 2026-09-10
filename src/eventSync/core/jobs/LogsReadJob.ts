import type { ContractLogTopicsFilter, RuntimeQueryConfig } from "../../types/config";
import { ReadJob } from "./ReadJob";
import { maxCursor } from "../../utils/comparators";
import type { HandlerRegistry } from "../../handlers/HandlerRegistry";
import type { Process } from "../../types/domain";
import { ReplayPaginator } from "../../utils/ReplayPaginator";
import { DEFAULTS } from "../../constants";
import { SyncHooks } from "../../hooks/SyncHooks";
import { RestClient } from "../../rest/RestClient";
import type { SyncContractLog, SyncContractLogsPage } from "../../types/mirror";
import type { Cursor } from "../../types/domain";
import { LogEventProcessor } from "../logs/LogEventProcessor";
import type { StagedProcess } from "../queue/types";
import { normalizeHederaTimestamp } from "../../utils/timestamp";
import { associateUserCallbackOwner } from "../../utils/userCallback";

type LogsQueryCfg = Extract<RuntimeQueryConfig, { type: "Multi-Contract-Logs" | "Single-Contract-Logs" }>;
type CapturedLogItem = { log: SyncContractLog; cursor: Cursor; sourceKey: string };

export class LogsReadJob extends ReadJob {
	private queryTyped: LogsQueryCfg;
	private readonly pagination = new ReplayPaginator<SyncContractLogsPage>();

	constructor(
		query: LogsQueryCfg,
		private readonly rest: RestClient,
		private readonly registry: HandlerRegistry,
		private readonly commitPage: (items: readonly StagedProcess[], signal: AbortSignal) => Promise<Process[]>,
		private readonly pruneSourceKeysBefore: (timestamp: string) => void,
		readonly getQueueSize: () => number,
		readonly backpressureThreshold: number = DEFAULTS.maxQueuedProcessesPerQuery,
		readonly hooks: SyncHooks,
		callbackOwner?: object
	) {
		super(query, getQueueSize, backpressureThreshold, hooks, callbackOwner);
		this.queryTyped = query;
	}

	protected async cycle(signal: AbortSignal): Promise<SyncContractLog[]> {
		const collected: SyncContractLog[] = [];
		await this.pagination.run({
			signal,
			maxPages: this.maxPagesPerCycle,
			replayEnabled: this.query.read.consistency.overlapSeconds > 0,
			replayStart: this.replayStartTimestamp(),
			safeHead: this.safeHeadTimestamp(),
			fetch: (start, end) => this.fetchFirstPage(signal, start, end),
			cursorTimestamp: () => this.cursor.timestamp,
			advanceTo: (timestamp) => { this.cursor = maxCursor(this.cursor, { timestamp, index: 0 }); },
			consume: async (page, lowerBound) => {
				const items = page.logs;
				await this.hooks.onReadPage?.(this.queryTyped, { kind: "logs", itemCount: items.length });
				if (signal.aborted) return false;

				// Keep synchronization identity independent from mutations made through
				// the intentionally identity-preserving raw-log callback context.
				const eligible = items
					.filter((log) => this.isEligibleCursor({ timestamp: log.timestamp, index: log.index }, lowerBound))
					.map((log): CapturedLogItem => ({
						log,
						cursor: { timestamp: log.timestamp, index: log.index },
						sourceKey: this.logSourceKey(log),
					}));
				const pageHandled = await this.ingestLogs(eligible, signal);
				if (!pageHandled) return false;

				collected.push(...eligible.map(({ log }) => log));
				for (const { cursor } of eligible) {
					this.cursor = maxCursor(this.cursor, cursor);
				}
				if (this.fetchDelayMs > 0) {
					await this.waitWithAbort(this.fetchDelayMs, signal);
				}
				return !signal.aborted;
			},
		});
		this.pruneSourceKeysBefore(this.getReplayFloor() ?? this.replayStartTimestamp());

		return collected;
	}

	override getReplayFloor(): string | undefined {
		return this.pagination.getReplayFloor();
	}

	private async fetchFirstPage(signal: AbortSignal, timestampGte: string, timestampLte: string): Promise<SyncContractLogsPage | null> {
		if (signal.aborted) return null;

		const { restProvider, network, batchSize } = this.queryTyped.read.fetch;
		const topics = this.queryTyped.params.topics as ContractLogTopicsFilter | undefined;
		const cursor = this.cursor;

		if (this.queryTyped.type === "Multi-Contract-Logs") {
			if (!topics?.some((topic) => typeof topic === "string" ? topic.length > 0 : Array.isArray(topic) && topic.length > 0)) {
				throw new Error("Multi-Contract-Logs requires at least one topic filter");
			}
			return this.rest.fetchGlobalLogs({
				provider: restProvider,
				network,
				topics,
				cursor,
				timestampGte,
				timestampLte,
				batchSize,
				signal,
			});
		} else {
			const address = this.queryTyped.contract?.address;
			if (!address) throw new Error("Single-Contract-Logs requires contract.address");
			return this.rest.fetchContractLogs({
				provider: restProvider,
				network,
				address,
				topics,
				cursor,
				timestampGte,
				timestampLte,
				batchSize,
				signal,
			});
		}
	}

	private async ingestLogs(items: readonly CapturedLogItem[], signal: AbortSignal): Promise<boolean> {
		const skipEventNames = this.queryTyped.process.skipEventNames ?? [];
		const processor = new LogEventProcessor(associateUserCallbackOwner({
			queryId: this.queryTyped.id,
			registry: this.registry,
			restClient: this.rest.client,
			network: this.query.read.fetch.network,
			provider: this.query.read.fetch.restProvider,
			now: () => Date.now(),
		}, this.callbackOwner));
		const staged: StagedProcess[] = [];
		const contractResolver = this.queryTyped.type === "Multi-Contract-Logs" ? this.queryTyped.params.contractResolver : undefined;

		for (const { log, cursor, sourceKey } of items) {
			if (signal.aborted) return false;

			const prepared = await processor.prepare(log, {
				abi: this.queryTyped.abi,
				contract: this.queryTyped.contract,
				contractResolver: contractResolver ? (rawLog) => contractResolver(rawLog.address) : undefined,
				skipEventNames,
				signal,
			});
			if (signal.aborted) return false;
			if (!prepared) continue;

			const now = Date.now();
			staged.push({ process: {
				queryId: this.queryTyped.id,
				sourceKey,
				eventName: prepared.eventName,
				contract: prepared.contract,
				data: prepared.data,
				rawData: log,
				pointerTS: cursor.timestamp,
				pointerIDX: cursor.index,
				status: "Queued",
				timeOfEmit: now,
				createdAt: now,
				updatedAt: now,
			}, handler: prepared.handler });
		}
		if (signal.aborted) return false;
		if (staged.length === 0) return true;

		await this.commitPage(staged, signal);
		// If shutdown raced the commit, replay the page. Store-level source-key
		// deduplication makes that retry safe and prevents a partial-page gap.
		return !signal.aborted;
	}

	private logSourceKey(log: SyncContractLog): string {
		// Mirror Node identifies a contract log by consensus timestamp and index.
		// Excluding provider metadata keeps replay/failover deduplication stable.
		return ["log", this.queryTyped.id, normalizeHederaTimestamp(log.timestamp), log.index].join(":");
	}
}
