import type { RuntimeQueryConfig } from "../../types/config";
import { ReadJob } from "./ReadJob";
import { maxCursor } from "../../utils/comparators";
import type { HandlerRegistry } from "../../handlers/HandlerRegistry";
import type { Process } from "../../types/domain";
import { ReplayPaginator } from "../../utils/ReplayPaginator";
import { DEFAULTS } from "../../constants";
import { SyncHooks } from "../../hooks/SyncHooks";
import { RestClient } from "../../rest/RestClient";
import type { SyncTransaction, SyncTransactionsPage } from "../../types/mirror";
import type { Cursor } from "../../types/domain";
import type { StagedProcess } from "../queue/types";
import { HandlerError } from "../../errors";
import { normalizeHederaTimestamp } from "../../utils/timestamp";

type TxQueryCfg = Extract<RuntimeQueryConfig, { type: "Transactions" }>;
type CapturedTransactionItem = { transaction: SyncTransaction; cursor: Cursor };

export class TransactionsReadJob extends ReadJob {
	private queryTyped: TxQueryCfg;
	private readonly pagination = new ReplayPaginator<SyncTransactionsPage>();

	constructor(
		query: TxQueryCfg,
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

	protected async cycle(signal: AbortSignal): Promise<SyncTransaction[]> {
		const collected: SyncTransaction[] = [];
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
				const batch = page.transactions;
				await this.hooks.onReadPage?.(this.queryTyped, { kind: "transactions", itemCount: batch.length });
				if (signal.aborted) return false;

				// The unhandled-item hook receives the raw transaction, so cursor state
				// must not be derived from that object after the callback completes.
				const eligible = batch
					.filter((tx) => this.isEligibleCursor({ timestamp: tx.consensus_timestamp, index: tx.nonce }, lowerBound))
					.map((transaction): CapturedTransactionItem => ({
						transaction,
						cursor: { timestamp: transaction.consensus_timestamp, index: transaction.nonce },
					}));
				const pageHandled = await this.ingestTransactions(eligible, signal);
				if (!pageHandled) return false;

				collected.push(...eligible.map(({ transaction }) => transaction));
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

	private async fetchFirstPage(signal: AbortSignal, timestampGte: string, timestampLte: string): Promise<SyncTransactionsPage | null> {
		if (signal.aborted) return null;
		const { restProvider, network, batchSize } = this.queryTyped.read.fetch;
		const accountId = this.queryTyped.params.accountId;
		return this.rest.fetchTransactions({
			provider: restProvider,
			network,
			accountId,
			timestampGte,
			timestampLte,
			pageSize: batchSize,
			result: this.queryTyped.params.result,
			signal,
		});
	}

	private async ingestTransactions(items: readonly CapturedTransactionItem[], signal: AbortSignal): Promise<boolean> {
		const skipTypes = this.queryTyped.process.skipTransactionTypes;
		const staged: StagedProcess[] = [];

		for (const { transaction: tx, cursor } of items) {
			if (signal.aborted) return false;
			if (skipTypes?.includes(tx.name)) continue;

			const sourceKey = this.transactionSourceKey(tx);
			const handler = this.registry.resolveTransaction(tx.name);
			if (!handler) {
				await this.hooks.onUnhandledItem?.(this.queryTyped, { kind: "transaction", itemType: tx.name, sourceKey, raw: tx });
				if (signal.aborted) return false;
				if ((this.queryTyped.process.unhandledTransactionPolicy ?? "error") === "skip") continue;
				throw new HandlerError(`No transaction handler registered for ${tx.name}`);
			}

			const now = Date.now();
			staged.push({ process: {
				queryId: this.queryTyped.id,
				sourceKey,
				eventName: tx.name,
				data: tx,
				rawData: tx,
				pointerTS: cursor.timestamp,
				pointerIDX: cursor.index,
				status: "Queued",
				timeOfEmit: now,
				createdAt: now,
				updatedAt: now,
			}, handler });
		}
		if (signal.aborted) return false;
		if (staged.length === 0) return true;

		await this.commitPage(staged, signal);
		// Replaying an ambiguously committed page is safe because source keys are
		// deduplicated; advancing past it would risk permanently omitting records.
		return !signal.aborted;
	}

	private transactionSourceKey(tx: SyncTransaction): string {
		// Consensus timestamp is the canonical Mirror Node transaction identity;
		// transaction IDs and nonces can be represented differently by providers.
		return ["transaction", this.queryTyped.id, normalizeHederaTimestamp(tx.consensus_timestamp)].join(":");
	}
}
