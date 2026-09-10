import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { TransactionsReadJob } from "../src/eventSync/core/jobs/TransactionsReadJob";
import { HookBase } from "../src/eventSync/hooks/HookBase";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { SyncTransaction, SyncTransactionsPage } from "../src/eventSync/types/mirror";
import type { StagedProcess } from "../src/eventSync/core/queue/types";
import type { Process } from "../src/eventSync/types/domain";

function query(policy?: "error" | "skip"): Extract<RuntimeQueryConfig, { type: "Transactions" }> {
	return {
		type: "Transactions",
		id: 5,
		title: "transaction ingestion",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 100, maxPagesPerCycle: 10, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 30 },
			pauseAfterConsecutiveFailures: 3,
			status: { name: "RUNNING" },
		},
		contract: { address: "0.0.123" },
		params: { accountId: "0.0.123", timestamp: "1.000000000", index: 0, result: "success" },
		process: {
			pauseAfterConsecutiveFailures: 3,
			maxQueuedProcesses: 100,
			holdPeriod: "daily",
			nextDelayMs: 0,
			skipTransactionTypes: [],
			unhandledTransactionPolicy: policy ?? "error",
			status: { name: "RUNNING" },
		},
	};
}

function tx(name: string, timestamp: string, nonce = 0): SyncTransaction {
	return {
		name,
		consensus_timestamp: timestamp,
		transaction_id: `0.0.123@${timestamp}`,
		transaction_hash: `hash-${timestamp}`,
		nonce,
		nft_transfers: [],
		token_transfers: [],
		transfers: [],
	} as unknown as SyncTransaction;
}

function page(transactions: SyncTransaction[]): SyncTransactionsPage {
	return {
		transactions,
		links: { next: null },
		next: Object.assign(async () => null, { url: () => null }),
		syncWindowEnd: "100.000000000",
	} as SyncTransactionsPage;
}

class ExposedTransactionsReadJob extends TransactionsReadJob {
	runCycle(signal: AbortSignal) {
		return this.cycle(signal);
	}
}

describe("transaction page ingestion", () => {
	it("fails the whole prepared page when any transaction has no handler", async () => {
		const registry = new HandlerRegistry();
		registry.registerTransaction("HANDLED", { async handle() {} });
		const transactions = [tx("HANDLED", "10.000000001"), tx("UNHANDLED", "11.000000001")];
		let commits = 0;
		let unhandled = 0;
		const job = new ExposedTransactionsReadJob(
			query(),
			{ fetchTransactions: async () => page(transactions) } as never,
			registry,
			async () => { commits++; return []; },
			() => {},
			() => 0,
			100,
			{ onUnhandledItem: () => void unhandled++ }
		);

		await assert.rejects(job.runCycle(new AbortController().signal), /No transaction handler registered for UNHANDLED/);
		assert.equal(commits, 0);
		assert.equal(unhandled, 1);
	});

	it("skips missing handlers only when the query explicitly requests it", async () => {
		const registry = new HandlerRegistry();
		registry.registerTransaction("HANDLED", { async handle() {} });
		const transactions = [tx("HANDLED", "10.000000001", 2), tx("UNHANDLED", "11.000000001")];
		let staged: readonly StagedProcess[] = [];
		const job = new ExposedTransactionsReadJob(
			query("skip"),
			{ fetchTransactions: async () => page(transactions) } as never,
			registry,
			async (items) => {
				staged = items;
				return items.map((item, index) => ({ ...item.process, id: index + 1 })) as Process[];
			},
			() => {},
			() => 0,
			100,
			new HookBase()
		);

		await job.runCycle(new AbortController().signal);
		assert.equal(staged.length, 1);
		assert.equal(staged[0].process.eventName, "HANDLED");
		assert.equal(staged[0].process.pointerIDX, 2);
		assert.equal(staged[0].process.sourceKey, "transaction:5:10.000000001");
		assert.deepEqual(job.getCursor(), { timestamp: "100.000000000", index: 0 });
	});

	it("uses one provider-independent dedupe key for the same canonical transaction", async () => {
		const first = tx("HANDLED", "10.000000001", 1);
		const second = tx("HANDLED", "10.000000001", 9);
		second.transaction_id = "0.0.999@10.000000001";
		let fetches = 0;
		const sourceKeys: string[] = [];
		const registry = new HandlerRegistry();
		registry.registerTransaction("HANDLED", { async handle() {} });
		const job = new ExposedTransactionsReadJob(
			query(),
			{
				fetchTransactions: async () => {
					const response = page([fetches++ === 0 ? first : second]);
					response.syncWindowEnd = undefined;
					return response;
				},
			} as never,
			registry,
			async (items) => {
				sourceKeys.push(...items.map((item) => item.process.sourceKey));
				return items.map((item, index) => ({ ...item.process, id: index + 1 })) as Process[];
			},
			() => {},
			() => 0,
			100,
			new HookBase()
		);

		await job.runCycle(new AbortController().signal);
		await job.runCycle(new AbortController().signal);

		assert.deepEqual(sourceKeys, ["transaction:5:10.000000001", "transaction:5:10.000000001"]);
		assert.equal(new Set(sourceKeys).size, 1);
	});

	it("continues from the provider next link across bounded cycles when overlap is disabled", async () => {
		const registry = new HandlerRegistry();
		registry.registerTransaction("HANDLED", { async handle() {} });
		const third = page([tx("HANDLED", "3.000000000")]);
		const second = page([tx("HANDLED", "2.000000000")]);
		const first = page([tx("HANDLED", "1.000000000")]);
		second.next = Object.assign(async () => third, { url: () => "page-3" });
		first.next = Object.assign(async () => second, { url: () => "page-2" });
		first.syncWindowEnd = "10.000000000";
		second.syncWindowEnd = undefined;
		third.syncWindowEnd = undefined;
		const config = query();
		config.params.timestamp = "0.000000000";
		config.read.fetch.maxPagesPerCycle = 1;
		config.read.consistency.overlapSeconds = 0;
		let initialFetches = 0;
		const committed: string[] = [];
		const job = new ExposedTransactionsReadJob(
			config,
			{ fetchTransactions: async () => { initialFetches++; return first; } } as never,
			registry,
			async (items) => {
				committed.push(...items.map((item) => item.process.pointerTS));
				return items.map((item, index) => ({ ...item.process, id: index + 1 })) as Process[];
			},
			() => {},
			() => 0,
			100,
			new HookBase()
		);

		await job.runCycle(new AbortController().signal);
		await job.runCycle(new AbortController().signal);
		await job.runCycle(new AbortController().signal);

		assert.equal(initialFetches, 1);
		assert.deepEqual(committed, ["1.000000000", "2.000000000", "3.000000000"]);
		assert.deepEqual(job.getCursor(), { timestamp: "10.000000000", index: 0 });
	});

	it("does not advance the cursor when abort interrupts an asynchronous unhandled-item hook", async () => {
		let hookStarted!: () => void;
		const started = new Promise<void>((resolve) => { hookStarted = resolve; });
		let releaseHook!: () => void;
		const blocked = new Promise<void>((resolve) => { releaseHook = resolve; });
		let commits = 0;
		const job = new ExposedTransactionsReadJob(
			query("skip"),
			{ fetchTransactions: async () => page([tx("UNHANDLED", "2.000000000")]) } as never,
			new HandlerRegistry(),
			async () => { commits++; return []; },
			() => {},
			() => 0,
			100,
			{
				async onUnhandledItem() {
					hookStarted();
					await blocked;
				},
			}
		);
		const controller = new AbortController();

		const cycle = job.runCycle(controller.signal);
		await started;
		controller.abort();
		releaseHook();
		const items = await cycle;

		assert.deepEqual(items, []);
		assert.equal(commits, 0);
		assert.deepEqual(job.getCursor(), { timestamp: "1.000000000", index: 0 });
	});
});
