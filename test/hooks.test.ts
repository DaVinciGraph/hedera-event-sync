import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { CompositeHooks } from "../src/eventSync/hooks/CompositeHooks";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { ProcessQueue } from "../src/eventSync/core/queue/ProcessQueue";
import { ProcessStore } from "../src/eventSync/core/queue/ProcessStore";
import { RestClient } from "../src/eventSync/rest/RestClient";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";

function transactionQuery(): Extract<RuntimeQueryConfig, { type: "Transactions" }> {
	return {
		type: "Transactions",
		id: 11,
		title: "hook isolation",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 100, maxPagesPerCycle: 10, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 15, overlapSeconds: 30 },
			pauseAfterConsecutiveFailures: 3,
			status: { name: "RUNNING" },
		},
		contract: { address: "0.0.123" },
		params: { accountId: "0.0.123", timestamp: "0.000000000", index: 0, result: "success" },
		process: {
			pauseAfterConsecutiveFailures: 3,
			maxQueuedProcesses: 100,
			holdPeriod: "daily",
			nextDelayMs: 0,
			skipTransactionTypes: [],
			unhandledTransactionPolicy: "error",
			status: { name: "RUNNING" },
		},
	};
}

describe("hook failure isolation", () => {
	for (const policy of ["pause", "fail-fast"] as const) {
		it(`applies the ${policy} policy before a blocked error observer settles`, async () => {
			let observerStarted!: () => void;
			const observerStart = new Promise<void>((resolve) => { observerStarted = resolve; });
			let releaseObserver!: () => void;
			const observerBlocked = new Promise<void>((resolve) => { releaseObserver = resolve; });
			let appliedPolicy: string | undefined;
			const hooks = new CompositeHooks(
				[
					{ onReadInit() { throw new Error("observer failed"); } },
					{
						async onHookError() {
							observerStarted();
							await observerBlocked;
						},
					},
				],
				{
					policy,
					onFailure(failure) {
						appliedPolicy = failure.policy;
					},
				}
			);

			const dispatch = hooks.onReadInit(transactionQuery(), 30);
			try {
				await observerStart;
				assert.equal(appliedPolicy, policy);
			} finally {
				releaseObserver();
				await dispatch;
			}
		});
	}

	it("reports each failed hook and still invokes later hooks", async () => {
		const calls: string[] = [];
		const failures: string[] = [];
		const hooks = new CompositeHooks([
			{
				onReadInit() {
					calls.push("first");
					throw new Error("observer failed");
				},
			},
			{
				onReadInit() {
					calls.push("second");
				},
				onHookError(failure) {
					failures.push(`${failure.hook}:${(failure.error as Error).message}`);
				},
			},
		]);

		await hooks.onReadInit(transactionQuery(), 30);
		assert.deepEqual(calls, ["first", "second"]);
		assert.deepEqual(failures, ["onReadInit:observer failed"]);
	});

	it("uses one stable hook set for an event already being dispatched", async () => {
		const calls: string[] = [];
		let releaseFirst!: () => void;
		const firstWaiting = new Promise<void>((resolve) => { releaseFirst = resolve; });
		const hooks = new CompositeHooks([
			{ async onReadInit() { calls.push("old-1"); await firstWaiting; } },
			{ onReadInit() { calls.push("old-2"); } },
		]);
		const dispatch = hooks.onReadInit(transactionQuery(), 30);
		await new Promise((resolve) => setTimeout(resolve, 0));
		hooks.setHooks([{ onReadInit() { calls.push("new"); } }]);
		releaseFirst();
		await dispatch;

		assert.deepEqual(calls, ["old-1", "old-2"]);
	});

	it("delivers steps before success and advances the processed checkpoint afterward", async () => {
		const query = transactionQuery();
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const order: string[] = [];
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle(_data, context) {
				context.addStep?.({ title: "handler" });
			},
		});
		const hooks = new CompositeHooks([{
			async onStepCreated(_query, _process, _try, step) {
				await new Promise((resolve) => setTimeout(resolve, 5));
				order.push(`step:${step.orderNo}`);
			},
			onProcessSucceeded() { order.push("success"); },
			onProcessedCheckpoint(_query, cursor) { order.push(`checkpoint:${cursor.timestamp}`); },
		}]);
		const queue = new ProcessQueue(query, registry, store, hooks, new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: query.id,
			sourceKey: "transaction:11:ordered-hooks",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "1700000000.000000003", pointerIDX: 2,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});

		await queue.processOne(process);
		assert.deepEqual(order, ["step:1", "step:2", "step:3", "step:4", "success", "checkpoint:1699999970.000000002"]);
		assert.deepEqual(queue.getProcessedCheckpoint(), { timestamp: "1699999970.000000002", index: 0 });
		queue.dispose();
	});

	it("serializes status notifications", async () => {
		const query = transactionQuery();
		const observed: string[] = [];
		const hooks = new CompositeHooks([{
			async onQueryStatusChange(_query, payload) {
				if (payload.status.name === "PAUSING") await new Promise((resolve) => setTimeout(resolve, 10));
				observed.push(payload.status.name);
			},
		}]);
		const queue = new ProcessQueue(query, new HandlerRegistry(), new ProcessStore(), hooks, new RestClient({} as HederaRestClient));
		await queue.pause();
		await queue.flushHooks();
		assert.deepEqual(observed, ["PAUSING", "PAUSED"]);
		queue.dispose();
	});

	it("does not retry a successful handler when its success hook fails", async () => {
		const query = transactionQuery();
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerCalls = 0;
		const handler = { async handle() { handlerCalls++; } };
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		let reported = 0;
		let laterHookCalls = 0;
		const hooks = new CompositeHooks([
			{ onProcessSucceeded: async () => { throw new Error("persistence observer failed"); } },
			{ onProcessSucceeded: () => void laterHookCalls++, onHookError: () => void reported++ },
		]);
		const queue = new ProcessQueue(query, registry, store, hooks, new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: query.id,
			sourceKey: "transaction:11:success-hook",
			eventName: "CRYPTOTRANSFER",
			data: {},
			rawData: {},
			pointerTS: "1700000000.000000001",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: Date.now(),
			createdAt: Date.now(),
			updatedAt: Date.now(),
		});

		await queue.processOne(process);

		assert.equal(handlerCalls, 1);
		assert.equal(process.status, "Processed");
		assert.equal(queue.length(), 0, "success hook failure did not add a retry");
		assert.equal(reported, 1);
		assert.equal(laterHookCalls, 1);
		queue.dispose();
	});
});
