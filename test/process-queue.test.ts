import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { HookBase } from "../src/eventSync/hooks/HookBase";
import { CompositeHooks } from "../src/eventSync/hooks/CompositeHooks";
import { ProcessQueue } from "../src/eventSync/core/queue/ProcessQueue";
import { ProcessStore } from "../src/eventSync/core/queue/ProcessStore";
import { RestClient } from "../src/eventSync/rest/RestClient";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import { TransactionsReadJob } from "../src/eventSync/core/jobs/TransactionsReadJob";
import type { SyncTransaction, SyncTransactionsPage } from "../src/eventSync/types/mirror";
import type { Process as ProcessRecord } from "../src/eventSync/types/domain";
import type { StagedProcess } from "../src/eventSync/core/queue/types";

const query: Extract<RuntimeQueryConfig, { type: "Transactions" }> = {
	type: "Transactions",
	id: 1,
	title: "attempt test",
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
		holdPeriod: "none",
		nextDelayMs: 0,
		skipTransactionTypes: [],
		unhandledTransactionPolicy: "error",
		status: { name: "RUNNING" },
	},
};

function freshTransactionQuery(): Extract<RuntimeQueryConfig, { type: "Transactions" }> {
	return {
		...query,
		read: {
			...query.read,
			fetch: { ...query.read.fetch },
			consistency: { ...query.read.consistency },
			status: { name: "RUNNING" },
		},
		contract: query.contract ? { ...query.contract } : undefined,
		params: { ...query.params },
		process: { ...query.process, status: { name: "RUNNING" } },
	};
}

describe("ProcessQueue attempts", () => {
	it("produces a restart-safe checkpoint that still admits late records from the overlap window", async () => {
		const runtimeQuery: Extract<RuntimeQueryConfig, { type: "Transactions" }> = {
			...query,
			read: {
				...query.read,
				fetch: { ...query.read.fetch },
				consistency: { finalityLagSeconds: 0, overlapSeconds: 30 },
				status: { name: "RUNNING" },
			},
			params: { ...query.params, timestamp: "0.000000000", index: 0 },
			process: { ...query.process, status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const handler = { async handle() {} };
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const late = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:late",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "90.000000000", pointerIDX: 0,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});
		const newer = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:newer",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "95.000000000", pointerIDX: 0,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});

		await queue.enqueue(late, handler);
		await queue.processOne(newer);
		assert.equal(queue.length(), 1, "the late record remains queued when the newer record succeeds");
		const persisted = queue.getProcessedCheckpoint();
		assert.deepEqual(persisted, { timestamp: "64.999999999", index: 0 });

		const restartQuery: typeof runtimeQuery = {
			...runtimeQuery,
			read: {
				...runtimeQuery.read,
				fetch: { ...runtimeQuery.read.fetch },
				consistency: { ...runtimeQuery.read.consistency },
				status: { name: "RUNNING" },
			},
			params: { ...runtimeQuery.params, ...persisted },
			process: { ...runtimeQuery.process, status: { name: "RUNNING" } },
		};
		const transaction = {
			name: "CRYPTOTRANSFER",
			consensus_timestamp: late.pointerTS,
			transaction_id: `0.0.123@${late.pointerTS}`,
			transaction_hash: "hash",
			nonce: 0,
			nft_transfers: [], token_transfers: [], transfers: [],
		} as unknown as SyncTransaction;
		const page = {
			transactions: [transaction],
			links: { next: null },
			next: Object.assign(async () => null, { url: () => null }),
			syncWindowEnd: "100.000000000",
		} as SyncTransactionsPage;
		let restartedItems: readonly StagedProcess[] = [];
		class RestartReadJob extends TransactionsReadJob {
			runCycle(signal: AbortSignal) { return this.cycle(signal); }
		}
		const restarted = new RestartReadJob(
			restartQuery,
			{ fetchTransactions: async () => page } as never,
			registry,
			async (items) => {
				restartedItems = items;
				return items.map((item, index) => ({ ...item.process, id: index + 1, tries: [] })) as ProcessRecord[];
			},
			() => {},
			() => 0,
			100,
			new HookBase()
		);

		await restarted.runCycle(new AbortController().signal);
		assert.deepEqual(restartedItems.map((item) => item.process.pointerTS), ["90.000000000"]);

		const stillQueued = (queue as unknown as { q: ProcessRecord[] }).q.shift();
		assert.equal(stillQueued, late);
		await queue.processOne(stillQueued);
		assert.deepEqual(queue.getProcessedCheckpoint(), persisted, "an older success cannot move the checkpoint backward");
		queue.dispose();
	});

	it("exposes an honest non-terminal attempt while the handler is active", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		registry.registerTransaction("CRYPTOTRANSFER", { async handle() {} });
		let activeOutcome: string | undefined;
		let activeEndTs: string | undefined;
		const queue = new ProcessQueue(
			query,
			registry,
			store,
			{ onProcessStarted: (_query, _process, attempt) => {
				activeOutcome = attempt.outcome;
				activeEndTs = attempt.endTs;
			} },
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:attempt-state",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000001",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		await queue.processOne(process);
		assert.equal(activeOutcome, "PROCESSING");
		assert.equal(activeEndTs, undefined);
		queue.dispose();
	});

	it("stores exactly one try per attempt with globally unique try and step ids", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let calls = 0;
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle(_data, context) {
				context.addStep?.({ title: "handler", data: 0 });
				calls++;
				if (calls === 1) throw new Error("first attempt fails");
			},
		});

		const queue = new ProcessQueue(query, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:1:first",
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
		await queue.processOne(process);

		assert.equal(process.tries?.length, 2);
		assert.deepEqual(process.tries?.map((attempt) => attempt.number), [1, 2]);
		assert.equal(new Set(process.tries?.map((attempt) => attempt.id)).size, 2);
		const stepIds = process.tries?.flatMap((attempt) => attempt.steps?.map((step) => step.id) ?? []) ?? [];
		assert.equal(new Set(stepIds).size, stepIds.length);
		const handlerStep = process.tries?.[1].steps?.find((step) => step.title === "handler");
		assert.equal(handlerStep?.type, "Info");
		assert.equal(handlerStep?.data, 0);
		assert.equal(process.tries?.[0].outcome, "FAILED");
		assert.equal(process.tries?.[1].outcome, "SUCCEEDED");
		queue.dispose();
	});

	it("executes the handler assigned during ingestion even if the registry changes", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const calls: string[] = [];
		const original = { async handle() { calls.push("original"); } };
		registry.registerTransaction("CRYPTOTRANSFER", original);
		const queue = new ProcessQueue(query, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:1:second",
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
		await queue.enqueue(process, original);
		registry.registerTransaction("CRYPTOTRANSFER", { async handle() { calls.push("replacement"); } });

		await queue.processOne(process);
		assert.deepEqual(calls, ["original"]);
		queue.dispose();
	});

	it("enforces maxQueuedProcesses as a hard capacity", async () => {
		const limited = { ...query, process: { ...query.process, maxQueuedProcesses: 2 } };
		const store = new ProcessStore();
		const queue = new ProcessQueue(limited, new HandlerRegistry(), store, new HookBase(), new RestClient({} as HederaRestClient));
		const handler = { async handle() {} };
		const items = [1, 2, 3].map((number) => ({
			process: store.createProcess({
				queryId: 1,
				sourceKey: `transaction:capacity:${number}`,
				eventName: "CRYPTOTRANSFER",
				data: {},
				rawData: {},
				pointerTS: `${number}.000000000`,
				pointerIDX: 0,
				status: "Queued",
				timeOfEmit: 0,
				createdAt: 0,
				updatedAt: 0,
			}),
			handler,
		}));

		await assert.rejects(queue.enqueueBatch(items), /capacity 2 exceeded/);
		assert.equal(queue.length(), 0);
		queue.dispose();
	});

	it("reserves capacity for an in-flight item's ordered retry", async () => {
		const limited = { ...query, process: { ...query.process, maxQueuedProcesses: 2 } };
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerStarted!: () => void;
		const started = new Promise<void>((resolve) => { handlerStarted = resolve; });
		let releaseHandler!: () => void;
		const blocked = new Promise<void>((resolve) => { releaseHandler = resolve; });
		const handler = {
			async handle() {
				handlerStarted();
				await blocked;
				throw new Error("retry me");
			},
		};
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(limited, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:capacity:active",
			eventName: "CRYPTOTRANSFER",
			data: {},
			rawData: {},
			pointerTS: "1700000000.000000001",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});
		await queue.enqueue(process, handler);
		const loop = queue.runLoop();
		await started;

		assert.equal(queue.length(), 1);
		const controller = new AbortController();
		let capacityGranted = false;
		const capacity = queue.waitForCapacity(2, controller.signal).then(() => { capacityGranted = true; });
		await new Promise<void>((resolve) => setTimeout(resolve, 40));
		assert.equal(capacityGranted, false);

		controller.abort();
		await assert.rejects(capacity, /aborted/);
		queue.stop();
		releaseHandler();
		await loop;
		assert.ok(queue.length() <= 2);
		queue.dispose();
	});

	it("accepts explicitly registered handlers regardless of their class name", async () => {
		class DefaultHandler {
			calls = 0;
			async handle() { this.calls++; }
		}
		const handler = new DefaultHandler();
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(query, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:default-handler",
			eventName: "CRYPTOTRANSFER",
			data: {},
			rawData: {},
			pointerTS: "1700000000.000000002",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		await queue.processOne(process);
		assert.equal(handler.calls, 1);
		assert.equal(process.status, "Processed");
		queue.dispose();
	});

	it("never overwrites FAILED with a pause transition when an active handler is aborted", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let queue!: ProcessQueue;
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle() {
				queue.fail("fatal hook failure");
				throw new Error("handler aborted");
			},
		});
		queue = new ProcessQueue(
			{ ...query, process: { ...query.process, pauseAfterConsecutiveFailures: 1 } },
			registry,
			store,
			new HookBase(),
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:terminal-state",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000003",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		await queue.processOne(process);
		assert.equal(queue.getStatus().name, "FAILED");
		assert.equal(process.status, "Queued");
		queue.dispose();
	});

	it("does not retain failure counters for successful processes", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const handler = { async handle() {} };
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(query, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		for (let number = 0; number < 25; number++) {
			const process = store.createProcess({
				queryId: 1,
				sourceKey: `transaction:successful:${number}`,
				eventName: "CRYPTOTRANSFER",
				data: {} as never,
				rawData: {},
				pointerTS: `1700000000.${String(number).padStart(9, "0")}`,
				pointerIDX: 0,
				status: "Queued",
				timeOfEmit: 0,
				createdAt: 0,
				updatedAt: 0,
			});
			await queue.processOne(process);
		}

		assert.equal((queue as unknown as { consecutiveFailuresFor: Map<number, number> }).consecutiveFailuresFor.size, 0);
		assert.equal(store.countByQuery(1), 0);
		queue.dispose();
	});

	it("keeps a single retention timer without rescanning retained records on every success", () => {
		const retainedQuery = { ...query, process: { ...query.process, holdPeriod: "daily" as const } };
		const queue = new ProcessQueue(retainedQuery, new HandlerRegistry(), new ProcessStore(), new HookBase(), new RestClient({} as HederaRestClient));
		const internals = queue as unknown as {
			retainedUntil: Map<number, number> & { values: () => IterableIterator<number> };
			retentionTimer?: ReturnType<typeof import("node:timers").setTimeout>;
			scheduleProcessRetention: (process: { id: number }) => void;
		};
		const originalValues = internals.retainedUntil.values.bind(internals.retainedUntil);
		let scans = 0;
		internals.retainedUntil.values = () => {
			scans++;
			return originalValues();
		};
		for (let id = 1; id <= 100; id++) internals.scheduleProcessRetention({ id });

		assert.equal(scans, 1);
		assert.ok(internals.retentionTimer);
		assert.equal(internals.retentionTimer.hasRef(), false, "history retention must not keep the Node process alive");
		queue.dispose();
	});

	it("preserves diagnostic information when a handler throws a non-Error value", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle() {
				throw "plain failure";
			},
		});
		const queue = new ProcessQueue(query, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:non-error",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000004",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		await queue.processOne(process);
		assert.deepEqual(JSON.parse(process.tries?.[0].error ?? "null"), {
			name: "NonErrorThrown",
			message: "plain failure",
		});
		const failureStep = process.tries?.[0].steps?.find((step) => step.title === "The process failed");
		assert.deepEqual(failureStep?.data, { error: "plain failure" });
		queue.dispose();
	});

	it("contains a revoked Proxy thrown by a handler and restores retry-safe state", async () => {
		const runtimeQuery = freshTransactionQuery();
		const thrown = Proxy.revocable({}, {});
		thrown.revoke();
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle() {
				throw thrown.proxy;
			},
		});
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:revoked-proxy-error",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000014",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		await queue.processOne(process);

		assert.equal(queue.getInFlight(), undefined);
		assert.equal(process.status, "Queued");
		assert.equal(process.tries.at(-1)?.outcome, "FAILED");
		assert.deepEqual(JSON.parse(process.tries.at(-1)?.error ?? "null"), {
			name: "NonErrorThrown",
			message: "[unserializable thrown value]",
		});
		const failureStep = process.tries.at(-1)?.steps.find((step) => step.title === "The process failed");
		assert.deepEqual(failureStep?.data, { error: "[unserializable thrown value]" });
		assert.equal(queue.length(), 1);
		queue.dispose();
	});

	it("turns an escaped handler addStep callback into a no-op when the awaited handler settles", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			params: { ...query.params },
			process: { ...query.process, holdPeriod: "none", status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let escapedAddStep: ((step: { title: string }) => void) | undefined;
		let stepHookCalls = 0;
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle(_data, context) {
				escapedAddStep = context.addStep;
				context.addStep?.({ title: "inside handler" });
			},
		});
		const queue = new ProcessQueue(
			runtimeQuery,
			registry,
			store,
			{ onStepCreated() { stepHookCalls++; } },
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:escaped-step",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "1700000000.000000006", pointerIDX: 0,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});

		await queue.processOne(process);
		await queue.flushHooks();
		const callsAtSettlement = stepHookCalls;
		assert.equal(store.countByQuery(runtimeQuery.id), 0);
		assert.ok(escapedAddStep);

		escapedAddStep({ title: "too late" });
		await queue.flushHooks();
		assert.equal(stepHookCalls, callsAtSettlement);
		assert.equal(store.countByQuery(runtimeQuery.id), 0);
		assert.equal(process.tries[0].steps.some((step) => step.title === "too late"), false);
		queue.dispose();
	});

	it("closes an escaped handler addStep callback after a failed attempt", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			params: { ...query.params },
			process: { ...query.process, status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let escapedAddStep: ((step: { title: string }) => void) | undefined;
		let stepHookCalls = 0;
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle(_data, context) {
				escapedAddStep = context.addStep;
				throw new Error("expected failure");
			},
		});
		const queue = new ProcessQueue(
			runtimeQuery,
			registry,
			store,
			{ onStepCreated() { stepHookCalls++; } },
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:escaped-step-failure",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "1700000000.000000006", pointerIDX: 0,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});

		await queue.processOne(process);
		await queue.flushHooks();
		const callsAtSettlement = stepHookCalls;
		const stepsAtSettlement = process.tries[0].steps.length;
		assert.ok(escapedAddStep);

		escapedAddStep({ title: "too late after failure" });
		await queue.flushHooks();
		assert.equal(stepHookCalls, callsAtSettlement);
		assert.equal(process.tries[0].steps.length, stepsAtSettlement);
		queue.dispose();
	});

	it("expires retained successes even after fail-fast stops processing", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			params: { ...query.params },
			process: { ...query.process, holdPeriod: "daily", status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		registry.registerTransaction("CRYPTOTRANSFER", { async handle() {} });
		let queue!: ProcessQueue;
		queue = new ProcessQueue(
			runtimeQuery,
			registry,
			store,
			{ onProcessSucceeded() { queue.fail("fail-fast hook policy"); } },
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:retention-after-failure",
			eventName: "CRYPTOTRANSFER",
			data: {}, rawData: {}, pointerTS: "1700000000.000000007", pointerIDX: 0,
			status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
		});

		await queue.processOne(process);
		assert.equal(queue.getStatus().name, "FAILED");
		assert.equal(store.countByQuery(runtimeQuery.id), 1);
		const internals = queue as unknown as {
			retainedUntil: Map<number, number>;
			scheduleRetentionSweep(): void;
		};
		internals.retainedUntil.set(process.id, Date.now() - 1);
		internals.scheduleRetentionSweep();
		await new Promise<void>((resolve) => setTimeout(resolve, 10));
		assert.equal(store.countByQuery(runtimeQuery.id), 0);
		queue.dispose();
	});

	it("does not start a batch item until its enqueue notification settles", async () => {
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerStarted!: () => void;
		const handlerStart = new Promise<void>((resolve) => { handlerStarted = resolve; });
		let handlerStarts = 0;
		const handler = {
			async handle() {
				handlerStarts++;
				handlerStarted();
			},
		};
		registry.registerTransaction("CRYPTOTRANSFER", handler);

		let notificationStarted!: () => void;
		const notificationStart = new Promise<void>((resolve) => { notificationStarted = resolve; });
		let releaseNotification!: () => void;
		const notificationBlocked = new Promise<void>((resolve) => { releaseNotification = resolve; });
		const queue = new ProcessQueue(
			query,
			registry,
			store,
			{
				async onProcessEnqueued() {
					notificationStarted();
					await notificationBlocked;
				},
			},
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: 1,
			sourceKey: "transaction:enqueue-hook-order",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000005",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		const enqueue = queue.enqueueBatch([{ process, handler }]);
		await notificationStart;
		const loop = queue.runLoop();
		let notificationReleased = false;
		try {
			await new Promise<void>((resolve) => setImmediate(resolve));
			assert.equal(handlerStarts, 0);

			releaseNotification();
			notificationReleased = true;
			await enqueue;
			await handlerStart;
			assert.equal(handlerStarts, 1);
		} finally {
			if (!notificationReleased) releaseNotification();
			await enqueue.catch(() => undefined);
			queue.stop();
			await loop;
			queue.dispose();
		}
	});

	it("does not dequeue a process after pause settles while the queue lock is pending", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			contract: query.contract ? { ...query.contract } : undefined,
			params: { ...query.params },
			process: { ...query.process, status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerStarts = 0;
		const handler = { async handle() { handlerStarts++; } };
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:pause-lock-race",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000005",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});
		await queue.enqueue(process, handler);

		const mutex = (queue as unknown as {
			mutex: { lock(): Promise<() => void>; waiters: Array<() => void> };
		}).mutex;
		const releaseHeldLock = await mutex.lock();
		let released = false;
		const loop = queue.runLoop();

		try {
			for (let attempt = 0; attempt < 10 && mutex.waiters.length === 0; attempt++) {
				await new Promise<void>((resolve) => setImmediate(resolve));
			}
			assert.equal(mutex.waiters.length, 1);
			await queue.pause();
			assert.equal(queue.getStatus().name, "PAUSED");

			releaseHeldLock();
			released = true;
			await new Promise<void>((resolve) => setImmediate(resolve));

			assert.equal(handlerStarts, 0);
			assert.equal(queue.length(), 1);
		} finally {
			if (!released) releaseHeldLock();
			queue.stop();
			await loop;
			queue.dispose();
		}
	});

	it("does not resolve an operator pause while an already-pausing attempt is still in flight", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			contract: query.contract ? { ...query.contract } : undefined,
			params: { ...query.params },
			process: { ...query.process, pauseAfterConsecutiveFailures: 1, status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const handler = { async handle() { throw new Error("expected failure"); } };
		registry.registerTransaction("CRYPTOTRANSFER", handler);

		let failureHookStarted!: () => void;
		const failureHookStart = new Promise<void>((resolve) => { failureHookStarted = resolve; });
		let releaseFailureHook!: () => void;
		const failureHookBlocked = new Promise<void>((resolve) => { releaseFailureHook = resolve; });
		const observedStatuses: string[] = [];
		const queue = new ProcessQueue(
			runtimeQuery,
			registry,
			store,
			{
				async onProcessConsecutiveFailuresReached() {
					failureHookStarted();
					await failureHookBlocked;
				},
				onQueryStatusChange(_query, payload) {
					observedStatuses.push(payload.status.name);
				},
			},
			new RestClient({} as HederaRestClient)
		);
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:operator-pause-race",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000008",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});
		await queue.enqueue(process, handler);

		const loop = queue.runLoop();
		let hookReleased = false;
		try {
			await failureHookStart;
			assert.equal(queue.getStatus().name, "PAUSING");
			assert.ok(queue.getInFlight(), "the failed attempt remains active until its failure hook settles");

			let pauseResolved = false;
			const pause = queue.pause("operator").then(() => { pauseResolved = true; });
			await new Promise<void>((resolve) => setImmediate(resolve));

			assert.equal(pauseResolved, false);
			assert.equal(queue.getStatus().name, "PAUSING");
			assert.ok(queue.getInFlight());

			releaseFailureHook();
			hookReleased = true;
			await pause;
			await queue.flushHooks();
			assert.equal(queue.getStatus().name, "PAUSED");
			assert.equal(queue.getInFlight(), undefined);
			assert.deepEqual(observedStatuses, ["PAUSING", "PAUSED"]);
		} finally {
			if (!hookReleased) releaseFailureHook();
			queue.stop();
			await loop;
			queue.dispose();
		}
	});

	it("settles an automatic failure pause before entering a large inter-process delay", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			contract: query.contract ? { ...query.contract } : undefined,
			params: { ...query.params },
			process: {
				...query.process,
				pauseAfterConsecutiveFailures: 1,
				nextDelayMs: 60_000,
				status: { name: "RUNNING" },
			},
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		const handler = { async handle() { throw new Error("expected failure"); } };
		registry.registerTransaction("CRYPTOTRANSFER", handler);
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));

		let delayStarted!: () => void;
		const delayStart = new Promise<void>((resolve) => { delayStarted = resolve; });
		let releaseDelay!: () => void;
		const delayBlocked = new Promise<void>((resolve) => { releaseDelay = resolve; });
		(queue as unknown as { delayNext(): Promise<void> }).delayNext = async () => {
			delayStarted();
			await delayBlocked;
		};

		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:pause-before-delay",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000009",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});
		await queue.enqueue(process, handler);

		const loop = queue.runLoop();
		try {
			await delayStart;
			assert.equal(queue.getStatus().name, "PAUSED");
			assert.equal(queue.getInFlight(), undefined);
		} finally {
			releaseDelay();
			queue.stop();
			await loop;
			queue.dispose();
		}
	});

	it("resets process failures when resumed while a threshold pause hook is settling", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			contract: query.contract ? { ...query.contract } : undefined,
			params: { ...query.params },
			process: { ...query.process, pauseAfterConsecutiveFailures: 2, status: { name: "RUNNING" } },
		};
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let attempts = 0;
		const handler = {
			async handle() {
				attempts++;
				throw new Error("expected failure");
			},
		};
		registry.registerTransaction("CRYPTOTRANSFER", handler);

		let thresholdStarted!: () => void;
		const thresholdStart = new Promise<void>((resolve) => { thresholdStarted = resolve; });
		let releaseThreshold!: () => void;
		const thresholdBlocked = new Promise<void>((resolve) => { releaseThreshold = resolve; });
		const thresholdCounts: number[] = [];
		const queue = new ProcessQueue(
			runtimeQuery,
			registry,
			store,
			{
				async onProcessConsecutiveFailuresReached(_query, _process, consecutiveFailures) {
					thresholdCounts.push(consecutiveFailures);
					if (consecutiveFailures === 2) {
						thresholdStarted();
						await thresholdBlocked;
					}
				},
			},
			new RestClient({} as HederaRestClient)
		);

		let thirdAttemptSettled!: () => void;
		const thirdAttemptSettlement = new Promise<void>((resolve) => { thirdAttemptSettled = resolve; });
		let releaseThirdDelay!: () => void;
		const thirdDelayBlocked = new Promise<void>((resolve) => { releaseThirdDelay = resolve; });
		(queue as unknown as { delayNext(): Promise<void> }).delayNext = async () => {
			if (attempts < 3) return;
			thirdAttemptSettled();
			await thirdDelayBlocked;
		};

		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:resume-pausing-counter-reset",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000010",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});
		await queue.enqueue(process, handler);

		const loop = queue.runLoop();
		let thresholdReleased = false;
		try {
			await thresholdStart;
			assert.equal(queue.getStatus().name, "PAUSING");
			assert.ok(queue.getInFlight());

			await queue.resume();
			assert.equal(queue.getStatus().name, "RUNNING");

			releaseThreshold();
			thresholdReleased = true;
			await thirdAttemptSettlement;

			assert.equal(attempts, 3);
			assert.equal(queue.getStatus().name, "RUNNING");
			assert.equal(
				(queue as unknown as { consecutiveFailuresFor: Map<number, number> }).consecutiveFailuresFor.get(process.id),
				1
			);
			assert.deepEqual(thresholdCounts, [2]);
		} finally {
			if (!thresholdReleased) releaseThreshold();
			queue.stop();
			releaseThirdDelay();
			await loop;
			queue.dispose();
		}
	});
});

describe("ProcessQueue lifecycle and snapshot safety", () => {
	it("rejects pause when a terminal failure wins the active-handler race", async () => {
		const runtimeQuery = freshTransactionQuery();
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerStarted!: () => void;
		const handlerStart = new Promise<void>((resolve) => { handlerStarted = resolve; });
		let releaseHandler!: () => void;
		const handlerBlocked = new Promise<void>((resolve) => { releaseHandler = resolve; });
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle() {
				handlerStarted();
				await handlerBlocked;
			},
		});
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:terminal-pause-race",
			eventName: "CRYPTOTRANSFER",
			data: {} as never,
			rawData: {},
			pointerTS: "1700000000.000000011",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		const processing = queue.processOne(process);
		try {
			await handlerStart;
			let statusWaitStarted!: () => void;
			const statusWaitStart = new Promise<void>((resolve) => { statusWaitStarted = resolve; });
			const internals = queue as unknown as { wait(milliseconds: number): Promise<void> };
			const wait = internals.wait.bind(queue);
			internals.wait = async (milliseconds) => {
				statusWaitStarted();
				await wait(milliseconds);
			};
			const pausing = queue.pause("operator");
			await statusWaitStart;
			queue.fail("terminal failure");

			await assert.rejects(pausing, {
				name: "Error",
				message: "Processing queue could not reach PAUSED because it entered FAILED: terminal failure",
			});
		} finally {
			releaseHandler();
			await processing;
			queue.dispose();
		}
	});

	it("rejects resume when a status observer stops the queue before it settles", async () => {
		const runtimeQuery: RuntimeQueryConfig = {
			...query,
			read: { ...query.read, fetch: { ...query.read.fetch }, consistency: { ...query.read.consistency }, status: { name: "RUNNING" } },
			contract: query.contract ? { ...query.contract } : undefined,
			params: { ...query.params },
			process: { ...query.process, status: { name: "PAUSED", info: "operator" } },
		};
		let queue!: ProcessQueue;
		queue = new ProcessQueue(
			runtimeQuery,
			new HandlerRegistry(),
			new ProcessStore(),
			{
				onQueryStatusChange(_query, payload) {
					if (payload.status.name === "RESUMING") queue.stop("terminal stop");
				},
			},
			new RestClient({} as HederaRestClient)
		);

		try {
			await assert.rejects(queue.resume(), {
				name: "Error",
				message: "Processing queue could not reach RUNNING because it entered STOPPED: terminal stop",
			});
			await queue.flushHooks();
			assert.deepEqual(queue.getStatus(), { name: "STOPPED", info: "terminal stop" });
		} finally {
			queue.dispose();
		}
	});

	it("omits active payloads without inspecting them", async () => {
		const runtimeQuery = freshTransactionQuery();
		let dataReads = 0;
		const data = Object.defineProperty({}, "danger", {
			enumerable: true,
			get() {
				dataReads++;
				throw new Error("data must not be read");
			},
		});
		let rawIntrospections = 0;
		const rawData = new Proxy({}, {
			ownKeys() {
				rawIntrospections++;
				throw new Error("rawData must not be inspected");
			},
		});
		const store = new ProcessStore();
		const registry = new HandlerRegistry();
		let handlerStarted!: () => void;
		const handlerStart = new Promise<void>((resolve) => { handlerStarted = resolve; });
		let releaseHandler!: () => void;
		const handlerBlocked = new Promise<void>((resolve) => { releaseHandler = resolve; });
		registry.registerTransaction("CRYPTOTRANSFER", {
			async handle() {
				handlerStarted();
				await handlerBlocked;
			},
		});
		const queue = new ProcessQueue(runtimeQuery, registry, store, new HookBase(), new RestClient({} as HederaRestClient));
		const process = store.createProcess({
			queryId: runtimeQuery.id,
			sourceKey: "transaction:opaque-inspection",
			eventName: "CRYPTOTRANSFER",
			data,
			rawData,
			pointerTS: "1700000000.000000012",
			pointerIDX: 0,
			status: "Queued",
			timeOfEmit: 0,
			createdAt: 0,
			updatedAt: 0,
		});

		const processing = queue.processOne(process);
		try {
			await handlerStart;
			// Processing may have produced full hook snapshots. Measure only the
			// default inspection call, which promises to omit both payload fields.
			dataReads = 0;
			rawIntrospections = 0;
			const visible = queue.getInFlight();
			assert.ok(visible);
			assert.equal(Object.hasOwn(visible, "data"), false);
			assert.equal(Object.hasOwn(visible, "rawData"), false);
			assert.equal(dataReads, 0);
			assert.equal(rawIntrospections, 0);
		} finally {
			releaseHandler();
			await processing;
			queue.dispose();
		}
	});

	for (const [description, createPayload] of [
		[
			"throwing accessor",
			() => Object.defineProperty({}, "danger", {
				enumerable: true,
				get() { throw new Error("accessor was invoked"); },
			}),
		],
		[
			"throwing proxy trap",
			() => new Proxy({}, {
				ownKeys() { throw new Error("proxy was introspected"); },
			}),
		],
	] as const) {
		it(`does not strand an attempt whose normalized payload has a ${description}`, async () => {
			const runtimeQuery = freshTransactionQuery();
			const store = new ProcessStore();
			const registry = new HandlerRegistry();
			registry.registerTransaction("CRYPTOTRANSFER", { async handle() {} });
			const hooks = new CompositeHooks([{ onProcessStarted() {} }]);
			const queue = new ProcessQueue(runtimeQuery, registry, store, hooks, new RestClient({} as HederaRestClient));
			const process = store.createProcess({
				queryId: runtimeQuery.id,
				sourceKey: `transaction:snapshot-${description}`,
				eventName: "CRYPTOTRANSFER",
				data: createPayload(),
				rawData: {},
				pointerTS: "1700000000.000000013",
				pointerIDX: 0,
				status: "Queued",
				timeOfEmit: 0,
				createdAt: 0,
				updatedAt: 0,
			});

			await queue.processOne(process);

			assert.equal(queue.getInFlight(), undefined);
			assert.equal(process.status, "Processed");
			assert.equal(process.tries.at(-1)?.outcome, "SUCCEEDED");
			queue.dispose();
		});
	}
});
