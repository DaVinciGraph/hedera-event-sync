import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { ProcessQueue } from "../src/eventSync/core/queue/ProcessQueue";
import { ProcessStore } from "../src/eventSync/core/queue/ProcessStore";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import type { TransactionHandler } from "../src/eventSync/handlers/types";
import type { SyncHooks } from "../src/eventSync/hooks/SyncHooks";
import { RestClient } from "../src/eventSync/rest/RestClient";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { Process, ProcessedCheckpoint } from "../src/eventSync/types/domain";

function setup(options: {
	getReplayFloor?: () => string | undefined;
	hooks?: SyncHooks;
	handler?: TransactionHandler;
	initialCheckpoint?: ProcessedCheckpoint;
} = {}) {
	const query: Extract<RuntimeQueryConfig, { type: "Transactions" }> = {
		type: "Transactions", id: 1, title: "checkpoint barriers",
		params: { accountId: "0.0.123", timestamp: "0.000000000", index: 0, result: "success", ...options.initialCheckpoint },
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 100, maxPagesPerCycle: 10, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 30 },
			pauseAfterConsecutiveFailures: 3, status: { name: "RUNNING" },
		},
		process: {
			pauseAfterConsecutiveFailures: 3, maxQueuedProcesses: 100,
			holdPeriod: "none", nextDelayMs: 0, skipTransactionTypes: [],
			unhandledTransactionPolicy: "error", status: { name: "RUNNING" },
		},
	};
	const registry = new HandlerRegistry();
	const handler = options.handler ?? { handle() {} };
	registry.registerTransaction("CRYPTOTRANSFER", handler);
	const store = new ProcessStore();
	const queue = new ProcessQueue(
		query, registry, store, options.hooks ?? {}, new RestClient({} as HederaRestClient),
		undefined, options.getReplayFloor
	);
	let sequence = 0;
	const create = (seconds: number) => store.createProcess({
		queryId: query.id, sourceKey: `transaction:${++sequence}`, eventName: "CRYPTOTRANSFER",
		data: {}, rawData: {}, pointerTS: `${seconds}.000000000`, pointerIDX: 0,
		status: "Queued", timeOfEmit: 0, createdAt: 0, updatedAt: 0,
	});
	const processNext = async () => {
		const process = (queue as unknown as { q: Process[] }).q.shift();
		assert.ok(process);
		await queue.processOne(process);
		return process;
	};
	return { queue, create, handler, processNext };
}

describe("ProcessQueue replay checkpoint barriers", () => {
	it("caps successful progress at an unfinished replay and releases duplicate-only replays on inspection", async () => {
		let replayFloor: string | undefined = "50.000000000";
		const notifications: ProcessedCheckpoint[] = [];
		const { queue, create } = setup({
			getReplayFloor: () => replayFloor,
			hooks: { onProcessedCheckpoint(_query, checkpoint) { notifications.push({ ...checkpoint }); } },
		});
		try {
			await queue.processOne(create(120));
			assert.deepEqual(queue.getProcessedCheckpoint(), { timestamp: "49.999999999", index: 0 });
			assert.deepEqual(notifications, [{ timestamp: "49.999999999", index: 0 }]);

			replayFloor = undefined;
			const checkpoint = queue.getProcessedCheckpoint();
			assert.deepEqual(checkpoint, { timestamp: "89.999999999", index: 0 });
			assert.equal(notifications.length, 1, "inspection does not fabricate a process-success notification");
			checkpoint.timestamp = "0.000000000";
			assert.equal(queue.getProcessedCheckpoint().timestamp, "89.999999999");
		} finally {
			queue.dispose();
		}
	});

	it("retains the oldest queued replay record after the reader barrier releases and through retries", async () => {
		let replayFloor: string | undefined = "50.000000000";
		let rejectLateRecord = true;
		const { queue, create, handler, processNext } = setup({
			getReplayFloor: () => replayFloor,
			handler: {
				handle(_data, context) {
					if (context.raw.consensus_timestamp === "55.000000000" && rejectLateRecord) throw new Error("retry late record");
				},
			},
		});
		const newer = create(120);
		const late = create(55);
		late.rawData = { consensus_timestamp: late.pointerTS };
		const newest = create(150);
		try {
			await queue.enqueueBatch([newer, late, newest].map((process) => ({ process, handler })));
			await processNext();
			assert.equal(queue.getProcessedCheckpoint().timestamp, "49.999999999");

			replayFloor = undefined;
			assert.equal(queue.getProcessedCheckpoint().timestamp, "54.999999999");
			await processNext();
			assert.equal(late.status, "Queued");
			assert.equal(queue.getProcessedCheckpoint().timestamp, "54.999999999");

			rejectLateRecord = false;
			await processNext();
			assert.equal(late.status, "Processed");
			assert.equal(queue.getProcessedCheckpoint().timestamp, "89.999999999");
			await processNext();
			assert.equal(queue.getProcessedCheckpoint().timestamp, "119.999999999");
		} finally {
			queue.dispose();
		}
	});

	it("installs all batch barriers before any enqueue observer can read a checkpoint", async () => {
		let replayFloor: string | undefined = "50.000000000";
		const observed: string[] = [];
		const { queue, create, handler } = setup({
			getReplayFloor: () => replayFloor,
			hooks: {
				onProcessEnqueued() {
					replayFloor = undefined;
					observed.push(queue.getProcessedCheckpoint().timestamp);
				},
			},
		});
		try {
			await queue.processOne(create(120));
			await queue.enqueueBatch([create(150), create(55)].map((process) => ({ process, handler })));
			assert.deepEqual(observed, ["54.999999999", "54.999999999"]);
		} finally {
			queue.dispose();
		}
	});

	it("does not expire an older queued barrier when an unqueued newer process succeeds", async () => {
		const { queue, create, handler } = setup();
		try {
			await queue.enqueue(create(55), handler);
			await queue.processOne(create(120));
			assert.equal(queue.getProcessedCheckpoint().timestamp, "54.999999999");
			assert.equal(queue.length(), 1);
		} finally {
			queue.dispose();
		}
	});

	it("holds a shared timestamp until every queued record at that timestamp succeeds", async () => {
		const { queue, create, handler, processNext } = setup();
		try {
			await queue.enqueueBatch([create(120), create(55), create(55)].map((process) => ({ process, handler })));
			await processNext();
			assert.equal(queue.getProcessedCheckpoint().timestamp, "54.999999999");
			await processNext();
			assert.equal(queue.getProcessedCheckpoint().timestamp, "54.999999999");
			await processNext();
			assert.equal(queue.getProcessedCheckpoint().timestamp, "89.999999999");
		} finally {
			queue.dispose();
		}
	});

	it("clamps barriers to the initial checkpoint and never moves successful progress backward", async () => {
		let replayFloor: string | undefined = "0.000000000";
		const initialCheckpoint = { timestamp: "100.000000000", index: 7 };
		const { queue, create } = setup({ getReplayFloor: () => replayFloor, initialCheckpoint });
		try {
			await queue.processOne(create(120));
			assert.deepEqual(queue.getProcessedCheckpoint(), initialCheckpoint);
			replayFloor = undefined;
			await queue.processOne(create(150));
			const advanced = queue.getProcessedCheckpoint();
			assert.deepEqual(advanced, { timestamp: "119.999999999", index: 0 });
			await queue.processOne(create(110));
			assert.deepEqual(queue.getProcessedCheckpoint(), advanced);
		} finally {
			queue.dispose();
		}
	});
});
