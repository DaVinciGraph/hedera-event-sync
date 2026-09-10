import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { QueryRuntime } from "../src/eventSync/core/QueryRuntime";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { HookBase } from "../src/eventSync/hooks/HookBase";
import { RestClient } from "../src/eventSync/rest/RestClient";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";

function query(): Extract<RuntimeQueryConfig, { type: "Transactions" }> {
	return {
		type: "Transactions",
		id: 61,
		title: "background supervision",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 1, maxPagesPerCycle: 1, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
			pauseAfterConsecutiveFailures: 1,
			status: { name: "RUNNING" },
		},
		params: { accountId: "0.0.98", timestamp: "1.000000000", index: 0, result: "success" },
		process: {
			pauseAfterConsecutiveFailures: 1,
			maxQueuedProcesses: 1,
			holdPeriod: "none",
			nextDelayMs: 0,
			skipTransactionTypes: [],
			unhandledTransactionPolicy: "error",
			status: { name: "RUNNING" },
		},
	};
}

describe("QueryRuntime background supervision", () => {
	it("observes an unexpected loop rejection and fails both runtime halves", async () => {
		const runtime = new QueryRuntime(
			query(),
			new RestClient({} as HederaRestClient),
			new HandlerRegistry(),
			new HookBase()
		);
		const internals = runtime as unknown as {
			readJob: { initialize: () => Promise<void>; runForever: () => Promise<void> };
		};
		internals.readJob.initialize = async () => {};
		internals.readJob.runForever = async () => { throw "read loop exploded"; };
		runtime.queue.runLoop = async () => {};

		await runtime.start();
		await new Promise<void>((resolve) => setImmediate(resolve));
		assert.equal(runtime.getReadStatus().status.name, "FAILED");
		assert.equal(runtime.getQueueStatus().status.name, "FAILED");
		assert.match(runtime.getReadStatus().status.info ?? "", /read loop exploded/);
		await runtime.stop();
	});
});
