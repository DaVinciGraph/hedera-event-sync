import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Abi } from "abitype";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { keccak256, toBytes } from "viem";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { QueryRuntime } from "../src/eventSync/core/QueryRuntime";
import { HookBase } from "../src/eventSync/hooks/HookBase";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { Process } from "../src/eventSync/types/domain";
import type { SyncContractLog, SyncContractLogsPage } from "../src/eventSync/types/mirror";
import type { RestClient } from "../src/eventSync/rest/RestClient";

const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const satisfies Abi;

function query(): Extract<RuntimeQueryConfig, { type: "Single-Contract-Logs" }> {
	return {
		type: "Single-Contract-Logs",
		id: 77,
		title: "full log pipeline",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 10, maxPagesPerCycle: 10, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
			pauseAfterConsecutiveFailures: 3,
			status: { name: "RUNNING" },
		},
		contract: { address: `0x${"1".repeat(40)}` },
		params: { timestamp: "1.000000000", index: 0 },
		abi,
		process: { pauseAfterConsecutiveFailures: 3, maxQueuedProcesses: 10, holdPeriod: "none", nextDelayMs: 0, skipEventNames: [], status: { name: "RUNNING" } },
	};
}

function page(log: SyncContractLog): SyncContractLogsPage {
	return {
		logs: [log],
		links: { next: null },
		next: Object.assign(async () => null, { url: () => null }),
		syncWindowEnd: "3.000000000",
	} as SyncContractLogsPage;
}

describe("single-contract log runtime", () => {
	it("reads, decodes, normalizes, queues, handles, and checkpoints one mirror log", async () => {
		const log: SyncContractLog = {
			address: `0x${"1".repeat(40)}`,
			block_number: 1,
			contract_id: "0.0.123",
			data: "0x",
			index: 2,
			timestamp: "2.000000000",
			topics: [keccak256(toBytes("Ping()"))],
			transaction_hash: `0x${"2".repeat(64)}`,
		};
		const rest = {
			client: {} as HederaRestClient,
			fetchContractLogs: async () => page(log),
		} as unknown as RestClient;
		const registry = new HandlerRegistry();
		let normalized = false;
		let handled = false;
		registry.registerLog("Ping", {
			normalize(_args, context) {
				normalized = context.raw === log;
				return { ok: true };
			},
			async handle(data, context) {
				handled = (data as { ok: boolean }).ok && context.raw === log;
			},
		});
		const runtime = new QueryRuntime(query(), rest, registry, new HookBase());
		const readJob = (runtime as unknown as { readJob: { cycle: (signal: AbortSignal) => Promise<unknown[]> } }).readJob;

		const read = await readJob.cycle(new AbortController().signal);
		assert.equal(read.length, 1);
		assert.equal(runtime.queue.length(), 1);
		const queued = (runtime.queue as unknown as { q: Process[] }).q.shift();
		assert.ok(queued);
		await runtime.queue.processOne(queued);

		assert.equal(normalized, true);
		assert.equal(handled, true);
		assert.equal(queued.status, "Processed");
		assert.deepEqual(runtime.getProcessedCheckpoint(), { timestamp: "1.999999999", index: 0 });
		assert.equal(runtime.store.countByQuery(77), 0);
		await runtime.stop();
	});
});
