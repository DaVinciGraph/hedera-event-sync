import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Abi } from "abitype";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { keccak256, toBytes } from "viem";
import { LogsReadJob } from "../src/eventSync/core/jobs/LogsReadJob";
import type { StagedProcess } from "../src/eventSync/core/queue/types";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { HookBase } from "../src/eventSync/hooks/HookBase";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { Process } from "../src/eventSync/types/domain";
import type { SyncContractLog, SyncContractLogsPage } from "../src/eventSync/types/mirror";

const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const satisfies Abi;
const canonicalAddress = `0x${"1".repeat(40)}`;

function query(): Extract<RuntimeQueryConfig, { type: "Single-Contract-Logs" }> {
	return {
		type: "Single-Contract-Logs",
		id: 77,
		title: "log ingestion",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 100, maxPagesPerCycle: 10, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
			pauseAfterConsecutiveFailures: 3,
			status: { name: "RUNNING" },
		},
		contract: { address: canonicalAddress },
		params: { timestamp: "1.000000000", index: 0 },
		abi,
		process: { pauseAfterConsecutiveFailures: 3, maxQueuedProcesses: 100, holdPeriod: "none", nextDelayMs: 0, skipEventNames: [], status: { name: "RUNNING" } },
	};
}

function log(timestamp = "2.000000000", index = 2): SyncContractLog {
	return {
		address: canonicalAddress,
		block_number: 1,
		contract_id: "0.0.123",
		data: "0x",
		index,
		timestamp,
		topics: [keccak256(toBytes("Ping()"))],
		transaction_hash: `0x${"2".repeat(64)}`,
	} as SyncContractLog;
}

function page(logs: SyncContractLog[]): SyncContractLogsPage {
	return {
		logs,
		links: { next: null },
		next: Object.assign(async () => null, { url: () => null }),
	} as SyncContractLogsPage;
}

class ExposedLogsReadJob extends LogsReadJob {
	runCycle(signal: AbortSignal) {
		return this.cycle(signal);
	}
}

describe("log page ingestion", () => {
	it("uses contract.address for single-contract Mirror Node requests", async () => {
		const config = query();
		let requestedAddress: string | undefined;
		const job = new ExposedLogsReadJob(
			config,
			{
				client: {} as HederaRestClient,
				fetchContractLogs: async ({ address }: { address: string }) => {
					requestedAddress = address;
					return page([]);
				},
			} as never,
			new HandlerRegistry(),
			async () => [],
			() => {},
			() => 0,
			100,
			new HookBase()
		);

		await job.runCycle(new AbortController().signal);

		assert.equal(requestedAddress, canonicalAddress);
	});

	it("keeps source identity and cursor bookkeeping independent from raw-log mutation", async () => {
		const raw = log();
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", {
			normalize(_args, context) {
				const mutableRaw = context.raw as SyncContractLog;
				mutableRaw.timestamp = "999.000000000";
				mutableRaw.index = 9;
				mutableRaw.transaction_hash = "mutated-hash";
				mutableRaw.address = "mutated-address";
				return { ok: true };
			},
			async handle() {},
		});
		let staged: readonly StagedProcess[] = [];
		const job = new ExposedLogsReadJob(
			query(),
			{ client: {} as HederaRestClient, fetchContractLogs: async () => page([raw]) } as never,
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
		assert.equal(staged[0].process.pointerTS, "2.000000000");
		assert.equal(staged[0].process.pointerIDX, 2);
		assert.equal(staged[0].process.sourceKey, "log:77:2.000000000:2");
		assert.deepEqual(job.getCursor(), { timestamp: "2.000000000", index: 2 });
	});

	it("uses one provider-independent dedupe key for the same canonical log", async () => {
		const first = log();
		const second = {
			...log(),
			address: `0x${"a".repeat(40)}`,
			transaction_hash: `0x${"b".repeat(64)}`,
		};
		let fetches = 0;
		const sourceKeys: string[] = [];
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", { normalize: () => ({ ok: true }), async handle() {} });
		const job = new ExposedLogsReadJob(
			query(),
			{ client: {} as HederaRestClient, fetchContractLogs: async () => page([fetches++ === 0 ? first : second]) } as never,
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

		assert.deepEqual(sourceKeys, ["log:77:2.000000000:2", "log:77:2.000000000:2"]);
		assert.equal(new Set(sourceKeys).size, 1);
	});

	it("does not advance the cursor when abort interrupts asynchronous normalization", async () => {
		let normalizationStarted!: () => void;
		const started = new Promise<void>((resolve) => { normalizationStarted = resolve; });
		let releaseNormalization!: () => void;
		const blocked = new Promise<void>((resolve) => { releaseNormalization = resolve; });
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", {
			async normalize() {
				normalizationStarted();
				await blocked;
				return { ok: true };
			},
			async handle() {},
		});
		let commits = 0;
		const job = new ExposedLogsReadJob(
			query(),
			{ client: {} as HederaRestClient, fetchContractLogs: async () => page([log()]) } as never,
			registry,
			async () => { commits++; return []; },
			() => {},
			() => 0,
			100,
			new HookBase()
		);
		const controller = new AbortController();

		const cycle = job.runCycle(controller.signal);
		await started;
		controller.abort();
		releaseNormalization();
		const items = await cycle;

		assert.deepEqual(items, []);
		assert.equal(commits, 0);
		assert.deepEqual(job.getCursor(), { timestamp: "1.000000000", index: 0 });
	});

	it("discards earlier staged items when abort interrupts a later item", async () => {
		let calls = 0;
		let laterStarted!: () => void;
		const started = new Promise<void>((resolve) => { laterStarted = resolve; });
		let releaseLater!: () => void;
		const blocked = new Promise<void>((resolve) => { releaseLater = resolve; });
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", {
			async normalize() {
				calls++;
				if (calls === 2) {
					laterStarted();
					await blocked;
				}
				return { ok: true };
			},
			async handle() {},
		});
		let commits = 0;
		const job = new ExposedLogsReadJob(
			query(),
			{ client: {} as HederaRestClient, fetchContractLogs: async () => page([log("2.000000000", 1), log("3.000000000", 2)]) } as never,
			registry,
			async () => { commits++; return []; },
			() => {},
			() => 0,
			100,
			new HookBase()
		);
		const controller = new AbortController();

		const cycle = job.runCycle(controller.signal);
		await started;
		controller.abort();
		releaseLater();
		const items = await cycle;

		assert.equal(calls, 2);
		assert.deepEqual(items, []);
		assert.equal(commits, 0);
		assert.deepEqual(job.getCursor(), { timestamp: "1.000000000", index: 0 });
	});

	it("does not advance the cursor when abort races a page commit", async () => {
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", { normalize: () => ({ ok: true }), async handle() {} });
		let commitStarted!: () => void;
		const started = new Promise<void>((resolve) => { commitStarted = resolve; });
		let releaseCommit!: () => void;
		const blocked = new Promise<void>((resolve) => { releaseCommit = resolve; });
		let committedItems = 0;
		const job = new ExposedLogsReadJob(
			query(),
			{ client: {} as HederaRestClient, fetchContractLogs: async () => page([log()]) } as never,
			registry,
			async (items) => {
				committedItems = items.length;
				commitStarted();
				await blocked;
				return [];
			},
			() => {},
			() => 0,
			100,
			new HookBase()
		);
		const controller = new AbortController();

		const cycle = job.runCycle(controller.signal);
		await started;
		controller.abort();
		releaseCommit();
		const items = await cycle;

		assert.equal(committedItems, 1);
		assert.deepEqual(items, []);
		assert.deepEqual(job.getCursor(), { timestamp: "1.000000000", index: 0 });
	});
});
