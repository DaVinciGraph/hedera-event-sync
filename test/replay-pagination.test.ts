import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient, RequestOptions } from "@davincigraph/hedera-rest-client";
import { keccak256, toBytes } from "viem";
import { LogsReadJob } from "../src/eventSync/core/jobs/LogsReadJob";
import { TransactionsReadJob } from "../src/eventSync/core/jobs/TransactionsReadJob";
import { ProcessStore } from "../src/eventSync/core/queue/ProcessStore";
import type { StagedProcess } from "../src/eventSync/core/queue/types";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import type { SyncHooks } from "../src/eventSync/hooks/SyncHooks";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { SyncContractLog, SyncContractLogsPage, SyncTransaction, SyncTransactionsPage } from "../src/eventSync/types/mirror";

const kinds = ["Single-Contract-Logs", "Multi-Contract-Logs", "Transactions"] as const;
type Kind = typeof kinds[number];
const address = `0x${"1".repeat(40)}`;
const topic = keccak256(toBytes("Ping()"));
const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const;
const ts = (seconds: number) => `${seconds}.000000000`;

class TestLogsReadJob extends LogsReadJob {
	runCycle(signal = new AbortController().signal) { return this.cycle(signal); }
	protected override safeHeadTimestamp() { return ts(300); }
	protected override async waitWithAbort() {
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
}

class TestTransactionsReadJob extends TransactionsReadJob {
	runCycle(signal = new AbortController().signal) { return this.cycle(signal); }
	protected override safeHeadTimestamp() { return ts(300); }
	protected override async waitWithAbort() {
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
}

function config(kind: Kind): RuntimeQueryConfig {
	const common = {
		id: 1,
		title: "overlap replay",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 1, maxPagesPerCycle: 1, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 30 },
			pauseAfterConsecutiveFailures: 3,
			status: { name: "RUNNING" },
		},
		process: {
			pauseAfterConsecutiveFailures: 3, maxQueuedProcesses: 100,
			holdPeriod: "none", nextDelayMs: 0, status: { name: "RUNNING" },
		},
	} as const;
	if (kind === "Transactions") {
		return { ...common, type: kind,
			params: { accountId: "0.0.123", timestamp: ts(0), index: 0, result: "success" },
			process: { ...common.process, skipTransactionTypes: [], unhandledTransactionPolicy: "error" },
		};
	}
	if (kind === "Multi-Contract-Logs") {
		return { ...common, type: kind, abi, params: { timestamp: ts(0), index: 0, topics: [topic] },
			process: { ...common.process, skipEventNames: [] },
		};
	}
	return { ...common, type: kind, abi, contract: { address }, params: { timestamp: ts(0), index: 0 },
		process: { ...common.process, skipEventNames: [] },
	};
}

type Request = { start: number; end: number; after?: number };

/** Simulates live range queries and strict timestamp continuation anchors. */
function fixture(kind: Kind, options: { hooks?: SyncHooks; windowMetadata?: boolean; maxPages?: number; overlap?: number } = {}) {
	const visible = new Set([100, 200]);
	const requests: Request[] = [];
	let requestFailure: Error | undefined;
	const makePage = async (request: Request, signal?: AbortSignal): Promise<SyncContractLogsPage | SyncTransactionsPage> => {
		requests.push({ ...request });
		if (signal?.aborted) throw new Error("cancelled request");
		if (requestFailure) {
			const error = requestFailure;
			requestFailure = undefined;
			throw error;
		}
		const remaining = [...visible].filter((value) => value >= request.start && value <= request.end && (request.after === undefined || value > request.after)).sort((a, b) => a - b);
		const values = remaining.slice(0, 1);
		const hasNext = remaining.length > 1;
		const nextRequest = { ...request, after: values[0] };
		const next = Object.assign(
			async (nextOptions?: RequestOptions) => hasNext ? makePage(nextRequest, nextOptions?.signal) : null,
			{ url: () => hasNext ? `https://mirror.example/next?after=${values[0]}` : null }
		);
		const common = {
			links: { next: next.url() }, next,
			...(request.after === undefined && options.windowMetadata !== false ? { syncWindowEnd: ts(request.end) } : {}),
		};
		if (kind === "Transactions") {
			return { ...common, transactions: values.map((value): SyncTransaction => ({
				name: "CRYPTOTRANSFER", consensus_timestamp: ts(value), nonce: 0,
				nft_transfers: [], token_transfers: [], transfers: [],
			})) } as SyncTransactionsPage;
		}
		return { ...common, logs: values.map((value): SyncContractLog => ({
			address, data: "0x", index: 1, timestamp: ts(value), topics: [topic],
		})) } as SyncContractLogsPage;
	};
	const fetch = ({ timestampGte, timestampLte, signal }: { timestampGte: string; timestampLte: string; signal?: AbortSignal }) =>
		makePage({ start: Number(timestampGte), end: Number(timestampLte) }, signal);
	const rest = { client: {} as HederaRestClient, fetchContractLogs: fetch, fetchGlobalLogs: fetch, fetchTransactions: fetch };
	const registry = new HandlerRegistry();
	registry.registerLog("Ping", { normalize: () => ({}), handle() {} });
	registry.registerTransaction("CRYPTOTRANSFER", { handle() {} });
	const store = new ProcessStore();
	const processed: number[] = [];
	const pruneFloors: string[] = [];
	const commit = async (items: readonly StagedProcess[]) => {
		const created = store.createProcesses(items.map((item) => item.process));
		processed.push(...created.map((item) => Number(item.pointerTS)));
		// Retention may delete records immediately; replay deduplication must survive it.
		for (const item of created) store.delete(item.id);
		return created;
	};
	const query = config(kind);
	if (options.maxPages !== undefined) query.read.fetch.maxPagesPerCycle = options.maxPages;
	if (options.overlap !== undefined) query.read.consistency.overlapSeconds = options.overlap;
	const prune = (floor: string) => { pruneFloors.push(floor); store.pruneSourceKeysBefore(query.id, floor); };
	const job = query.type === "Transactions"
		? new TestTransactionsReadJob(query, rest as never, registry, commit, prune, () => 0, 100, options.hooks ?? {})
		: new TestLogsReadJob(query, rest as never, registry, commit, prune, () => 0, 100, options.hooks ?? {});
	const cycle = async (signal?: AbortSignal) => {
		const before = requests.length;
		try { return await job.runCycle(signal); }
		finally { assert.ok(requests.length - before <= query.read.fetch.maxPagesPerCycle!, "every cycle respects its request budget"); }
	};
	return { job, query, visible, requests, processed, pruneFloors, cycle, failNextRequest(error: Error) { requestFailure = error; } };
}

describe("bounded forward pagination and overlap replay", () => {
	for (const kind of kinds) {
		it(`${kind}: recovers late records behind the continuation without duplicate processing or forward starvation`, async () => {
			const f = fixture(kind);
			await f.cycle();
			assert.deepEqual(f.processed, [100]);
			f.visible.add(90);
			await f.cycle();
			assert.deepEqual(f.requests[1], { start: 0, end: 100 });
			assert.deepEqual(f.processed, [100, 90]);
			await f.cycle();
			assert.deepEqual(f.requests[2], { start: 0, end: 100, after: 90 });
			assert.equal(f.job.getReplayFloor(), undefined);

			// It became visible behind the first replay's anchor. The next segment's
			// frozen overlap must still accept it after the forward cursor reaches 300.
			f.visible.add(80);
			await f.cycle();
			assert.deepEqual(f.requests[3], { start: 0, end: 300, after: 100 });
			assert.equal(f.job.getCursor().timestamp, ts(300));
			assert.equal(f.job.getReplayFloor(), ts(70));
			for (let page = 0; page < 4; page++) await f.cycle();
			assert.deepEqual(f.requests[4], { start: 70, end: 300 });
			assert.deepEqual(f.processed, [100, 90, 200, 80]);
			assert.equal(f.job.getReplayFloor(), undefined);
			assert.equal(f.pruneFloors.at(-1), ts(270));
			await f.cycle();
			assert.deepEqual(f.processed, [100, 90, 200, 80]);
		});

		it(`${kind}: preserves a replay continuation and floor when a request fails`, async () => {
			const f = fixture(kind);
			await f.cycle();
			f.visible.add(90);
			await f.cycle();
			f.failNextRequest(new Error("temporary mirror failure"));
			await assert.rejects(f.cycle(), /temporary mirror failure/);
			assert.equal(f.job.getReplayFloor(), ts(0));
			assert.deepEqual(f.processed, [100, 90]);
			await f.cycle();
			assert.deepEqual(f.requests[3], f.requests[2]);
			assert.equal(f.job.getReplayFloor(), undefined);
			assert.deepEqual(f.processed, [100, 90]);
		});

		it(`${kind}: retries an aborted replay page before releasing its floor`, async () => {
			let abortPage: AbortController | undefined;
			const f = fixture(kind, { hooks: { onReadPage() { abortPage?.abort(); } } });
			await f.cycle();
			f.visible.add(90);
			abortPage = new AbortController();
			await f.cycle(abortPage.signal);
			assert.equal(f.job.getReplayFloor(), ts(0));
			assert.deepEqual(f.processed, [100]);
			abortPage = undefined;
			await f.cycle();
			assert.deepEqual(f.requests[2], f.requests[1]);
			assert.deepEqual(f.processed, [100, 90]);
			await f.cycle();
			assert.equal(f.job.getReplayFloor(), undefined);
		});

		it(`${kind}: keeps read.until pending until dense replay is complete`, async () => {
			let firstCycle = true;
			const f = fixture(kind, { hooks: {
				onReadCycleCompleted() {
					if (firstCycle) { firstCycle = false; f.visible.add(90); }
				},
			} });
			const predicateCalls: number[] = [];
			f.query.read.until = (cursor) => { predicateCalls.push(Number(cursor.timestamp)); return Number(cursor.timestamp) >= 100; };
			const loop = f.job.runForever();
			try {
				for (let tick = 0; tick < 100 && f.job.getStatus().name !== "PAUSED"; tick++) {
					await new Promise<void>((resolve) => setImmediate(resolve));
				}
				assert.equal(f.job.getStatus().name, "PAUSED");
				assert.deepEqual(predicateCalls, [100]);
				assert.deepEqual(f.processed, [100, 90]);
				assert.equal(f.requests.length, 3);
				assert.equal(f.job.getReplayFloor(), undefined);
			} finally {
				f.job.abort("test complete");
				await loop;
			}
		});

		it(`${kind}: finishes pages without window metadata at the last actual record`, async () => {
			const f = fixture(kind, { windowMetadata: false, maxPages: 10 });
			await f.cycle();
			assert.equal(f.job.getCursor().timestamp, ts(200));
			await f.cycle();
			assert.equal(f.job.getCursor().timestamp, ts(200));
			assert.deepEqual(f.processed, [100, 200]);
		});

		it(`${kind}: counts consecutive until failures across deferred pagination cycles`, async () => {
			const failures: number[] = [];
			const f = fixture(kind, { hooks: {
				onReadFailure(_query, error, consecutiveFailures) {
					assert.match((error as Error).message, /predicate unavailable/);
					failures.push(consecutiveFailures);
				},
			} });
			f.query.read.pauseAfterConsecutiveFailures = 2;
			f.query.read.until = () => { throw new Error("predicate unavailable"); };
			const loop = f.job.runForever();
			try {
				for (let tick = 0; tick < 100 && f.job.getStatus().name !== "PAUSED"; tick++) {
					await new Promise<void>((resolve) => setImmediate(resolve));
				}
				assert.equal(f.job.getStatus().name, "PAUSED");
				assert.deepEqual(failures, [1, 2]);
				assert.deepEqual(f.processed, [100, 200]);
			} finally {
				f.job.abort("test complete");
				await loop;
			}
		});

		it(`${kind}: recovers between fetch failures while until is deferred`, async () => {
			const failures: number[] = [];
			const thresholds: number[] = [];
			let replayFailureScheduled = false;
			const f = fixture(kind, { hooks: {
				onReadCycleCompleted(_query, result) {
					if (result.success && !replayFailureScheduled) {
						replayFailureScheduled = true;
						f.failNextRequest(new Error("replay fetch failed"));
					}
				},
				onReadFailure(_query, _error, count) { failures.push(count); },
				onReadConsecutiveFailuresReached(_query, count) { thresholds.push(count); },
			} });
			f.query.read.pauseAfterConsecutiveFailures = 2;
			f.query.read.until = () => false;
			f.failNextRequest(new Error("forward fetch failed"));
			const loop = f.job.runForever();
			try {
				for (let tick = 0; tick < 100 && !f.processed.includes(200) && f.job.getStatus().name !== "PAUSED"; tick++) {
					await new Promise<void>((resolve) => setImmediate(resolve));
				}
				assert.deepEqual(failures, [1, 1]);
				assert.deepEqual(thresholds, []);
				assert.deepEqual(f.processed, [100, 200]);
				assert.equal(f.job.getStatus().name, "RUNNING");
			} finally {
				f.job.abort("test complete");
				await loop;
			}
		});
	}
});
