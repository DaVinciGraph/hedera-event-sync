import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Abi } from "abitype";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { encodeAbiParameters, toEventSelector } from "viem";
import { HederaEventSync } from "../src/eventSync/core/HederaEventSync";
import { QueryRuntime } from "../src/eventSync/core/QueryRuntime";
import { LogEventProcessor } from "../src/eventSync/core/logs/LogEventProcessor";
import { DecodeError, LifecycleReentrancyError } from "../src/eventSync/errors";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import type { SyncHooks } from "../src/eventSync/hooks/SyncHooks";
import type { RestClient } from "../src/eventSync/rest/RestClient";
import type { MultiContractLogsParams, RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { Cursor, Process, ProcessedCheckpoint } from "../src/eventSync/types/domain";
import type { SyncContractLog, SyncContractLogsPage } from "../src/eventSync/types/mirror";
import { assertOutsideUserCallback, associateUserCallbackOwner } from "../src/eventSync/utils/userCallback";

const pairAddress = `0x${"1".repeat(40)}`;
const foreignAddress = `0x${"2".repeat(40)}`;
const transferAbi = [{
	type: "event", name: "Transfer", anonymous: false,
	inputs: [
		{ name: "from", type: "address", indexed: true },
		{ name: "to", type: "address", indexed: true },
		{ name: "value", type: "uint256", indexed: false },
	],
}] as const satisfies Abi;
const transferTopic = toEventSelector(transferAbi[0]);
const initialCursor: Cursor = { timestamp: "1.000000000", index: 0 };

function log(kind: "ERC20" | "ERC721", timestamp = "2.000000000"): SyncContractLog {
	const amount = encodeAbiParameters([{ type: "uint256" }], [7n]);
	return {
		address: kind === "ERC20" ? pairAddress : foreignAddress,
		timestamp, index: 1,
		data: kind === "ERC20" ? amount : "0x",
		topics: [
			transferTopic,
			encodeAbiParameters([{ type: "address" }], [pairAddress as `0x${string}`]),
			encodeAbiParameters([{ type: "address" }], [foreignAddress as `0x${string}`]),
			...(kind === "ERC721" ? [amount] : []),
		],
	};
}

function processor(registry = new HandlerRegistry(), owner?: object) {
	return new LogEventProcessor(associateUserCallbackOwner({
		queryId: 77, registry, restClient: {} as HederaRestClient,
		network: "testnet", provider: "public",
	}, owner));
}

function query(logFilter?: MultiContractLogsParams["logFilter"]): Extract<RuntimeQueryConfig, { type: "Multi-Contract-Logs" }> {
	return {
		type: "Multi-Contract-Logs", id: 77, title: "Global pair transfers", abi: transferAbi,
		contract: { type: "PAIR", version: "V1" },
		params: { topics: [transferTopic], ...initialCursor, logFilter },
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 30, batchSize: 100, maxPagesPerCycle: 1, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
			pauseAfterConsecutiveFailures: 3, status: { name: "RUNNING" },
		},
		process: { pauseAfterConsecutiveFailures: 3, maxQueuedProcesses: 100, holdPeriod: "none", nextDelayMs: 0, skipEventNames: [], status: { name: "RUNNING" } },
	};
}

function runtime(config: ReturnType<typeof query>, logs: SyncContractLog[], hooks: SyncHooks = {}, owner?: object) {
	const registry = new HandlerRegistry();
	const handled: string[] = [];
	registry.registerLog("Transfer", {
		normalize: (args) => args,
		handle: (_data, context) => { handled.push(context.raw.timestamp); },
	});
	const rest = {
		client: {} as HederaRestClient,
		fetchGlobalLogs: async () => ({
			logs, links: { next: null }, next: Object.assign(async () => null, { url: () => null }),
		} as SyncContractLogsPage),
	} as unknown as RestClient;
	const instance = new QueryRuntime(config, rest, registry, hooks, owner);
	// Drive one real reader/queue cycle at a time without timers or network access.
	const reader = (instance as unknown as {
		readJob: { cycle(signal: AbortSignal): Promise<SyncContractLog[]> };
	}).readJob;
	const processNext = async () => {
		const next = (instance.queue as unknown as { q: Process[] }).q.shift();
		assert.ok(next);
		await instance.queue.processOne(next);
	};
	return { instance, reader, handled, processNext };
}

describe("predecode log filters", () => {
	it("rejects a real ERC721 Transfer before decoding it with the pair ABI or calling a resolver", async () => {
		const raw = log("ERC721");
		const signal = new AbortController().signal;
		const result = await processor().prepare(raw, {
			abi: transferAbi, signal,
			logFilter: async (received, receivedSignal) => {
				assert.notEqual(received, raw);
				assert.deepEqual(received, raw);
				assert.equal(receivedSignal, signal);
				return false;
			},
			contractResolver: () => assert.fail("Rejected logs must not reach the resolver"),
		});
		assert.equal(result, null);
	});

	for (const logFilter of [undefined, () => true]) {
		it(`preserves strict decoding when the filter is ${logFilter ? "accepting" : "absent"}`, async () => {
			await assert.rejects(processor().prepare(log("ERC721"), {
				abi: transferAbi, logFilter,
				contractResolver: () => assert.fail("Malformed accepted logs must fail before resolution"),
			}), (error: unknown) => {
				assert.ok(error instanceof DecodeError);
				assert.match(error.message, /exactly 3 topics; received 4/);
				return true;
			});
		});
	}

	it("decodes and handles accepted logs in filter, resolver, normalize, handle order", async () => {
		const order: string[] = [];
		const registry = new HandlerRegistry();
		registry.registerLog("Transfer", {
			normalize(args) { order.push("normalize"); return args; },
			handle(data) { order.push("handle"); assert.equal((data as { value: bigint }).value, 7n); },
		});
		await processor(registry).prepareAndExecute(log("ERC20"), {
			abi: transferAbi,
			logFilter() { order.push("filter"); return true; },
			contractResolver() { order.push("resolver"); return { address: pairAddress }; },
		});
		assert.deepEqual(order, ["filter", "resolver", "normalize", "handle"]);
	});

	it("isolates decoding and raw identity from attempted predicate mutations", async () => {
		const raw = log("ERC20");
		const registry = new HandlerRegistry();
		registry.registerLog("Transfer", { normalize: (args) => args, handle() {} });
		const prepared = await processor(registry).prepare(raw, {
			abi: transferAbi,
			logFilter(snapshot) {
				const unsafe = snapshot as SyncContractLog;
				assert.throws(() => { unsafe.address = foreignAddress; }, TypeError);
				assert.throws(() => { unsafe.timestamp = "999.000000000"; }, TypeError);
				assert.throws(() => { unsafe.topics.push("0x01"); }, TypeError);
				assert.throws(() => { unsafe.data = "0x"; }, TypeError);
				return true;
			},
			contractResolver(source) { assert.equal(source, raw); return { address: source.address }; },
		});
		assert.equal(prepared?.rawLog, raw);
		assert.equal(prepared?.contract.address, pairAddress);
		assert.equal((prepared?.data as { value: bigint }).value, 7n);
		assert.equal(raw.timestamp, "2.000000000");
		assert.equal(Object.isFrozen(raw), false);
		assert.equal(Object.isFrozen(raw.topics), false);
	});

	it("fails closed when a JavaScript predicate returns a nonboolean", async () => {
		await assert.rejects(processor().prepare(log("ERC721"), {
			abi: transferAbi, logFilter: (() => undefined) as unknown as MultiContractLogsParams["logFilter"],
		}), /logFilter must return a boolean/);
	});

	it("retries a throwing predicate without committing the earlier accepted log or page cursor", async () => {
		let uncertain = true;
		const failure = new Error("Emitter lookup unavailable");
		const calls: string[] = [];
		const config = query((raw) => {
			calls.push(raw.timestamp);
			if (raw.address === foreignAddress && uncertain) throw failure;
			return raw.address === pairAddress;
		});
		const { instance, reader, handled, processNext } = runtime(config, [log("ERC20"), log("ERC721", "3.000000000")]);
		try {
			await assert.rejects(reader.cycle(new AbortController().signal), (error) => error === failure);
			assert.equal(instance.countProcesses(), 0);
			assert.deepEqual(instance.getReadStatus().cursor, initialCursor);
			assert.deepEqual(instance.getProcessedCheckpoint(), initialCursor);
			assert.deepEqual(handled, []);

			uncertain = false;
			await reader.cycle(new AbortController().signal);
			assert.equal(instance.countProcesses("Queued"), 1);
			assert.deepEqual(calls, ["2.000000000", "3.000000000", "2.000000000", "3.000000000"]);
			await processNext();
			assert.deepEqual(handled, ["2.000000000"]);
		} finally { await instance.stop(); }
	});

	it("never checkpoints a later rejected log ahead of the earlier accepted handler", async () => {
		const notifications: ProcessedCheckpoint[] = [];
		const { instance, reader, handled, processNext } = runtime(
			query((raw) => raw.address === pairAddress), [log("ERC20"), log("ERC721", "3.000000000")],
			{ onProcessedCheckpoint(_query, checkpoint) { notifications.push({ ...checkpoint }); } }
		);
		try {
			await reader.cycle(new AbortController().signal);
			assert.deepEqual(instance.getReadStatus().cursor, { timestamp: "3.000000000", index: 1 });
			assert.deepEqual(instance.getProcessedCheckpoint(), initialCursor);
			assert.deepEqual(notifications, []);
			assert.deepEqual(handled, []);
			await processNext();
			assert.deepEqual(handled, ["2.000000000"]);
			// Checkpoints retain the nanosecond before the successful boundary so
			// a restart can replay every index at that timestamp.
			assert.deepEqual(instance.getProcessedCheckpoint(), { timestamp: "1.999999999", index: 0 });
			assert.deepEqual(notifications, [{ timestamp: "1.999999999", index: 0 }]);
		} finally { await instance.stop(); }
	});

	it("lets rejected-only pages advance reading without inventing durable processing progress", async () => {
		const { instance, reader } = runtime(query(() => false), [log("ERC721")], {
			onProcessedCheckpoint: () => assert.fail("Rejected logs cannot emit a processed checkpoint"),
		});
		try {
			await reader.cycle(new AbortController().signal);
			assert.deepEqual(instance.getReadStatus().cursor, { timestamp: "2.000000000", index: 1 });
			assert.deepEqual(instance.getProcessedCheckpoint(), initialCursor);
			assert.equal(instance.countProcesses(), 0);
		} finally { await instance.stop(); }
	});

	it("cancels a pending filter without decoding or committing any part of the page", async () => {
		let release!: () => void;
		let started!: () => void;
		const blocked = new Promise<void>((resolve) => { release = resolve; });
		const entered = new Promise<void>((resolve) => { started = resolve; });
		const controller = new AbortController();
		const config = query(async (raw, signal) => {
			assert.equal(signal, controller.signal);
			if (raw.address === foreignAddress) { started(); await blocked; }
			return true;
		});
		const { instance, reader } = runtime(config, [log("ERC20"), log("ERC721", "3.000000000")]);
		try {
			const cycle = reader.cycle(controller.signal);
			await entered;
			controller.abort();
			release();
			await assert.rejects(cycle, (error) => error === controller.signal.reason);
			assert.equal(instance.countProcesses(), 0);
			assert.deepEqual(instance.getReadStatus().cursor, initialCursor);
			assert.deepEqual(instance.getProcessedCheckpoint(), initialCursor);
		} finally { release(); await instance.stop(); }
	});

	it("guards lifecycle reentrancy for both standalone and managed filters", async () => {
		const owner = {};
		const logFilter: NonNullable<MultiContractLogsParams["logFilter"]> = async () => {
			await Promise.resolve();
			assert.throws(() => assertOutsideUserCallback("pauseRead", owner), (error: unknown) => {
				assert.ok(error instanceof LifecycleReentrancyError);
				assert.deepEqual(error.callback, { kind: "filter", name: "logFilter", queryId: 77 });
				return true;
			});
			return false;
		};
		await processor(undefined, owner).prepare(log("ERC721"), { abi: transferAbi, logFilter });
		const { instance, reader } = runtime(query(logFilter), [log("ERC721")], {}, owner);
		try { await reader.cycle(new AbortController().signal); }
		finally { await instance.stop(); }
		assert.doesNotThrow(() => assertOutsideUserCallback("pauseRead", owner));
	});

	it("preserves filter functions across normalization, snapshots, reload and distinct query IDs", async () => {
		let catalog = [query(() => false)];
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => catalog });
		const first = () => false;
		const second = () => true;
		try {
			catalog = [query(first)];
			await sync.start();
			const snapshot = sync.getQuery(77);
			assert.ok(snapshot?.type === "Multi-Contract-Logs");
			assert.equal(snapshot.params.logFilter, first);
			snapshot.params.logFilter = second;
			const current = sync.getQuery(77);
			assert.ok(current?.type === "Multi-Contract-Logs");
			assert.equal(current.params.logFilter, first);
			catalog = [query(first), { ...query(first), id: 78 }, { ...query(second), id: 79 }];
			await sync.reloadQueries();
			assert.ok(sync.getQuery(78), "Distinct query IDs remain authoritative even for identical callbacks");
			assert.ok(sync.getQuery(79), "Distinct callbacks must not be considered duplicate configs");
			catalog = [query(second)];
			await sync.reloadQueries();
			assert.equal(sync.getQuery(79), undefined);
			const updated = sync.getQuery(77);
			assert.ok(updated?.type === "Multi-Contract-Logs");
			assert.equal(updated.params.logFilter, second);
		} finally { await sync.shutdown(); }
	});

	it("validates callback shape before starting or replacing a query", async () => {
		let catalog = [query(() => false)];
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => catalog });
		try {
			const invalid = query("allow" as unknown as MultiContractLogsParams["logFilter"]);
			catalog = [invalid];
			await assert.rejects(sync.start(), /logFilter must be a function/);
			assert.equal(sync.getQuery(77), undefined);
			const logFilter = () => false;
			catalog = [query(logFilter)];
			await sync.start();
			catalog = [invalid];
			await assert.rejects(sync.reloadQueries(), /logFilter must be a function/);
			const current = sync.getQuery(77);
			assert.ok(current?.type === "Multi-Contract-Logs");
			assert.equal(current.params.logFilter, logFilter);
		} finally { await sync.shutdown(); }
	});
});
