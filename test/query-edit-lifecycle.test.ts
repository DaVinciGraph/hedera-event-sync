import assert from "node:assert/strict";
import { test } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type QueryConfig, type QueryStartState, type ProcessedCheckpoint } from "../src/index";

const query = (id = 101): QueryConfig => ({
	type: "Transactions", id, title: "Editable query",
	read: { fetch: { network: "testnet", restProvider: "public", batchSize: 10, pollIntervalSeconds: 1 }, pauseAfterConsecutiveFailures: 2, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
	params: { accountId: "0.0.123", timestamp: "1700000000.123456789", index: 2 },
	process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10 },
});
const paused: QueryStartState = { read: { name: "PAUSED", info: "manual read pause" }, process: { name: "PAUSED", info: "consecutive failures" } };
const tick = () => new Promise(resolve => setTimeout(resolve, 20));

test("validation is side-effect free, and initial pauses exist before hooks and network access", async () => {
	let reads = 0, hooks = 0;
	let catalog: QueryConfig[] = [{ ...query(), initialState: paused }];
	let finalCheckpoint: ProcessedCheckpoint | undefined;
	const sync = await HederaEventSync.create({ restClient: { useProvider() { reads++; throw new Error("must not read"); } } as unknown as HederaRestClient, singletonGuard: false, querySource: () => catalog, beforeReplace: ({ checkpoints }) => { finalCheckpoint = checkpoints.find(item => item.query.id === 101)?.checkpoint; } });
	sync.setHooks({ onQueryAdded(config) { hooks++; assert.deepEqual(config.read.status, paused.read); assert.deepEqual(config.process.status, paused.process); } });
	try {
		const normalized = sync.validateQuery(query(), paused);
		assert.equal(normalized.params.timestamp, "1700000000.123456789");
		assert.equal(normalized.params.index, 2);
		assert.equal(sync.listQueries().length, 0); assert.equal(hooks, 0); assert.equal(reads, 0);
		assert.throws(() => sync.validateQuery({ ...query(), title: "" }), /title/);
		for (const state of [null, [], { read: { name: "FAILED" } }, { process: { name: "PAUSED", info: 1 } }]) assert.throws(() => sync.validateQuery(query(), state as QueryStartState), /initialState/);
		await sync.start();
		await tick();
		assert.deepEqual(sync.getReadStatus(101)?.status, paused.read);
		assert.deepEqual(sync.getQueueStatus(101)?.status, paused.process);
		await sync.pauseRead(101, { preserveExistingPause: true });
		await sync.pauseProcessing(101, { preserveExistingPause: true });
		assert.deepEqual(sync.getReadStatus(101)?.status, paused.read);
		assert.deepEqual(sync.getQueueStatus(101)?.status, paused.process);
		assert.equal(reads, 0);
		catalog = [{ ...query(), title: "Edited", initialState: paused }];
		await sync.reloadQueries();
		assert.equal(reads, 0);
		catalog = [];
		await sync.reloadQueries();
		assert.deepEqual(finalCheckpoint, { timestamp: "1700000000.123456789", index: 2 });
		assert.equal(sync.getQuery(101), undefined);
	} finally { await sync.shutdown(); }
});

test("preserveExistingPause does not replace an in-progress processing failure reason", { timeout: 10000 }, async () => {
	let release!: () => void, failed!: () => void;
	const gate = new Promise<void>(resolve => { release = resolve; });
	const reached = new Promise<void>(resolve => { failed = resolve; });
	const builder: any = new Proxy({}, { get: () => () => builder });
	const next = Object.assign(async () => null, { url: () => null });
	const rest: any = { useProvider() { return this; }, useNetwork() { return this; }, transactions() { return { list(cb: any) { cb(builder); return { get: async () => ({ transactions: [{ name: "CRYPTOTRANSFER", consensus_timestamp: "1700000001.123456789", transaction_id: "0.0.123-1700000001-123456789", transaction_hash: "failure", result: "SUCCESS", nonce: 0, transfers: [], token_transfers: [], nft_transfers: [] }], next }) }; } }; } };
	const input = query(); input.process.pauseAfterConsecutiveFailures = 1;
	const sync = await HederaEventSync.create({ restClient: rest, singletonGuard: false, querySource: () => [input] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle() { throw new Error("handler failed"); } });
	sync.setHooks({ async onProcessConsecutiveFailuresReached() { failed(); await gate; } });
	try {
		await sync.start(); await reached;
		assert.equal(sync.getQueueStatus(101)?.status.name, "PAUSING");
		const pausing = sync.pauseProcessing(101, { preserveExistingPause: true });
		await tick(); release(); await pausing;
		assert.equal(sync.getQueueStatus(101)?.status.name, "PAUSED");
		assert.match(sync.getQueueStatus(101)?.status.info ?? "", /consecutive failures/);
	} finally { release(); await sync.shutdown(); }
});

test("reload persists the checkpoint after active handler and checkpoint hooks settle, before queued records are disposed", { timeout: 10000 }, async () => {
	let releaseHandler!: () => void, started!: () => void, releaseHook!: () => void, hookStarted!: () => void;
	const handlerGate = new Promise<void>(resolve => { releaseHandler = resolve; });
	const handlerStarted = new Promise<void>(resolve => { started = resolve; });
	const hookGate = new Promise<void>(resolve => { releaseHook = resolve; });
	const checkpointHookStarted = new Promise<void>(resolve => { hookStarted = resolve; });
	const builder: any = new Proxy({}, { get: () => () => builder });
	const next = Object.assign(async () => null, { url: () => null });
	const transactions = ["1700000001.123456789", "1700000002.987654321"].map((consensus_timestamp, index) => ({ name: "CRYPTOTRANSFER", consensus_timestamp, transaction_id: `0.0.123-${consensus_timestamp}`, transaction_hash: `hash${index}`, result: "SUCCESS", nonce: 0, transfers: [], token_transfers: [], nft_transfers: [] }));
	const restClient: any = { useProvider() { return this; }, useNetwork() { return this; }, transactions() { return { list(cb: any) { cb(builder); return { get: async () => ({ transactions, next }) }; } }; } };
	let catalog = [query()];
	let finalCheckpoint: ProcessedCheckpoint | undefined;
	const sync = await HederaEventSync.create({ restClient, singletonGuard: false, querySource: () => catalog, beforeReplace: ({ checkpoints }) => { finalCheckpoint = checkpoints.find(item => item.query.id === 101)?.checkpoint; } });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle() { started(); await handlerGate; } });
	sync.setHooks({ async onProcessedCheckpoint() { hookStarted(); await hookGate; } });
	try {
		await sync.start();
		await handlerStarted;
		releaseHandler(); await checkpointHookStarted;
		let settled = false;
		catalog = [];
		const removing = sync.reloadQueries().then(() => { settled = true; });
		await tick(); assert.equal(settled, false);
		releaseHook();
		await removing;
		const checkpoint = finalCheckpoint;
		assert.equal(checkpoint?.timestamp, "1700000001.123456788");
		assert.equal(sync.getQuery(101), undefined);
		assert.ok(checkpoint!.timestamp < transactions[1].consensus_timestamp);
	} finally { releaseHandler(); releaseHook(); await sync.shutdown(); }
});

