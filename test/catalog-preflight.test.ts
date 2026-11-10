import assert from "node:assert/strict";
import { test } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type QueryConfig } from "../src/index";

const query = (id = 1): Extract<QueryConfig, { type: "Transactions" }> => ({
	type: "Transactions", id, title: `Query ${id}`,
	read: { fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 60, batchSize: 10, nextDelayMs: 0 },
		pauseAfterConsecutiveFailures: 2, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
	params: { accountId: "0.0.123", timestamp: "1700000000" },
	process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10 },
});

function invalidCatalogs(): Array<{ name: string; catalog: QueryConfig[] }> {
	const leading = new Array<QueryConfig>(2);
	leading[1] = query();
	const middle = new Array<QueryConfig>(3);
	middle[0] = query();
	middle[2] = query(2);
	const trailing = new Array<QueryConfig>(2);
	trailing[0] = query();
	return [
		{ name: "leading hole", catalog: leading },
		{ name: "middle hole", catalog: middle },
		{ name: "trailing hole", catalog: trailing },
		{ name: "all holes", catalog: new Array<QueryConfig>(2) },
		{ name: "explicit undefined", catalog: [query(), undefined] as unknown as QueryConfig[] },
	];
}

function operationalState(sync: HederaEventSync) {
	// A rejected load should update the diagnostic error, but no operational state.
	const { lastError: _lastError, ...state } = sync.getLifecycleState();
	return state;
}

function transactionRest(): HederaRestClient {
	const builder: any = new Proxy({}, { get: () => () => builder });
	const transactions = [1, 2].map((index) => ({
		name: "CRYPTOTRANSFER", consensus_timestamp: `170000000${index}.123456789`,
		transaction_id: `0.0.123-${index}`, transaction_hash: `hash${index}`, result: "SUCCESS", nonce: 0,
		transfers: [], token_transfers: [], nft_transfers: [],
	}));
	const next = Object.assign(async () => null, { url: () => null });
	return { useProvider() { return this; }, useNetwork() { return this; }, transactions() {
		return { list(callback: any) { callback(builder); return { get: async () => ({ transactions, next }) }; } };
	} } as unknown as HederaRestClient;
}

test("start rejects every empty catalog position before changing lifecycle state and permits a valid retry", async () => {
	let catalog: QueryConfig[] = [];
	let added = 0;
	let removed = 0;
	let checkpointCalls = 0;
	const sync = await HederaEventSync.create({ singletonGuard: false, restClient: {} as HederaRestClient,
		querySource: () => catalog, beforeReplace() { checkpointCalls++; } });
	sync.setHooks({ onQueryAdded() { added++; }, onQueryRemoved() { removed++; } });
	try {
		await sync.pauseAll({ scope: "process" });
		const before = operationalState(sync);
		assert.equal(before.started, false);
		assert.equal(before.globalReadPaused, false);
		assert.equal(before.globalProcessPaused, true);
		for (const invalid of invalidCatalogs()) {
			catalog = invalid.catalog;
			await assert.rejects(sync.start(), /Query config must be an object/, invalid.name);
			assert.deepEqual(operationalState(sync), before, invalid.name);
			assert.deepEqual(sync.listQueries(), [], invalid.name);
			assert.deepEqual({ added, removed, checkpointCalls }, { added: 0, removed: 0, checkpointCalls: 0 }, invalid.name);
		}
		catalog = [{ ...query(), initialState: { read: { name: "PAUSED", info: "manual" } } }];
		await sync.start();
		assert.equal(sync.getLifecycleState().started, true);
		assert.equal(sync.getLifecycleState().generation, 1);
		assert.equal(sync.getLifecycleState().globalReadPaused, false);
		assert.equal(sync.getLifecycleState().globalProcessPaused, true);
		assert.equal(sync.getReadStatus(1)?.status.info, "manual");
		assert.equal(sync.getQueueStatus(1)?.status.name, "PAUSED");
		assert.equal(sync.getQuery(1)?.title, "Query 1");
		assert.deepEqual({ added, removed, checkpointCalls }, { added: 1, removed: 0, checkpointCalls: 0 });
	} finally { await sync.shutdown(); }
});

test("reload rejects empty catalog positions without holding readers or replacing queued work", { timeout: 12_000 }, async () => {
	let catalog: QueryConfig[] = [query()];
	let added = 0;
	let removed = 0;
	let checkpointCalls = 0;
	let readCompleted!: () => void;
	const firstRead = new Promise<void>((resolve) => { readCompleted = resolve; });
	const sync = await HederaEventSync.create({ singletonGuard: false, restClient: transactionRest(),
		querySource: () => catalog, beforeReplace() { checkpointCalls++; } });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { handle() {} });
	sync.setHooks({ onQueryAdded() { added++; }, onQueryRemoved() { removed++; },
		onReadCycleCompleted(_query, audit) { if (audit.success) readCompleted(); } });
	try {
		await sync.pauseAll({ scope: "process" });
		await sync.start();
		await firstRead;
		const before = operationalState(sync);
		const accepted = sync.listQueries();
		const cursor = sync.getReadStatus(1)?.cursor;
		const checkpoint = sync.getProcessedCheckpoint(1);
		const queued = sync.listProcesses(1, 1, 10, "Queued");
		assert.equal(before.globalReadPaused, false);
		assert.equal(before.globalProcessPaused, true);
		assert.equal(sync.getReadStatus(1)?.status.name, "RUNNING");
		assert.equal(queued.total, 2, "the test must retain actual work while the reader is running");
		assert.ok(cursor);
		assert.ok(checkpoint);
		for (const invalid of invalidCatalogs()) {
			catalog = invalid.catalog;
			await assert.rejects(sync.reloadQueries(), /Query config must be an object/, invalid.name);
			assert.deepEqual(operationalState(sync), before, invalid.name);
			assert.deepEqual(sync.listQueries(), accepted, invalid.name);
			assert.equal(sync.getReadStatus(1)?.status.name, "RUNNING", invalid.name);
			assert.deepEqual(sync.getReadStatus(1)?.cursor, cursor, invalid.name);
			assert.deepEqual(sync.getProcessedCheckpoint(1), checkpoint, invalid.name);
			assert.deepEqual(sync.listProcesses(1, 1, 10, "Queued"), queued, invalid.name);
			assert.deepEqual({ added, removed, checkpointCalls }, { added: 1, removed: 0, checkpointCalls: 0 }, invalid.name);
		}
		catalog = [{ ...query(), title: "Reloaded query" }, { ...query(2), initialState: {
			read: { name: "PAUSED", info: "manual" }, process: { name: "PAUSED", info: "manual" },
		} }];
		await sync.reloadQueries();
		assert.equal(sync.getLifecycleState().generation, 2);
		assert.equal(sync.getLifecycleState().globalReadPaused, false);
		assert.equal(sync.getLifecycleState().globalProcessPaused, true);
		assert.equal(sync.getQuery(1)?.title, "Reloaded query");
		assert.equal(sync.getQuery(2)?.title, "Query 2");
		assert.deepEqual({ added, removed, checkpointCalls }, { added: 3, removed: 1, checkpointCalls: 1 });
		catalog = []; // An intentional empty catalog still replaces the accepted generation.
		await sync.reloadQueries();
		assert.equal(sync.getLifecycleState().generation, 3);
		assert.equal(sync.getLifecycleState().globalReadPaused, false);
		assert.equal(sync.getLifecycleState().globalProcessPaused, true);
		assert.deepEqual(sync.listQueries(), []);
		assert.deepEqual({ added, removed, checkpointCalls }, { added: 3, removed: 3, checkpointCalls: 2 });
	} finally { await sync.shutdown(); }
});
