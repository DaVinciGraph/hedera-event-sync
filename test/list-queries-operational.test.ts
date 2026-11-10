import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type ListQueriesOptions, type QueryConfig } from "../src/index";

const abi = [{ type: "event", name: "NotSearchableEvent", anonymous: false, inputs: [] }] as const;
const ids = (items: readonly { id: number }[]) => items.map(item => item.id);
const deferred = () => {
	let resolve!: () => void;
	return { promise: new Promise<void>(done => { resolve = done; }), resolve: () => resolve() };
};
async function within<T>(promise: Promise<T>, description: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), 3000);
		})]);
	} finally { if (timer) clearTimeout(timer); }
}
async function waitFor(predicate: () => boolean, description: string): Promise<void> {
	const deadline = Date.now() + 3000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${description}`);
		await new Promise<void>(done => setTimeout(done, 5));
	}
}
function transactionRest(withRecord = false) {
	let requests = 0;
	const builder: any = new Proxy({}, { get: () => () => builder });
	const rest = { useProvider() { return this; }, useNetwork() { return this; }, transactions() {
		return { list(callback: any) { callback(builder); return { get: async () => {
			requests++;
			return { transactions: withRecord ? [{ name: "CRYPTOTRANSFER", consensus_timestamp: "1700000001.123456789",
				transaction_id: "0.0.9001-1", transaction_hash: "hash1", result: "SUCCESS", nonce: 0,
				transfers: [], token_transfers: [], nft_transfers: [] }] : [],
				next: Object.assign(async () => null, { url: () => null }) };
		} }; } };
	} } as unknown as HederaRestClient;
	return { rest, requests: () => requests };
}
function transaction(id: number): Extract<QueryConfig, { type: "Transactions" }> {
	return { type: "Transactions", id, title: `Query ${id}`,
		read: { fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 60, nextDelayMs: 0 },
			pauseAfterConsecutiveFailures: 1, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
		params: { accountId: "0.0.9001", timestamp: "1700000000" },
		process: { pauseAfterConsecutiveFailures: 1, nextDelayMs: 0 },
	};
}
function paused(id: number): Extract<QueryConfig, { type: "Transactions" }> {
	return { ...transaction(id), initialState: { read: { name: "PAUSED", info: "manual read" }, process: { name: "PAUSED", info: "manual process" } } };
}
async function catalogFixture(t: TestContext) {
	const treasury = paused(30); treasury.title = "Station treasury"; treasury.contract = { type: "STATION", id: "0.0.9123" };
	treasury.initialState!.process = { name: "RUNNING" };
	const mainnet = paused(10); mainnet.title = "Station mainnet"; mainnet.contract = { type: "STATION" };
	mainnet.read.fetch.network = "mainnet"; mainnet.initialState!.process = { name: "RUNNING" };
	const logBase = paused(20);
	const single: QueryConfig = { ...logBase, type: "Single-Contract-Logs", title: "Station events", abi,
		contract: { type: "STATION", address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd" }, params: {} };
	const launch = paused(40); launch.title = "Launch index"; launch.contract = { type: "LAUNCH" };
	launch.read.fetch.network = "mainnet"; launch.initialState!.read = { name: "RUNNING" };
	let resolutions = 0;
	const multi: QueryConfig = { ...paused(50), type: "Multi-Contract-Logs", title: "Global [V2].*", abi,
		params: { topics: ["0x1"], contractResolver() { resolutions++; return { id: "0.0.888888" }; } } };
	const account = paused(60); account.title = "Account activity"; account.params.accountId = "0.0.456789";
	account.initialState!.process = { name: "RUNNING" };
	const active = transaction(70); active.title = "Station activity"; active.contract = { type: "STATION" };
	const archive = paused(80); archive.title = "Archived"; archive.contract = { type: "FILTER_ONLY", version: "NotSearchableVersion" };
	let loads = 0, statusChanges = 0;
	const fixture = transactionRest();
	const sync = await HederaEventSync.create({ restClient: fixture.rest, singletonGuard: false,
		querySource: () => { loads++; return [treasury, mainnet, single, launch, multi, account, active, archive]; } });
	sync.setHooks({ onQueryStatusChange() { statusChanges++; } });
	t.after(() => sync.shutdown());
	await sync.start();
	return { sync, loads: () => loads, statusChanges: () => statusChanges, requests: fixture.requests, resolutions: () => resolutions };
}

for (const order of ["insertion", "id-asc", "id-desc"] as const) {
	test(`${order}: operational filters compose before pagination, totals, and out-of-range clamping`, async t => {
		const { sync } = await catalogFixture(t);
		const options = { order, readStatus: ["PAUSED", "RUNNING"], processStatus: "RUNNING", search: " station ", contractType: "STATION" } satisfies ListQueriesOptions;
		const expected = order === "insertion" ? [30, 10, 70] : order === "id-asc" ? [10, 30, 70] : [70, 30, 10];
		const first = sync.listQueries(1, 2, options);
		assert.deepEqual({ ...first, items: ids(first.items) }, { items: expected.slice(0, 2), page: 1, pageSize: 2, total: 3, totalPages: 2, hasPrev: false, hasNext: true });
		const last = sync.listQueries(999, 2, options);
		assert.deepEqual({ ...last, items: ids(last.items) }, { items: expected.slice(2), page: 2, pageSize: 2, total: 3, totalPages: 2, hasPrev: true, hasNext: false });
		assert.deepEqual(last, sync.listQueries(2, 2, options));
		assert.deepEqual(ids(sync.listQueries(1, 20, { ...options, network: "testnet", type: "Transactions" }).items), order === "id-desc" ? [70, 30] : [30, 70]);
		assert.equal(sync.listQueries(1, 20, { ...options, type: "Single-Contract-Logs" }).total, 0);
		assert.deepEqual(sync.listQueries(99, 2, { ...options, processStatus: [] }), { items: [], page: 1, pageSize: 2, total: 0, totalPages: 1, hasPrev: false, hasNext: false });
		assert.equal(sync.listQueries(1, 20, { ...options, readStatus: [] }).total, 0);
	});
}

test("reader and processor status filters are independent, exact, and OR values within each field", async t => {
	const { sync } = await catalogFixture(t);
	assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "RUNNING", processStatus: "PAUSED" }).items), [40]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSED", processStatus: "RUNNING" }).items), [30, 10, 60]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: ["RUNNING", "PAUSED", "RUNNING"], processStatus: ["RUNNING", "PAUSED"] }).items), [30, 10, 20, 40, 50, 60, 70, 80]);
	for (const status of ["PAUSING", "RESUMING", "FAILED", "STOPPED"] as const) {
		assert.equal(sync.listQueries(1, 20, { readStatus: status }).total, 0);
		assert.equal(sync.listQueries(1, 20, { processStatus: status }).total, 0);
	}
});

test("literal, trimmed, case-insensitive search covers each configured identifier and ignores unrelated fields", async t => {
	const { sync } = await catalogFixture(t);
	for (const [search, expected] of [
		[" sTaTiOn ", [30, 10, 20, 70]], ["30", [30]], ["ABCDEF", [20]], ["0.0.9123", [30]],
		["0.0.456789", [60]], ["[v2].*", [50]], [".*", [50]], ["NotSearchableEvent", []],
		["NotSearchableVersion", []], ["FILTER_ONLY", []], ["mainnet", [10]], ["CRYPTOTRANSFER", []], ["0.0.888888", []],
	] as const) assert.deepEqual(ids(sync.listQueries(1, 20, { search }).items), expected, `search ${search}`);
	assert.deepEqual(ids(sync.listQueries(1, 20, { search: "global", contractType: null }).items), [50]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { search: "account", type: "Transactions", contractType: null }).items), [60]);
	const all = sync.listQueries(1, 20);
	for (const search of [undefined, "", " \t\n "]) assert.deepEqual(sync.listQueries(1, 20, { search }), all);
	assert.deepEqual(sync.listQueries(1, 20, { readStatus: undefined, processStatus: undefined, search: undefined }), all);
});

test("filtering returns isolated snapshots without changing lifecycle, callbacks, checkpoints, or requests", async t => {
	const fixture = await catalogFixture(t), { sync } = fixture;
	const before = sync.listQueries(), lifecycle = sync.getLifecycleState();
	const checkpoints = before.map(query => sync.getProcessedCheckpoint(query.id));
	const requests = fixture.requests(), changes = fixture.statusChanges();
	const readStatus = Object.freeze(["RUNNING", "PAUSED"] as const);
	const options = Object.freeze({ readStatus, processStatus: "RUNNING", search: "station" } as const);
	const page = sync.listQueries(1, 20, options);
	page.items[0]!.title = "Changed";
	page.items[0]!.contract!.id = "0.0.9999";
	page.items[0]!.read.status.name = "FAILED";
	page.items[0]!.process.status.name = "STOPPED";
	page.items.pop();
	assert.deepEqual(sync.listQueries(), before);
	assert.deepEqual(sync.getLifecycleState(), lifecycle);
	assert.deepEqual(before.map(query => sync.getProcessedCheckpoint(query.id)), checkpoints);
	assert.deepEqual(ids(sync.listQueries(1, 20, options).items), [30, 10, 70]);
	assert.equal(fixture.requests(), requests); assert.equal(fixture.statusChanges(), changes); assert.equal(fixture.loads(), 1);
	assert.equal(fixture.resolutions(), 0);
	assert.deepEqual(readStatus, ["RUNNING", "PAUSED"]);
});

test("invalid operational filters reject descriptively even for empty catalogs and leave state untouched", async t => {
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => [] });
	t.after(() => sync.shutdown()); await sync.start();
	const before = sync.getLifecycleState();
	for (const field of ["readStatus", "processStatus"] as const) {
		for (const value of ["paused", "", "UNKNOWN", null, 1, {}, ["RUNNING", "BAD"], [undefined], [null], [["PAUSED"]]]) {
			assert.throws(() => sync.listQueries(1, 20, { [field]: value } as unknown as ListQueriesOptions), new RegExp(field));
		}
	}
	for (const search of [null, 1, {}, [], ["station"]]) assert.throws(() => sync.listQueries(1, 20, { search } as unknown as ListQueriesOptions), /search/);
	assert.deepEqual(sync.getLifecycleState(), before);
	assert.deepEqual(sync.listQueries(99, 3, { readStatus: "PAUSED", processStatus: ["PAUSED"], search: "station" }),
		{ items: [], page: 1, pageSize: 3, total: 0, totalPages: 1, hasPrev: false, hasNext: false });
});

for (const scope of ["read", "process", "both"] as const) {
	test(`effective ${scope} global holds filter PAUSING in-flight work and PAUSED settled work`, { timeout: 10000 }, async t => {
		t.mock.method(Math, "random", () => 0);
		const readEntered = deferred(), processEntered = deferred(), releaseRead = deferred(), releaseProcess = deferred();
		const sync = await HederaEventSync.create({ restClient: transactionRest(true).rest, singletonGuard: false,
			querySource: () => [transaction(1), transaction(2), paused(3)] });
		sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle(_data, context) {
			if (context.queryId === 2) { processEntered.resolve(); await releaseProcess.promise; }
		} });
		sync.setHooks({ async onReadCycleStarted(query) { if (query.id === 1) { readEntered.resolve(); await releaseRead.promise; } } });
		let pausing: Promise<void> | undefined;
		try {
			await sync.start(); await within(Promise.all([readEntered.promise, processEntered.promise]), "active reader and processor");
			pausing = sync.pauseAll({ scope });
			await waitFor(() => sync.getLifecycleState().operation?.name === "pauseAll", "global pause gate");
			assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSING" }).items), scope === "process" ? [] : [1]);
			assert.deepEqual(ids(sync.listQueries(1, 20, { processStatus: "PAUSING" }).items), scope === "read" ? [] : [2]);
			assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSED", processStatus: "PAUSED", search: "3" }).items), [3]);
			for (const item of sync.listQueries(1, 20, { readStatus: ["PAUSED", "PAUSING"] }).items) assert.deepEqual(item.read.status, sync.getReadStatus(item.id)!.status);
			for (const item of sync.listQueries(1, 20, { processStatus: ["PAUSED", "PAUSING"] }).items) assert.deepEqual(item.process.status, sync.getQueueStatus(item.id)!.status);
			releaseRead.resolve(); releaseProcess.resolve(); await within(pausing, "global pause completion");
			assert.equal(sync.listQueries(1, 20, { readStatus: "PAUSING" }).total, 0);
			assert.equal(sync.listQueries(1, 20, { processStatus: "PAUSING" }).total, 0);
			assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSED" }).items), scope === "process" ? [3] : [1, 2, 3]);
			assert.deepEqual(ids(sync.listQueries(1, 20, { processStatus: "PAUSED" }).items), scope === "read" ? [3] : [1, 2, 3]);
			await sync.resumeAll({ scope });
			assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSED", processStatus: "PAUSED" }).items), [3]);
			assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "RUNNING", processStatus: "RUNNING" }).items), [1, 2]);
		} finally { releaseRead.resolve(); releaseProcess.resolve(); if (pausing) await pausing; await sync.shutdown(); }
	});
}

test("consecutive processing failures match PAUSED while terminal hook failures match FAILED through global holds", { timeout: 10000 }, async t => {
	t.mock.method(Math, "random", () => 0);
	const sync = await HederaEventSync.create({ restClient: transactionRest(true).rest, singletonGuard: false,
		hookFailurePolicy: "fail-fast", querySource: () => [transaction(1), transaction(2)] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { handle(_data, context) { if (context.queryId === 1) throw new Error("Business failure"); } });
	sync.setHooks({ onReadCycleStarted(query) { if (query.id === 2) throw new Error("Terminal hook failure"); } });
	try {
		await sync.start();
		await waitFor(() => sync.getQueueStatus(1)?.status.name === "PAUSED" && sync.getReadStatus(2)?.status.name === "FAILED", "failure states");
		assert.deepEqual(ids(sync.listQueries(1, 20, { processStatus: "PAUSED" }).items), [1]);
		assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "FAILED", processStatus: "FAILED" }).items), [2]);
		assert.match(sync.getQueueStatus(1)!.status.info!, /consecutive failures/);
		await sync.pauseAll();
		assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "PAUSED", processStatus: "PAUSED" }).items), [1]);
		assert.deepEqual(ids(sync.listQueries(1, 20, { readStatus: "FAILED", processStatus: "FAILED" }).items), [2]);
		await sync.resumeAll();
		assert.deepEqual(ids(sync.listQueries(1, 20, { processStatus: "PAUSED" }).items), [1]);
		assert.deepEqual(ids(sync.listQueries(1, 20, { processStatus: "FAILED" }).items), [2]);
		assert.equal(sync.getFailedProcess(1)!.tries.length, 1);
	} finally { await sync.shutdown(); }
});
