import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type FailureRecoveryOptions, type QueryConfig } from "../src/index";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const deferred = () => {
	let resolve!: () => void;
	return { promise: new Promise<void>(done => { resolve = done; }), resolve: () => resolve() };
};
async function until(predicate: () => boolean) {
	const end = performance.now() + 5000;
	while (!predicate()) { if (performance.now() > end) throw new Error("Condition did not settle"); await sleep(10); }
}
function fixture(t: TestContext) {
	t.mock.method(Math, "random", () => 0);
	let now = 1_800_000_000_000;
	t.mock.method(Date, "now", () => now);
	const state = { readFails: false, reads: 0, handleFails: false, attempts: [] as string[] };
	const builder: any = new Proxy({}, { get: () => () => builder });
	const rest = { useProvider() { return this; }, useNetwork() { return this; }, transactions() {
		return { list(callback: any) { callback(builder); return { get: async () => {
			state.reads++;
			if (state.readFails) throw new Error("Mirror temporarily unavailable");
			return { transactions: [1, 2].map(id => ({ name: "CRYPTOTRANSFER", consensus_timestamp: `170000000${id}.000000001`,
				transaction_id: `tx-${id}`, transaction_hash: `hash-${id}`, result: "SUCCESS", nonce: 0,
				transfers: [], token_transfers: [], nft_transfers: [] })), next: Object.assign(async () => null, { url: () => null }) };
		} }; } };
	} } as unknown as HederaRestClient;
	let catalog: QueryConfig[] = [{ type: "Transactions", id: 1, title: "Recovery", params: { accountId: "0.0.123", timestamp: "1700000000" },
		read: { fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 2 },
			pauseAfterConsecutiveFailures: 1, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
		process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10, holdPeriod: "daily" } }];
	return { state, advance: () => { now += 3_600_000; }, now: () => now, setCatalog: (next: QueryConfig[]) => { catalog = next; },
		catalog: () => catalog,
		async create(recovery?: FailureRecoveryOptions, extra: Partial<Parameters<typeof HederaEventSync.create>[0]> = {}) {
			const sync = await HederaEventSync.create({ restClient: rest, singletonGuard: false, querySource: () => catalog, failureRecovery: recovery, ...extra });
			sync.registry.registerTransaction("CRYPTOTRANSFER", { handle(tx) {
				state.attempts.push(tx.transaction_id!);
				if (state.handleFails) throw new Error("Storage temporarily unavailable");
			} });
			t.after(async () => { await sync.shutdown(); });
			return sync;
		} };
}

test("recovery is opt-in, including a disabled policy with an interval", async t => {
	const f = fixture(t); f.state.readFails = true;
	for (const policy of [undefined, { enabled: false, delaySeconds: 1 }]) {
		const sync = await f.create(policy); await sync.start();
		await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
		assert.equal(sync.getReadStatus(1)?.status.failureRecovery, undefined);
		const reads = f.state.reads; f.advance(); await sleep(1100);
		assert.equal(f.state.reads, reads);
		await sync.shutdown();
	}
});

test("a real scheduled retry uses the one-hour default and preserves an independent manual processing pause", async t => {
	const f = fixture(t); f.state.readFails = true;
	f.catalog()[0].initialState = { process: { name: "PAUSED", info: "operator" } };
	const sync = await f.create({ enabled: true }); await sync.start();
	await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
	const cursor = sync.getReadStatus(1)!.cursor;
	assert.equal(sync.getReadStatus(1)!.status.failureRecovery!.retryAt, f.now() + 3_600_000);
	const snapshot = sync.getQuery(1)!; snapshot.read.status.failureRecovery!.retryAt = 0;
	assert.notEqual(sync.getQuery(1)!.read.status.failureRecovery!.retryAt, 0);
	await sleep(1100); assert.equal(f.state.reads, 1);
	assert.deepEqual(sync.getReadStatus(1)!.cursor, cursor);
	f.state.readFails = false; f.advance();
	await until(() => f.state.reads > 1);
	assert.equal(sync.getReadStatus(1)!.status.failureRecovery, undefined);
	assert.equal(sync.getQueueStatus(1)!.status.info, "operator");
	assert.equal(sync.countProcesses(1, "Queued"), 2);
});

test("processing retries retain FIFO records and receive a full failure budget on every cooldown", async t => {
	const f = fixture(t); f.state.handleFails = true;
	const sync = await f.create({ enabled: true }); await sync.start();
	await until(() => sync.getQueueStatus(1)?.status.name === "PAUSED");
	const checkpoint = sync.getProcessedCheckpoint(1);
	assert.deepEqual(f.state.attempts, ["tx-1", "tx-1"]);
	await sync.pauseAll({ scope: "read" });
	f.advance();
	await until(() => f.state.attempts.length === 4 && sync.getQueueStatus(1)?.status.name === "PAUSED");
	assert.deepEqual(sync.getProcessedCheckpoint(1), checkpoint);
	assert.equal(sync.getQueueStatus(1)!.status.failureRecovery!.retryAt, f.now() + 3_600_000);
	f.state.handleFails = false; f.advance();
	await until(() => sync.countProcesses(1, "Processed") === 2);
	assert.deepEqual(f.state.attempts, ["tx-1", "tx-1", "tx-1", "tx-1", "tx-1", "tx-2"]);
	assert.equal(sync.getLifecycleState().globalReadPaused, true);
	assert.equal(sync.getQueueStatus(1)!.status.failureRecovery, undefined);
});

for (const stage of ["read", "process"] as const) {
	test(`${stage}: explicit pause cancels recovery, including a pause during the threshold hook`, async t => {
		const f = fixture(t); f.state.readFails = stage === "read"; f.state.handleFails = stage === "process";
		const entered = deferred(), release = deferred();
		const sync = await f.create({ enabled: true });
		const block = async () => { entered.resolve(); await release.promise; };
		sync.setHooks(stage === "read" ? { onReadConsecutiveFailuresReached: block } : { onProcessConsecutiveFailuresReached: block });
		await sync.start(); await entered.promise;
		const pause = stage === "read" ? sync.pauseRead(1) : sync.pauseProcessing(1);
		await sleep(10); f.advance(); await sleep(1100);
		release.resolve(); await pause;
		const status = () => sync.getQuery(1)![stage].status;
		assert.equal(status().info, "admin"); assert.equal(status().failureRecovery, undefined);
		const attempts = stage === "read" ? f.state.reads : f.state.attempts.length;
		await sleep(1100);
		assert.equal(stage === "read" ? f.state.reads : f.state.attempts.length, attempts);
	});

	test(`${stage}: scoped global hold defers recovery without discarding its deadline`, async t => {
		const f = fixture(t); f.state.readFails = stage === "read"; f.state.handleFails = stage === "process";
		const sync = await f.create({ enabled: true }); await sync.start();
		await until(() => sync.getQuery(1)?.[stage].status.name === "PAUSED");
		const deadline = sync.getQuery(1)![stage].status.failureRecovery!.retryAt;
		await sync.pauseAll({ scope: stage }); f.advance(); await sleep(1100);
		assert.equal(sync.getQuery(1)![stage].status.failureRecovery!.retryAt, deadline);
		f.state.readFails = f.state.handleFails = false;
		await sync.resumeAll({ scope: stage });
		await until(() => sync.getQuery(1)?.[stage].status.failureRecovery === undefined);
	});
}

test("a manual processing pause during an active failed handler cannot become automatically recoverable", async t => {
	const f = fixture(t); f.catalog()[0].process.pauseAfterConsecutiveFailures = 1;
	const entered = deferred(), release = deferred();
	const sync = await f.create({ enabled: true });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle() { entered.resolve(); await release.promise; throw new Error("failure"); } });
	await sync.start(); await entered.promise;
	const pause = sync.pauseProcessing(1); await sleep(10); release.resolve(); await pause;
	assert.equal(sync.getQueueStatus(1)!.status.info, "admin");
	assert.equal(sync.getQueueStatus(1)!.status.failureRecovery, undefined);
});

for (const operation of ["restartQueries", "reloadQueries"] as const) {
	test(`${operation} transfers failure ownership and the original deadline to the retained query`, async t => {
		const f = fixture(t); f.state.readFails = true;
		const sync = await f.create({ enabled: true }); await sync.start();
		await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
		const deadline = sync.getQuery(1)!.read.status.failureRecovery!.retryAt;
		await sync[operation]();
		assert.equal(sync.getQuery(1)!.read.status.failureRecovery!.retryAt, deadline);
		f.state.readFails = false; f.advance(); await until(() => f.state.reads > 1);
	});
}

test("loading a replacement catalog defers recovery; removed and re-added IDs do not inherit stale recovery", async t => {
	const f = fixture(t); f.state.readFails = true;
	let loading = false; const entered = deferred(), release = deferred();
	const sync = await f.create({ enabled: true }, { querySource: async () => {
		if (loading) { entered.resolve(); await release.promise; }
		return f.catalog();
	} });
	await sync.start(); await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
	const saved = f.catalog(); loading = true; f.setCatalog([]);
	const reload = sync.reloadQueries(); await entered.promise; f.advance(); await sleep(1100);
	assert.equal(f.state.reads, 1);
	release.resolve(); await reload; assert.equal(sync.getQuery(1), undefined);
	saved[0].initialState = { read: { name: "PAUSED", info: "Read Job is paused due to 1 consecutive failures" } };
	f.setCatalog(saved); await sync.reloadQueries(); await sleep(1100);
	assert.equal(sync.getQuery(1)!.read.status.failureRecovery, undefined);
	assert.equal(f.state.reads, 1);
});

test("a failed replacement holds recovery, and shutdown leaves no delayed network requests", async t => {
	const f = fixture(t); f.state.readFails = true;
	let rejectPersistence = true;
	const sync = await f.create({ enabled: true }, { beforeReplace() { if (rejectPersistence) throw new Error("persistence offline"); } });
	await sync.start(); await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
	await assert.rejects(sync.restartQueries(), /persistence offline/);
	f.advance(); await sleep(1100); assert.equal(f.state.reads, 1);
	rejectPersistence = false; await sync.shutdown(); f.advance(); await sleep(1100);
	assert.equal(f.state.reads, 1);
});

for (const policy of ["pause", "fail-fast"] as const) {
	test(`a ${policy} threshold-hook failure is not automatically resumed`, async t => {
		const f = fixture(t); f.state.readFails = true;
		const sync = await f.create({ enabled: true }, { hookFailurePolicy: policy });
		sync.setHooks({ onReadConsecutiveFailuresReached() { throw new Error("required observer failed"); } });
		await sync.start(); await until(() => ["PAUSED", "FAILED"].includes(sync.getReadStatus(1)!.status.name));
		assert.equal(sync.getReadStatus(1)!.status.failureRecovery, undefined);
		f.advance(); await sleep(1100); assert.equal(f.state.reads, 1);
	});
}

test("invalid recovery settings reject construction; long intervals are not platform timer delays", async t => {
	const f = fixture(t);
	for (const value of [null, true, {}, { enabled: 1 }, ...[null, "60", 0, -1, 1.5, NaN, Infinity, 2147483648].map(delaySeconds => ({ enabled: true, delaySeconds }))]) {
		await assert.rejects(f.create(value as FailureRecoveryOptions), /failureRecovery/);
	}
	f.state.readFails = true;
	const sync = await f.create({ enabled: true, delaySeconds: 2147483647 }); await sync.start();
	await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
	assert.equal(sync.getReadStatus(1)!.status.failureRecovery!.retryAt, f.now() + 2147483647000);
});

test("application recovery holds compose, preserve the deadline, and permit explicit controls", async t => {
	const f = fixture(t); f.state.readFails = true;
	const sync = await f.create({ enabled: true }); await sync.start();
	await until(() => sync.getReadStatus(1)?.status.name === "PAUSED");
	const deadline = sync.getReadStatus(1)!.status.failureRecovery!.retryAt;
	const releaseOne = sync.holdFailureRecovery(), releaseTwo = sync.holdFailureRecovery();
	f.advance(); releaseOne(); releaseOne(); await sleep(1100);
	assert.equal(f.state.reads, 1);
	assert.equal(sync.getReadStatus(1)!.status.failureRecovery!.retryAt, deadline);
	await sync.pauseProcessing(1); await sync.resumeProcessing(1);
	f.state.readFails = false; releaseTwo();
	await until(() => f.state.reads > 1);
});

test("explicitly preserving an existing failure pause retains its automatic retry", async t => {
	const f = fixture(t); f.state.handleFails = true;
	const sync = await f.create({ enabled: true }); await sync.start();
	await until(() => sync.getQueueStatus(1)?.status.name === "PAUSED");
	const status = sync.getQueueStatus(1)!.status;
	await sync.pauseProcessing(1, { preserveExistingPause: true });
	assert.deepEqual(sync.getQueueStatus(1)!.status, status);
	await sync.reloadQueries();
	assert.deepEqual(sync.getQueueStatus(1)!.status, status);
	f.state.handleFails = false; f.advance();
	await until(() => sync.countProcesses(1, "Processed") === 2);
	assert.deepEqual(f.state.attempts, ["tx-1", "tx-1", "tx-1", "tx-2"]);
});
