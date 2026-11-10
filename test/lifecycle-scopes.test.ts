import assert from "node:assert/strict";
import { test } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, LifecycleBusyError, LifecycleTimeoutError, type GlobalPauseScope, type QueryConfig } from "../src/index";

const deferred = () => {
	let resolve!: () => void;
	return { promise: new Promise<void>((done) => { resolve = done; }), resolve: () => resolve() };
};
const turn = () => new Promise<void>((done) => setImmediate(done));
async function within<T>(promise: Promise<T>, description = "operation"): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), 3000);
		})]);
	} finally { if (timer) clearTimeout(timer); }
}
async function settled(sync: HederaEventSync): Promise<void> {
	await within((async () => {
		while (sync.getLifecycleState().operation) await new Promise<void>((done) => setTimeout(done, 5));
	})(), "lifecycle gate to settle");
}
const query = (id = 1): Extract<QueryConfig, { type: "Transactions" }> => ({
	type: "Transactions", id, title: `Query ${id}`,
	read: { fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 60, batchSize: 1, nextDelayMs: 0 },
		pauseAfterConsecutiveFailures: 2, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
	params: { accountId: "0.0.123", timestamp: "1700000000" },
	process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10, nextDelayMs: 0 },
});
const paused = (id = 1): Extract<QueryConfig, { type: "Transactions" }> => ({ ...query(id), initialState: {
	read: { name: "PAUSED", info: "manual read pause" }, process: { name: "PAUSED", info: "manual process pause" },
} });

function transactionRest(pages: readonly (readonly number[])[] = [[1, 2]]) {
	const requests: number[] = [];
	const builder: any = new Proxy({}, { get: () => () => builder });
	const page = (index: number): any => {
		requests.push(index);
		return { transactions: pages[index].map((value) => ({ name: "CRYPTOTRANSFER", consensus_timestamp: `170000000${value}.123456789`,
			transaction_id: `0.0.123-${value}`, transaction_hash: `hash${value}`, result: "SUCCESS", nonce: 0,
			transfers: [], token_transfers: [], nft_transfers: [] })),
			next: Object.assign(async () => index + 1 < pages.length ? page(index + 1) : null,
				{ url: () => index + 1 < pages.length ? `https://mirror.example/transactions?page=${index + 1}` : null }) };
	};
	const rest = { useProvider() { return this; }, useNetwork() { return this; }, transactions() {
		return { list(callback: any) { callback(builder); return { get: async () => page(0) }; } };
	} } as unknown as HederaRestClient;
	return { rest, requests };
}

function assertHolds(sync: HederaEventSync, read: boolean, process: boolean, maintenance = false): void {
	const state = sync.getLifecycleState();
	assert.equal(state.globalReadPaused, read);
	assert.equal(state.globalProcessPaused, process);
	assert.equal(state.globalPaused, read && process);
	assert.equal(state.maintenanceHeld, maintenance);
}

test("resuming only reads after pauseAll fills bounded capacity, and a read hold cancels its capacity wait", { timeout: 10000 }, async (t) => {
	t.mock.method(Math, "random", () => 0);
	const thirdPage = deferred(), releasePage = deferred(), drained = deferred(), allProcessed = deferred();
	const fixture = transactionRest([[1], [2], [3], [4]]);
	const config = query(); config.process.maxQueuedProcesses = 2;
	let pages = 0, processed = 0, interrupted = false;
	const sync = await HederaEventSync.create({ restClient: fixture.rest, singletonGuard: false, querySource: () => [config] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { handle() {} });
	sync.setHooks({
		async onReadPage() { if (++pages === 3) { thirdPage.resolve(); await releasePage.promise; } },
		onReadCycleCompleted(_query, audit) { if (!audit.success) interrupted = true; },
		onProcessSucceeded() { if (++processed === 2) drained.resolve(); if (processed === 4) allProcessed.resolve(); },
	});
	try {
		await sync.pauseAll(); await sync.start();
		await sync.resumeAll({ scope: "read" });
		assertHolds(sync, false, true);
		await within(thirdPage.promise, "third page at queue capacity");
		assert.equal(sync.countProcesses(1, "Queued"), 2);
		assert.equal(processed, 0);
		releasePage.resolve(); await turn();
		await within(sync.pauseAll({ scope: "process" }), "process pause while reader waits for capacity");
		assert.deepEqual(fixture.requests, [0, 1, 2]);
		await within(sync.pauseAll({ scope: "read" }), "read cancellation of capacity wait");
		assert.equal(interrupted, true);
		assert.equal(sync.getReadStatus(1)?.cursor.timestamp, "1700000002.123456789");
		assert.equal(sync.countProcesses(1, "Queued"), 2);
		await sync.resumeAll({ scope: "process" });
		await within(drained.promise, "backlog draining with reads held");
		assertHolds(sync, true, false);
		assert.deepEqual(fixture.requests, [0, 1, 2]);
		await sync.resumeAll();
		await within(allProcessed.promise, "remaining pages after default resume");
		assertHolds(sync, false, false);
	} finally { releasePage.resolve(); await settled(sync); await sync.shutdown(); }
});

test("read-only pause does not wait for a blocked handler and lets queued work drain", { timeout: 10000 }, async (t) => {
	t.mock.method(Math, "random", () => 0);
	const entered = deferred(), finish = deferred(), readComplete = deferred(), drained = deferred();
	const fixture = transactionRest([[1, 2, 3]]);
	let processed = 0;
	const sync = await HederaEventSync.create({ restClient: fixture.rest, singletonGuard: false, querySource: () => [query()] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle() { entered.resolve(); await finish.promise; } });
	sync.setHooks({ onReadCycleCompleted() { readComplete.resolve(); }, onProcessSucceeded() { if (++processed === 3) drained.resolve(); } });
	try {
		await sync.start(); await within(Promise.all([entered.promise, readComplete.promise]));
		await within(sync.pauseAll({ scope: "read" }), "read pause while handler is blocked");
		assertHolds(sync, true, false);
		assert.ok(sync.getInFlightProcess(1));
		assert.equal(sync.countProcesses(1, "Queued"), 2);
		finish.resolve(); await within(drained.promise, "queued processing to continue");
		assert.equal(sync.countProcesses(1, "Queued"), 0);
		assert.deepEqual(fixture.requests, [0]);
	} finally { finish.resolve(); await settled(sync); await sync.shutdown(); }
});

test("process-only pause never waits for a reader hook and leaves reads able to enqueue", { timeout: 10000 }, async (t) => {
	t.mock.method(Math, "random", () => 0);
	const reading = deferred(), finishRead = deferred(), enqueued = deferred(), drained = deferred();
	let queued = 0, processed = 0;
	const sync = await HederaEventSync.create({ restClient: transactionRest().rest, singletonGuard: false, querySource: () => [query()] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { handle() {} });
	sync.setHooks({
		async onReadCycleStarted() { reading.resolve(); await finishRead.promise; },
		onProcessEnqueued() { if (++queued === 2) enqueued.resolve(); },
		onProcessSucceeded() { if (++processed === 2) drained.resolve(); },
	});
	try {
		await sync.start(); await within(reading.promise);
		await within(sync.pauseAll({ scope: "process" }), "process pause while reader hook is blocked");
		assertHolds(sync, false, true);
		finishRead.resolve(); await within(enqueued.promise);
		assert.equal(sync.countProcesses(1, "Queued"), 2);
		assert.equal(processed, 0);
		await sync.resumeAll({ scope: "process" }); await within(drained.promise);
		assertHolds(sync, false, false);
	} finally { finishRead.resolve(); await settled(sync); await sync.shutdown(); }
});

for (const scope of ["read", "process"] as const) {
	test(`${scope} pause timeout keeps its scoped gate until the active callback settles`, { timeout: 10000 }, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const entered = deferred(), finish = deferred();
		let handlerSignal: AbortSignal | undefined;
		const sync = await HederaEventSync.create({ restClient: transactionRest([[1]]).rest, singletonGuard: false, querySource: () => [query()] });
		sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle(_data, context) {
			handlerSignal = context.signal;
			if (scope === "process") { entered.resolve(); await finish.promise; }
		} });
		if (scope === "read") sync.setHooks({ async onReadPage() { entered.resolve(); await finish.promise; } });
		try {
			await sync.start(); await within(entered.promise);
			await assert.rejects(sync.pauseAll({ scope, timeoutMs: 30 }), LifecycleTimeoutError);
			assertHolds(sync, scope === "read", scope === "process");
			assert.equal(sync.getLifecycleState().operation?.scope, scope);
			assert.equal(sync.getLifecycleState().operation?.timedOut, true);
			if (scope === "process") assert.equal(handlerSignal?.aborted, false);
			await assert.rejects(sync.resumeAll({ scope }), LifecycleBusyError);
			await assert.rejects(sync.pauseAll({ scope: scope === "read" ? "process" : "read" }), LifecycleBusyError);
			finish.resolve(); await settled(sync);
			assertHolds(sync, scope === "read", scope === "process");
			await sync.resumeAll({ scope }); assertHolds(sync, false, false);
		} finally { finish.resolve(); await settled(sync); await sync.shutdown(); }
	});
}

test("scoped global resume preserves manual read pauses and an actual processing failure pause", { timeout: 10000 }, async (t) => {
	t.mock.method(Math, "random", () => 0);
	const failed = deferred(), recovered = deferred();
	const config = query(); config.process.pauseAfterConsecutiveFailures = 1;
	let attempts = 0, reject = true;
	const sync = await HederaEventSync.create({ restClient: transactionRest([[1]]).rest, singletonGuard: false, querySource: () => [config] });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { handle() { attempts++; if (reject) throw new Error("business failure"); } });
	sync.setHooks({ onProcessConsecutiveFailuresReached() { failed.resolve(); }, onProcessSucceeded() { recovered.resolve(); } });
	try {
		await sync.start(); await within(failed.promise);
		await sync.pauseRead(1); await sync.pauseAll();
		await sync.resumeAll({ scope: "read" }); await sync.resumeAll({ scope: "process" });
		assertHolds(sync, false, false);
		assert.equal(sync.getReadStatus(1)?.status.name, "PAUSED");
		assert.equal(sync.getReadStatus(1)?.status.info, "admin");
		assert.equal(sync.getQueueStatus(1)?.status.name, "PAUSED");
		assert.match(sync.getQueueStatus(1)?.status.info ?? "", /consecutive failures/);
		assert.equal(sync.getFailedProcess(1)?.tries.length, 1);
		assert.equal(attempts, 1);
		reject = false; await sync.resumeProcessing(1); await within(recovered.promise);
		assert.equal(attempts, 2);
		assert.equal(sync.getReadStatus(1)?.status.name, "PAUSED");
	} finally { await settled(sync); await sync.shutdown(); }
});

for (const scope of ["read", "process"] as const) {
	test(`restart and reload restore a ${scope}-only hold, including newly admitted queries`, { timeout: 10000 }, async (t) => {
		t.mock.method(Math, "random", () => 0);
		let catalog = [paused()], checkpointEntered = deferred(), checkpointRelease = deferred();
		const sync = await HederaEventSync.create({ restClient: transactionRest([[]]).rest, singletonGuard: false, querySource: () => catalog,
			async beforeReplace() { checkpointEntered.resolve(); await checkpointRelease.promise; } });
		try {
			await sync.pauseAll({ scope }); await sync.start();
			assertHolds(sync, scope === "read", scope === "process");
			for (const operation of ["restart", "reload"] as const) {
				checkpointEntered = deferred(); checkpointRelease = deferred();
				if (operation === "reload") catalog = [paused(), query(2)];
				const replacing = operation === "restart" ? sync.restartQueries() : sync.reloadQueries();
				await within(checkpointEntered.promise);
				assertHolds(sync, true, true, true);
				assert.equal(sync.getLifecycleState().operation?.name, `${operation}Queries`);
				assert.equal(sync.getLifecycleState().operation?.scope, undefined);
				checkpointRelease.resolve(); await replacing;
				assertHolds(sync, scope === "read", scope === "process");
			}
			assert.equal(sync.getReadStatus(2)?.status.name, scope === "read" ? "PAUSED" : "RUNNING");
			assert.equal(sync.getQueueStatus(2)?.status.name, scope === "process" ? "PAUSED" : "RUNNING");
			assert.equal(sync.getReadStatus(1)?.status.info, "manual read pause");
			assert.equal(sync.getQueueStatus(1)?.status.info, "manual process pause");
		} finally { checkpointRelease.resolve(); await settled(sync); await sync.shutdown(); }
	});
}

test("precommit replacement failure holds both stages but permits explicit scoped resume", async () => {
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false,
		querySource: () => [paused()], beforeReplace({ reason }) { if (reason === "restart") throw new Error("checkpoint unavailable"); } });
	try {
		await sync.pauseAll({ scope: "process" }); await sync.start();
		await assert.rejects(sync.restartQueries(), /checkpoint unavailable/);
		assertHolds(sync, true, true);
		assert.equal(sync.getLifecycleState().recoveryRequired, false);
		assert.equal(sync.getLifecycleState().generation, 1);
		await sync.resumeAll({ scope: "read" }); assertHolds(sync, false, true);
		assert.equal(sync.getReadStatus(1)?.status.info, "manual read pause");
	} finally { await sync.shutdown(); }
});

test("partial replacement failure blocks every scoped resume until a complete retry", async () => {
	let fail = false;
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false,
		hookFailurePolicy: "fail-fast", querySource: () => [paused(), paused(2)] });
	sync.setHooks({ onReadInit(config) { if (fail && config.id === 2) throw new Error("initialization unavailable"); } });
	try {
		await sync.pauseAll({ scope: "read" }); await sync.start(); fail = true;
		await assert.rejects(sync.restartQueries(), /failed during initialization/);
		assertHolds(sync, true, true, true);
		assert.equal(sync.getLifecycleState().recoveryRequired, true);
		for (const scope of ["read", "process", "both"] as const) await assert.rejects(sync.resumeAll({ scope }), /restart or reload/);
		assertHolds(sync, true, true, true);
		fail = false; await sync.restartQueries();
		assert.equal(sync.getLifecycleState().recoveryRequired, false);
		assertHolds(sync, true, true);
		await sync.resumeAll({ scope: "process" }); assertHolds(sync, true, false);
	} finally { await sync.shutdown(); }
});

test("invalid scopes have no lifecycle, hold, query, or callback side effects", async () => {
	let loads = 0, notifications = 0;
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false,
		querySource: () => { loads++; return [paused()]; } });
	sync.setHooks({ onQueryStatusChange() { notifications++; } });
	try {
		await sync.start(); await sync.pauseAll({ scope: "read" });
		const before = sync.getLifecycleState(), queryBefore = sync.getQuery(1), notificationsBefore = notifications;
		for (const scope of ["processing", "READ", "", null, 0, {}]) {
			await assert.rejects(sync.pauseAll({ scope: scope as GlobalPauseScope }), /scope/);
			await assert.rejects(sync.resumeAll({ scope: scope as GlobalPauseScope }), /scope/);
			assert.deepEqual(sync.getLifecycleState(), before);
			assert.deepEqual(sync.getQuery(1), queryBefore);
		}
		assert.equal(loads, 1);
		assert.equal(notifications, notificationsBefore);
	} finally { await sync.shutdown(); }
});
