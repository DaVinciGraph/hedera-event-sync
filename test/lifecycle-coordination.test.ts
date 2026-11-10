import assert from "node:assert/strict";
import { test } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, LifecycleBusyError, LifecycleReentrancyError, LifecycleTimeoutError, type QueryConfig, type BeforeReplaceContext } from "../src/index";

const deferred = () => {
	let resolve!: () => void;
	return { promise: new Promise<void>((done) => { resolve = done; }), resolve: () => resolve() };
};
const tick = (ms = 20) => new Promise<void>((done) => setTimeout(done, ms));
async function settled(sync: HederaEventSync) {
	for (let i = 0; i < 200 && sync.getLifecycleState().operation; i++) await tick(10);
	assert.equal(sync.getLifecycleState().operation, undefined);
}
const query = (id = 1): Extract<QueryConfig, { type: "Transactions" }> => ({
	type: "Transactions", id, title: `Query ${id}`,
	read: { fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 10 },
		pauseAfterConsecutiveFailures: 2, consistency: { finalityLagSeconds: 0, overlapSeconds: 0 } },
	params: { accountId: "0.0.123", timestamp: "1700000000" },
	process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10 },
});
const paused = (id = 1): Extract<QueryConfig, { type: "Transactions" }> => ({ ...query(id), initialState: {
	read: { name: "PAUSED", info: "manual" }, process: { name: "PAUSED", info: "failure threshold" },
} });
const noNetwork = {} as HederaRestClient;

test("guarded create waits for shutdown disposal after the caller times out", { timeout: 3_000 }, async () => {
	const removalEntered = deferred();
	const removalGate = deferred();
	const options = { restClient: noNetwork, querySource: () => [paused()] };
	const first = await HederaEventSync.create(options);
	const creations: Promise<HederaEventSync>[] = [];
	let created = 0;
	let removals = 0;
	const createReplacement = () => {
		const pending = HederaEventSync.create(options).then((sync) => { created++; return sync; });
		creations.push(pending);
		return pending;
	};
	first.setHooks({ async onQueryRemoved() {
		removals++;
		removalEntered.resolve();
		await removalGate.promise;
	} });
	try {
		await first.start();
		const timedOut = assert.rejects(first.shutdown({ timeoutMs: 30 }), LifecycleTimeoutError);
		await removalEntered.promise;
		createReplacement();
		await timedOut;
		createReplacement();
		await tick(0);
		assert.equal(created, 0, "creates before and after the timeout must both wait for disposal");
		assert.equal(first.getLifecycleState().operation?.timedOut, true);
		await assert.rejects(first.shutdown(), LifecycleBusyError);
		await assert.rejects(first.resumeAll(), LifecycleBusyError);

		removalGate.resolve();
		const [replacement, concurrent] = await Promise.all(creations);
		assert.notEqual(replacement, first);
		assert.equal(concurrent, replacement, "concurrent creates must share one replacement");
		assert.equal(removals, 1);
		assert.equal(first.getLifecycleState().closed, true);
		await replacement.start();
		assert.equal(replacement.getLifecycleState().closed, false);
		assert.equal(await HederaEventSync.create(options), replacement);
	} finally {
		removalGate.resolve();
		await settled(first);
		await first.shutdown();
		for (const sync of new Set(await Promise.all(creations))) await sync.shutdown();
	}
});

test("guarded create waits for an aborted shutdown checkpoint before returning the retained instance", { timeout: 3_000 }, async () => {
	const checkpointEntered = deferred();
	const checkpointGate = deferred();
	let block = true;
	let checkpoints = 0;
	const options = { restClient: noNetwork, querySource: () => [paused()],
		beforeReplace: async () => {
			checkpoints++;
			if (block) { checkpointEntered.resolve(); await checkpointGate.promise; }
		} };
	const first = await HederaEventSync.create(options);
	let pending: Promise<HederaEventSync> | undefined;
	let returned = false;
	try {
		await first.start();
		const timedOut = assert.rejects(first.shutdown({ timeoutMs: 30 }), LifecycleTimeoutError);
		await checkpointEntered.promise;
		await timedOut;
		pending = HederaEventSync.create(options).then((sync) => { returned = true; return sync; });
		await tick(0);
		assert.equal(returned, false, "the checkpoint callback still owns the lifecycle gate");
		checkpointGate.resolve();
		assert.equal(await pending, first);
		assert.equal(first.getLifecycleState().operation, undefined);
		assert.equal(first.getLifecycleState().closed, false);
		assert.equal(first.getLifecycleState().globalPaused, true);
		assert.equal(first.listQueries().length, 1);
		block = false;
		await first.shutdown();
		assert.equal(checkpoints, 2, "shutdown can be retried after the aborted work settles");
	} finally {
		block = false;
		checkpointGate.resolve();
		await settled(first);
		await pending;
		await first.shutdown();
	}
});

function transactionRest(timestamps = ["1700000001.123456789", "1700000002.123456789"]): HederaRestClient {
	const builder: any = new Proxy({}, { get: () => () => builder });
	const next = Object.assign(async () => null, { url: () => null });
	const transactions = timestamps.map((consensus_timestamp, index) => ({ name: "CRYPTOTRANSFER", consensus_timestamp,
		transaction_id: `0.0.123-${consensus_timestamp}`, transaction_hash: `hash${index}`, result: "SUCCESS", nonce: 0,
		transfers: [], token_transfers: [], nft_transfers: [] }));
	return { useProvider() { return this; }, useNetwork() { return this; }, transactions() {
		return { list(callback: any) { callback(builder); return { get: async () => ({ transactions, next }) }; } };
	} } as unknown as HederaRestClient;
}

test("construction requires one authoritative loader and never invokes it or starts queries", async () => {
	await assert.rejects(HederaEventSync.create({ restClient: noNetwork, singletonGuard: false } as any), /querySource/);
	let calls = 0;
	const original = paused();
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		querySource: ({ reason }) => { calls++; assert.equal(reason, "start"); return [original, paused(2)]; } });
	assert.equal(calls, 0);
	assert.equal(sync.getLifecycleState().started, false);
	assert.equal(sync.listQueries().length, 0);
	try {
		await sync.start();
		await sync.start();
		assert.equal(calls, 1);
		original.title = "mutated loader object";
		assert.equal(sync.getQuery(1)?.title, "Query 1");
		assert.equal(sync.listQueries().length, 2); // Equivalent sources with distinct IDs are explicit.
		assert.equal(sync.getLifecycleState().generation, 1);
	} finally { await sync.shutdown(); }
});

test("invalid catalogs and loader errors leave the accepted generation and hold state unchanged", async () => {
	let catalog: readonly QueryConfig[] = [paused()];
	let sourceError = false;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		querySource: () => { if (sourceError) throw new Error("database unavailable"); return catalog; } });
	try {
		await sync.start();
		for (const invalid of [[paused(), paused()], [{ ...paused(), title: "" }], [{ ...paused(), params: { accountId: "0.0.999" } }]]) {
			catalog = invalid;
			await assert.rejects(sync.reloadQueries());
			assert.equal(sync.getLifecycleState().generation, 1);
			assert.equal(sync.getLifecycleState().globalPaused, false);
			assert.equal(sync.getQuery(1)?.title, "Query 1");
		}
		sourceError = true;
		await assert.rejects(sync.reloadQueries(), /database unavailable/);
		assert.equal(sync.getReadStatus(1)?.status.info, "manual");
	} finally { await sync.shutdown(); }
});

test("global hold is independent of manual/failure pauses and survives restart and new query admission", async () => {
	let catalog = [paused()];
	let loads = 0;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		querySource: () => { loads++; return catalog; } });
	try {
		await sync.pauseAll();
		await sync.start();
		assert.equal(sync.getLifecycleState().globalPaused, true);
		await sync.restartQueries();
		assert.equal(loads, 1);
		catalog = [paused(), query(2)];
		await sync.reloadQueries();
		assert.equal(sync.getReadStatus(2)?.status.name, "PAUSED");
		assert.match(sync.getReadStatus(2)?.status.info ?? "", /instance/);
		await sync.resumeRead(1); // Changes independent pause, but cannot bypass global hold.
		assert.equal(sync.getReadStatus(1)?.status.name, "PAUSED");
		await sync.resumeAll();
		assert.equal(sync.getReadStatus(1)?.status.name, "RUNNING");
		assert.equal(sync.getQueueStatus(1)?.status.info, "failure threshold");
		assert.equal(sync.getReadStatus(2)?.status.name, "RUNNING");
	} finally { await sync.shutdown(); }
});

test("durable checkpoint rejection preserves old queued work and permits an explicit retry", { timeout: 12000 }, async () => {
	const started = deferred(), finish = deferred();
	let rejectPersistence = true;
	let saved: BeforeReplaceContext | undefined;
	let stopNotifications = 0;
	const sync = await HederaEventSync.create({ restClient: transactionRest(), singletonGuard: false, querySource: () => [query()],
		beforeReplace: (context) => {
			if (context.reason === "restart" && rejectPersistence) throw new Error("checkpoint database unavailable");
			saved = context;
		} });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle() { started.resolve(); await finish.promise; } });
	sync.setHooks({ onQueryStatusChange(_query, change) { if (change.status.name === "STOPPED") stopNotifications++; } });
	try {
		await sync.start(); await started.promise;
		const restarting = sync.restartQueries();
		finish.resolve();
		await assert.rejects(restarting, /checkpoint database unavailable/);
		assert.equal(sync.getLifecycleState().generation, 1);
		assert.equal(sync.getLifecycleState().globalPaused, true);
		assert.equal(sync.countProcesses(1, "Queued"), 1);
		assert.equal(stopNotifications, 0); // Unannounced staging cleanup must not report false stops for the old generation.
		const checkpoint = sync.getProcessedCheckpoint(1)!;
		assert.equal(checkpoint.timestamp, "1700000001.123456788");
		rejectPersistence = false;
		await sync.restartQueries();
		assert.equal(sync.getLifecycleState().generation, 2);
		assert.equal(sync.getLifecycleState().globalPaused, true);
		assert.deepEqual(saved?.checkpoints[0].checkpoint, checkpoint);
		assert.equal(sync.getQuery(1)?.params.timestamp, checkpoint.timestamp);
	} finally { finish.resolve(); await sync.shutdown(); }
});

test("timeout of a hung handler never overlaps generations or aborts its business work", { timeout: 12000 }, async () => {
	const started = deferred(), finish = deferred();
	let attempts = 0;
	let handlerSignal: AbortSignal | undefined;
	let persistenceCalls = 0;
	const sync = await HederaEventSync.create({ restClient: transactionRest(), singletonGuard: false,
		querySource: () => [query()], beforeReplace: () => { persistenceCalls++; } });
	sync.registry.registerTransaction("CRYPTOTRANSFER", { async handle(_data, context) {
		attempts++; handlerSignal = context.signal; started.resolve(); await finish.promise;
	} });
	try {
		await sync.start(); await started.promise;
		await assert.rejects(sync.restartQueries({ timeoutMs: 30 }), LifecycleTimeoutError);
		assert.equal(sync.getLifecycleState().operation?.timedOut, true);
		assert.equal(sync.getLifecycleState().globalPaused, true);
		assert.equal(handlerSignal?.aborted, false);
		await assert.rejects(sync.resumeAll(), LifecycleBusyError);
		await assert.rejects(sync.resumeRead(1), LifecycleBusyError);
		assert.equal(attempts, 1);
		finish.resolve(); await settled(sync);
		assert.equal(sync.getLifecycleState().generation, 1);
		assert.equal(persistenceCalls, 0);
		assert.equal(attempts, 1);
		await sync.restartQueries();
		assert.equal(sync.getLifecycleState().generation, 2);
	} finally { finish.resolve(); await settled(sync); await sync.shutdown(); }
});

test("a timed-out source retains the running catalog and keeps the lifecycle gate until the source settles", async () => {
	const sourceGate = deferred();
	let loading = false;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		querySource: async ({ reason, signal }) => {
			if (reason === "reload") { loading = true; await sourceGate.promise; assert.equal(signal.aborted, true); return []; }
			return [paused()];
		} });
	try {
		await sync.start();
		await assert.rejects(sync.reloadQueries({ timeoutMs: 20 }), LifecycleTimeoutError);
		assert.equal(loading, true);
		assert.equal(sync.getLifecycleState().globalPaused, false);
		await assert.rejects(sync.pauseAll(), LifecycleBusyError);
		assert.equal(sync.listQueries().length, 1);
		sourceGate.resolve(); await settled(sync);
		assert.equal(sync.listQueries().length, 1);
		assert.equal(sync.getLifecycleState().generation, 1);
	} finally { sourceGate.resolve(); await settled(sync); await sync.shutdown(); }
});

test("loader and durable checkpoint callbacks cannot reenter lifecycle controls", async () => {
	let sync!: HederaEventSync;
	let rejections = 0;
	sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		querySource: async () => { await assert.rejects(sync.pauseAll(), LifecycleReentrancyError); rejections++; return [paused()]; },
		beforeReplace: async () => { await assert.rejects(sync.resumeAll(), LifecycleReentrancyError); rejections++; } });
	try {
		await sync.start(); await sync.restartQueries();
		assert.equal(rejections, 2);
	} finally { await sync.shutdown(); }
});

test("empty source is an intentional complete removal and snapshots cannot mutate lifecycle state", async () => {
	let catalog = [paused()];
	let removed = 0;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false, querySource: () => catalog,
		beforeReplace: ({ reason, checkpoints }) => { if (reason === "reload") assert.deepEqual(checkpoints.map(item => item.query.id), [1]); } });
	sync.setHooks({ onQueryRemoved() { removed++; } });
	try {
		await sync.start(); catalog = [];
		await sync.reloadQueries();
		assert.equal(sync.listQueries().length, 0);
		assert.equal(removed, 1);
		const state = sync.getLifecycleState();
		assert.equal(Object.isFrozen(state), true);
		assert.throws(() => { (state as any).globalPaused = true; });
		assert.equal(sync.getLifecycleState().globalPaused, false);
	} finally { await sync.shutdown(); }
});

test("initialization failure holds the complete installed generation and restart permits explicit stage recovery", async () => {
	let fail = true;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false,
		hookFailurePolicy: "fail-fast", querySource: () => [paused(), paused(2)] });
	sync.setHooks({ onReadInit() { if (fail) throw new Error("initialization failed"); } });
	try {
		await assert.rejects(sync.start(), /failed during initialization/);
		assert.equal(sync.getLifecycleState().globalPaused, true);
		assert.equal(sync.listQueries().length, 2);
		assert.equal(sync.getReadStatus(1)?.status.name, "FAILED");
		assert.equal(sync.getLifecycleState().recoveryRequired, true);
		await assert.rejects(sync.resumeAll(), /restart or reload/);
		fail = false;
		await sync.restartQueries();
		assert.equal(sync.getLifecycleState().recoveryRequired, false);
		assert.equal(sync.getReadStatus(1)?.status.name, "PAUSED");
		await sync.resumeRead(1); await sync.resumeAll();
		assert.equal(sync.getReadStatus(1)?.status.name, "RUNNING");
	} finally { await sync.shutdown(); }
});

test("a timeout during replacement initialization keeps every query held until a complete retry", async () => {
	const initGate = deferred(), entered = deferred();
	let block = false;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false, querySource: () => [paused(), query(2)] });
	sync.setHooks({ async onReadInit(config) { if (block && config.id === 1) { entered.resolve(); await initGate.promise; } } });
	try {
		await sync.start(); block = true;
		const restart = sync.restartQueries({ timeoutMs: 40 });
		await entered.promise;
		await assert.rejects(restart, LifecycleTimeoutError);
		assert.equal(sync.getLifecycleState().operation?.timedOut, true);
		assert.equal(sync.getLifecycleState().recoveryRequired, true);
		assert.equal(sync.getReadStatus(2)?.status.name, "PAUSED");
		initGate.resolve(); await settled(sync);
		await assert.rejects(sync.resumeAll(), /restart or reload/);
		block = false; await sync.restartQueries();
		assert.equal(sync.getLifecycleState().recoveryRequired, false);
		assert.equal(sync.getReadStatus(2)?.status.name, "PAUSED");
		await sync.resumeAll();
		assert.equal(sync.getReadStatus(2)?.status.name, "RUNNING");
	} finally { initGate.resolve(); await settled(sync); await sync.shutdown(); }
});

test("restart retains consecutive read failures and never mistakes global resume for recovery", { timeout: 16000 }, async () => {
	const failedOnce = deferred(), threshold = deferred();
	const counts: number[] = [];
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false, querySource: () => [query()] });
	sync.setHooks({ onReadFailure(_query, _error, count) { counts.push(count); failedOnce.resolve(); if (count === 2) threshold.resolve(); } });
	try {
		await sync.start(); await failedOnce.promise;
		await sync.pauseAll(); await sync.restartQueries();
		await sync.resumeAll(); await threshold.promise;
		assert.deepEqual(counts.slice(0, 2), [1, 2]);
		assert.match(sync.getReadStatus(1)?.status.info ?? "", /consecutive failures/);
	} finally { await sync.shutdown(); }
});

test("checkpoint callback timeout does not discard old queues or accept late persistence as permission to activate", async () => {
	const persistGate = deferred();
	let block = true;
	const sync = await HederaEventSync.create({ restClient: noNetwork, singletonGuard: false, querySource: () => [paused()],
		beforeReplace: async ({ reason }) => { if (reason === "restart" && block) await persistGate.promise; } });
	try {
		await sync.start();
		await assert.rejects(sync.restartQueries({ timeoutMs: 20 }), LifecycleTimeoutError);
		assert.equal(sync.getLifecycleState().generation, 1);
		assert.equal(sync.getLifecycleState().recoveryRequired, false);
		persistGate.resolve(); await settled(sync);
		assert.equal(sync.getLifecycleState().generation, 1);
		assert.equal(sync.getReadStatus(1)?.status.info, "manual");
		block = false;
		await sync.resumeAll();
		assert.equal(sync.getLifecycleState().globalPaused, false);
	} finally { persistGate.resolve(); await settled(sync); await sync.shutdown(); }
});
