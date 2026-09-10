import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { toEventSelector } from "viem";
import { HandlerRegistry, HederaEventSync, LifecycleReentrancyError, LogEventProcessor } from "../src/index";
import { assertOutsideUserCallback, runInUserCallback, type UserCallbackContext } from "../src/eventSync/utils/userCallback";

const outerContext: UserCallbackContext = { kind: "hook", name: "onReadCycleStarted", queryId: 1 };
const innerContext: UserCallbackContext = { kind: "handler", name: "Ping", queryId: null };

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => { resolve = done; });
	return { promise, resolve };
}

describe("user callback ancestry", () => {
	it("guards every active owner through nested callbacks and reports the matching callback", async () => {
		const outer = {};
		const inner = {};
		const independent = {};
		await runInUserCallback(outerContext, async () => {
			await runInUserCallback(innerContext, async () => {
				await Promise.resolve();
				for (const [owner, expected] of [[outer, outerContext], [inner, innerContext]] as const) {
					assert.throws(() => assertOutsideUserCallback("shutdown", owner), (error: unknown) => {
						assert.ok(error instanceof LifecycleReentrancyError);
						assert.deepEqual(error.callback, expected);
						return true;
					});
				}
				assert.doesNotThrow(() => assertOutsideUserCallback("shutdown", independent));
			}, inner);
		}, outer);
		assert.doesNotThrow(() => assertOutsideUserCallback("shutdown", outer));
	});

	it("keeps active ancestors visible in deferred work after the inner callback settles", async () => {
		const outer = {};
		const inner = {};
		const deferredOwner = {};
		const runDeferred = deferred();
		let deferredWork!: Promise<void>;
		await runInUserCallback(outerContext, async () => {
			await runInUserCallback(innerContext, () => {
				deferredWork = runDeferred.promise.then(() => runInUserCallback(innerContext, () => {
					assert.throws(() => assertOutsideUserCallback("shutdown", outer), LifecycleReentrancyError);
					assert.doesNotThrow(() => assertOutsideUserCallback("shutdown", inner));
					assert.throws(() => assertOutsideUserCallback("shutdown", deferredOwner), LifecycleReentrancyError);
				}, deferredOwner));
			}, inner);
			runDeferred.resolve();
			await deferredWork;
		}, outer);
	});

	it("allows deferred lifecycle work after all inherited callbacks have settled, including rejection", async () => {
		const owner = {};
		const runDeferred = deferred();
		let deferredWork!: Promise<void>;
		await assert.rejects(runInUserCallback(outerContext, () => runInUserCallback(innerContext, () => {
			deferredWork = runDeferred.promise.then(() => {
				assert.doesNotThrow(() => assertOutsideUserCallback("shutdown", owner));
				assert.doesNotThrow(() => assertOutsideUserCallback("shutdown"));
			});
			throw new Error("handler failed");
		}), owner), /handler failed/);
		runDeferred.resolve();
		await deferredWork;
	});

	it("prevents a standalone processor nested in a read hook from deadlocking pauseRead", async () => {
		const restClient = {} as HederaRestClient;
		const sync = await HederaEventSync.create({ restClient, singletonGuard: false });
		const registry = new HandlerRegistry();
		const begin = deferred();
		const completed = deferred();
		let failure: unknown;
		let pauseTimeout: ReturnType<typeof setTimeout> | undefined;
		registry.registerLog("Ping", {
			normalize: () => undefined,
			handle: async () => {
				await Promise.race([
					sync.pauseRead(1),
					new Promise<never>((_, reject) => {
						pauseTimeout = setTimeout(() => reject(new Error("nested pauseRead deadlocked")), 1000);
					}),
				]);
			},
		});
		const processor = new LogEventProcessor({ registry, restClient, network: "testnet", provider: "public" });
		sync.setHooks({
			async onReadCycleStarted() {
				await begin.promise;
				try {
					await processor.prepareAndExecute({
						address: `0x${"1".repeat(40)}`, data: "0x", index: 0,
						timestamp: "1.000000000", topics: [toEventSelector("Ping()")],
					}, { abi: [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] });
				} catch (error) {
					failure = error;
				} finally {
					if (pauseTimeout) clearTimeout(pauseTimeout);
					completed.resolve();
				}
			},
		});
		let testTimeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await sync.addQuery({
				type: "Transactions", id: 1, title: "callback ancestry",
				read: {
					fetch: { network: "testnet", restProvider: "public" },
					pauseAfterConsecutiveFailures: 3,
				},
				params: { accountId: "0.0.123", timestamp: "1" },
				process: { pauseAfterConsecutiveFailures: 3 },
			});
			begin.resolve();
			await Promise.race([
				completed.promise,
				new Promise<never>((_, reject) => {
					testTimeout = setTimeout(() => reject(new Error("read hook did not run")), 8000);
				}),
			]);
			assert.ok(failure instanceof LifecycleReentrancyError);
			assert.equal(failure.operation, "pauseRead");
			assert.deepEqual(failure.callback, outerContext);
		} finally {
			begin.resolve();
			if (testTimeout) clearTimeout(testTimeout);
			await sync.shutdown();
		}
	});
});
