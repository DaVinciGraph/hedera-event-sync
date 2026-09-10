import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Abi } from "abitype";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, LifecycleBusyError, LifecycleReentrancyError } from "../src/index";
import type { ContractLogTopicsFilter, QueryConfig } from "../src/eventSync/types/config";

const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const satisfies Abi;

function query(id = 1): Extract<QueryConfig, { type: "Multi-Contract-Logs" }> {
	return {
		type: "Multi-Contract-Logs",
		id,
		title: `query ${id}`,
		read: {
			fetch: { network: "private-network", restProvider: "custom" },
			pauseAfterConsecutiveFailures: 3,
		},
		params: { topics: ["0x1"], timestamp: "1" },
		abi,
		process: { pauseAfterConsecutiveFailures: 3 },
	};
}

function transactionQuery(id = 100): Extract<QueryConfig, { type: "Transactions" }> {
	return {
		type: "Transactions",
		id,
		title: `transaction query ${id}`,
		read: {
			fetch: { network: "private-network", restProvider: "custom" },
			pauseAfterConsecutiveFailures: 3,
		},
		params: { accountId: "0.0.123", timestamp: "1" },
		process: { pauseAfterConsecutiveFailures: 3 },
	};
}

describe("HederaEventSync lifecycle", () => {
	it("resolves defaults, protects internal state, and rejects duplicate ids", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const input = query();
		await sync.addQuery(input);
		input.title = "mutated input";
		const resolved = sync.getQuery(1)!;
		assert.equal(resolved.read.fetch.pollIntervalSeconds, 30);
		assert.equal(resolved.read.fetch.batchSize, 100);
		assert.deepEqual(resolved.read.consistency, { finalityLagSeconds: 15, overlapSeconds: 30 });
		assert.equal(resolved.params.timestamp, "1.000000000");

		resolved.title = "mutated snapshot";
		assert.equal(sync.getQuery(1)?.title, "query 1");
		await assert.rejects(sync.addQuery(query()), /already exists/);
		await sync.shutdown();
	});

	it("updates hooks already held by running query runtimes", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		let statusChanges = 0;
		await sync.addQuery(query());
		sync.setHooks({ onQueryStatusChange: () => void statusChanges++ });
		await sync.pauseRead(1);
		assert.ok(statusChanges > 0);
		await sync.shutdown();
	});

	it("announces a query before initializing its read loop", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const events: string[] = [];
		sync.setHooks({
			onQueryAdded: () => void events.push("added"),
			onReadInit: () => void events.push("read-init"),
		});
		await sync.addQuery(query(30));
		assert.deepEqual(events.slice(0, 2), ["added", "read-init"]);
		await sync.shutdown();
	});

	it("rejects invalid runtime values before starting a query", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const invalid = query();
		invalid.read.fetch.batchSize = Number.NaN;
		await assert.rejects(sync.addQuery(invalid), /batchSize/);
		await sync.shutdown();
	});

	it("rejects malformed event ABI items during add and update", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const malformedEvents: unknown[] = [
			{ type: "event", anonymous: false, inputs: [] },
			{ type: "event", anonymous: false, name: "MissingInputs" },
			{ type: "event", anonymous: false, name: "InvalidType", inputs: [{ name: "value", type: "notatype" }] },
			{ type: "event", anonymous: "false", name: "InvalidAnonymous", inputs: [] },
			{ type: "event", anonymous: false, name: "InvalidIndexed", inputs: [{ name: "value", type: "uint256", indexed: "false" }] },
		];

		for (let index = 0; index < malformedEvents.length; index++) {
			const invalid = query(70 + index);
			invalid.abi = [malformedEvents[index], ...abi] as unknown as Abi;
			await assert.rejects(sync.addQuery(invalid), /malformed event ABI item at index 0/);
		}

		const validWithNonEventItem = query(73);
		validWithNonEventItem.abi = [
			{ type: "function", name: "ping", stateMutability: "view", inputs: [], outputs: [] },
			...abi,
		] as const satisfies Abi;
		await sync.addQuery(validWithNonEventItem);

		const invalidUpdate = query(73);
		invalidUpdate.abi = [{ type: "event", anonymous: false, name: "InvalidType", inputs: [{ type: "notatype" }] }] as unknown as Abi;
		await assert.rejects(sync.updateQuery(invalidUpdate), /malformed event ABI item at index 0/);
		assert.equal(sync.getQuery(73)?.title, "query 73");

		await sync.shutdown();
	});

	it("contains opaque ABI-validation failures inside a descriptive Error", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const thrown = Proxy.revocable({}, {});
		thrown.revoke();
		const hostileEvent = {
			type: "event",
			anonymous: false,
			inputs: [],
			get name() {
				throw thrown.proxy;
			},
		};
		const invalid = query(79);
		invalid.abi = [hostileEvent] as unknown as Abi;

		await assert.rejects(
			sync.addQuery(invalid),
			/malformed event ABI item at index 0: \[unserializable thrown value\]/i
		);
		await sync.shutdown();
	});

	it("accepts repeated topic filters in any Mirror Node topic position", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const independentTopics = query(6);
		independentTopics.params.topics = [undefined, ["0x2", "0x22"]];
		await sync.addQuery(independentTopics);
		const added = sync.getQuery(6);
		assert.equal(added?.type, "Multi-Contract-Logs");
		if (added?.type === "Multi-Contract-Logs") {
			assert.deepEqual(added.params.topics, [undefined, ["0x2", "0x22"]]);
		}
		await sync.shutdown();
	});

	it("serializes concurrent query additions so an id can own only one runtime", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const results = await Promise.allSettled([sync.addQuery(query(7)), sync.addQuery(query(7))]);
		assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
		assert.equal(results.filter((result) => result.status === "rejected").length, 1);
		assert.equal(sync.listQueries().length, 1);
		await sync.shutdown();
	});

	it("rejects a duplicate add using the captured ID when the caller mutates its input", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(transactionQuery(1));
		const originalRuntime = (sync as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.get(1)!;
		try {
			const input = transactionQuery(1);
			input.params.accountId = "0.0.456";
			const addition = sync.addQuery(input);
			input.id = 2;

			await assert.rejects(addition, /Query 1 already exists/);
			const original = sync.getQuery(1);
			assert.ok(original?.type === "Transactions");
			assert.equal(original.params.accountId, "0.0.123");
			assert.equal(sync.getQuery(2), undefined);
		} finally {
			await sync.shutdown();
			// Also stop the original if a regression overwrites and orphans it.
			await originalRuntime.stop();
		}
	});

	it("adds the captured query ID when the caller reuses its input for an existing ID", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		try {
			await sync.addQuery(transactionQuery(2));
			const input = transactionQuery(1);
			input.params.accountId = "0.0.456";
			const addition = sync.addQuery(input);
			input.id = 2;

			await addition;
			const added = sync.getQuery(1);
			const existing = sync.getQuery(2);
			assert.ok(added?.type === "Transactions");
			assert.ok(existing?.type === "Transactions");
			assert.equal(added.params.accountId, "0.0.456");
			assert.equal(existing.params.accountId, "0.0.123");
		} finally {
			await sync.shutdown();
		}
	});

	it("updates only the captured query ID when the caller mutates its input", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(transactionQuery(1));
		const other = transactionQuery(2);
		other.params.accountId = "0.0.456";
		await sync.addQuery(other);
		const originalRuntime = (sync as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.get(1)!;
		const removed: number[] = [];
		sync.setHooks({ onQueryRemoved: (query) => void removed.push(query.id) });
		try {
			const input = transactionQuery(1);
			input.params.accountId = "0.0.789";
			const update = sync.updateQuery(input);
			input.id = 2;

			await update;
			assert.deepEqual([...removed], [1]);
			const updated = sync.getQuery(1);
			const unchanged = sync.getQuery(2);
			assert.ok(updated?.type === "Transactions");
			assert.ok(unchanged?.type === "Transactions");
			assert.equal(updated.params.accountId, "0.0.789");
			assert.equal(unchanged.params.accountId, "0.0.456");
			assert.equal(sync.getReadStatus(2)?.status.name, "RUNNING");
			assert.equal(sync.getQueueStatus(2)?.status.name, "RUNNING");
		} finally {
			await sync.shutdown();
			// Also stop the original if a regression replaces it without stopping it.
			await originalRuntime.stop();
		}
	});

	it("keeps a missing update ID missing when the caller changes it to an existing ID", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		try {
			await sync.addQuery(transactionQuery(2));
			const input = transactionQuery(1);
			input.params.accountId = "0.0.456";
			const update = sync.updateQuery(input);
			input.id = 2;

			await assert.rejects(update, /Query 1 not found/);
			assert.equal(sync.getQuery(1), undefined);
			assert.equal(sync.getReadStatus(2)?.status.name, "RUNNING");
			assert.equal(sync.getQueueStatus(2)?.status.name, "RUNNING");
		} finally {
			await sync.shutdown();
		}
	});

	it("excludes the captured ID from duplicate checks when the caller mutates its update input", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(transactionQuery(1));
		const originalRuntime = (sync as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.get(1)!;
		try {
			const other = transactionQuery(2);
			other.params.accountId = "0.0.456";
			await sync.addQuery(other);
			const input = transactionQuery(1);
			input.title = "updated title";
			const update = sync.updateQuery(input);
			input.id = 2;

			await update;
			assert.equal(sync.getQuery(1)?.title, "updated title");
			assert.equal(sync.getQuery(2)?.title, "transaction query 2");
		} finally {
			await sync.shutdown();
			await originalRuntime.stop();
		}
	});

	for (const sparseFirst of [true, false]) {
		const order = sparseFirst ? "sparse first" : "sparse second";
		const sparseTopics: ContractLogTopicsFilter = [, "0x1"];
		const restrictedTopics: ContractLogTopicsFilter = ["0x2", "0x1"];

		it(`keeps distinct topic filters when adding queries (${order})`, async () => {
			const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
			try {
				const first = query(1);
				const second = query(2);
				first.params.topics = sparseFirst ? sparseTopics : restrictedTopics;
				second.params.topics = sparseFirst ? restrictedTopics : sparseTopics;
				await sync.addQuery(first);
				await sync.addQuery(second);
				assert.deepEqual(sync.listQueries().map((item) => item.id), [1, 2]);
			} finally {
				await sync.shutdown();
			}
		});

		it(`keeps distinct topic filters when updating queries (${order})`, async () => {
			const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
			try {
				const first = query(1);
				first.params.topics = sparseFirst ? sparseTopics : restrictedTopics;
				await sync.addQuery(first);
				await sync.addQuery(query(2));
				const replacement = query(2);
				replacement.params.topics = sparseFirst ? restrictedTopics : sparseTopics;
				await sync.updateQuery(replacement);
				const updated = sync.getQuery(2);
				assert.equal(updated?.type, "Multi-Contract-Logs");
				if (updated?.type === "Multi-Contract-Logs") {
					assert.deepEqual(updated.params.topics, replacement.params.topics);
				}
			} finally {
				await sync.shutdown();
			}
		});

		it(`treats empty topic positions as undefined during duplicate checks (${order})`, async () => {
			const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
			try {
				const explicitTopics: ContractLogTopicsFilter = [undefined, "0x1"];
				const first = query(1);
				const duplicate = query(2);
				first.params.topics = sparseFirst ? sparseTopics : explicitTopics;
				duplicate.params.topics = sparseFirst ? explicitTopics : sparseTopics;
				await sync.addQuery(first);
				await sync.addQuery(duplicate);
				assert.equal(sync.getQuery(2), undefined);

				await sync.addQuery(query(2));
				const beforeUpdate = sync.getQuery(2);
				await sync.updateQuery(duplicate);
				assert.deepEqual(sync.getQuery(2), beforeUpdate);
			} finally {
				await sync.shutdown();
			}
		});
	}

	it("rejects reentrant lifecycle controls instead of deadlocking an active callback", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		let callbackError: unknown;
		sync.setHooks({
			async onQueryAdded(added) {
				if (added.id === 40) await sync.addQuery(query(41));
			},
			onHookError(failure) {
				callbackError = failure.error;
			},
		});

		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				sync.addQuery(query(40)),
				new Promise<never>((_, reject) => {
					timeout = setTimeout(() => reject(new Error("lifecycle operation deadlocked")), 500);
				}),
			]);
		} finally {
			if (timeout) clearTimeout(timeout);
		}
		assert.ok(callbackError instanceof LifecycleReentrancyError);
		assert.equal(sync.getQuery(41), undefined);
		await sync.shutdown();
	});

	it("allows an active hook to await independently scheduled work for another query", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		let releaseExternal!: () => void;
		const externalSignal = new Promise<void>((resolve) => { releaseExternal = resolve; });
		const externalAddition = externalSignal.then(() => sync.addQuery(query(42), false));
		sync.setHooks({
			async onQueryAdded(added) {
				if (added.id !== 41) return;
				releaseExternal();
				await externalAddition;
			},
		});

		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				sync.addQuery(query(41)),
				new Promise<never>((_, reject) => {
					timeout = setTimeout(() => reject(new Error("independent lifecycle operation deadlocked")), 500);
				}),
			]);
		} finally {
			if (timeout) clearTimeout(timeout);
		}
		assert.ok(sync.getQuery(41));
		assert.ok(sync.getQuery(42));
		await sync.shutdown();
	});

	it("rejects same-query lifecycle work arriving through an independent async context", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		let releaseExternal!: () => void;
		const externalSignal = new Promise<void>((resolve) => { releaseExternal = resolve; });
		const externalRemoval = externalSignal.then(() => sync.removeQuery(43));
		let callbackError: unknown;
		sync.setHooks({
			async onQueryAdded(added) {
				if (added.id !== 43) return;
				releaseExternal();
				try {
					await externalRemoval;
				} catch (error) {
					callbackError = error;
				}
			},
		});

		await sync.addQuery(query(43));
		assert.ok(callbackError instanceof LifecycleBusyError);
		assert.ok(sync.getQuery(43));
		await sync.shutdown();
	});

	it("keeps callback activity isolated between synchronizer instances", async () => {
		const first = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const second = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		first.setHooks({
			async onQueryAdded(added) {
				if (added.id === 44) await second.addQuery(query(44));
			},
		});

		await first.addQuery(query(44));
		assert.ok(first.getQuery(44));
		assert.ok(second.getQuery(44));
		await first.shutdown();
		await second.shutdown();
	});

	it("updates and removes a query without exposing an incomplete replacement", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const events: string[] = [];
		sync.setHooks({
			onQueryAdded: (added) => void events.push(`added:${added.title}`),
			onQueryRemoved: (removed) => void events.push(`removed:${removed.title}`),
		});
		await sync.addQuery(query(45));
		const updated = query(45);
		updated.title = "updated query";
		updated.process.holdPeriod = "weekly";
		await sync.updateQuery(updated);

		assert.equal(sync.getQuery(45)?.title, "updated query");
		assert.deepEqual(events, ["added:query 45", "removed:query 45", "added:updated query"]);
		await sync.removeQuery(45);
		assert.equal(sync.getQuery(45), undefined);
		assert.deepEqual(events, ["added:query 45", "removed:query 45", "added:updated query", "removed:updated query"]);
		await sync.shutdown();
	});

	it("rejects shutdown immediately while an independently observed hook is active", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		let hookStarted!: () => void;
		let releaseHook!: () => void;
		const started = new Promise<void>((resolve) => { hookStarted = resolve; });
		const release = new Promise<void>((resolve) => { releaseHook = resolve; });
		sync.setHooks({
			async onQueryAdded(added) {
				if (added.id !== 46) return;
				hookStarted();
				await release;
			},
		});

		const addition = sync.addQuery(query(46));
		await started;
		await assert.rejects(sync.shutdown(), LifecycleBusyError);
		releaseHook();
		await addition;
		await sync.shutdown();
	});

	it("does not mistake queries with different processing behavior for duplicates", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const first = query(1);
		const second = query(2);
		second.process.holdPeriod = "weekly";
		await sync.addQuery(first);
		await sync.addQuery(second);
		assert.equal(sync.listQueries().length, 2);
		await sync.shutdown();
	});

	it("requires the canonical contract.address for single-contract log queries", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const canonicalAddress = "0.0.111";
		const canonical = {
			...query(50),
			type: "Single-Contract-Logs" as const,
			contract: { address: canonicalAddress },
			params: { timestamp: "1" },
		};
		await sync.addQuery(canonical);

		const stored = sync.getQuery(50);
		assert.equal(stored?.type, "Single-Contract-Logs");
		if (stored?.type === "Single-Contract-Logs") {
			assert.equal(stored.contract.address, canonicalAddress);
		}

		const missingCanonicalSource = {
			...canonical,
			id: 51,
			title: "missing canonical source",
			contract: undefined,
			params: { contractAddress: canonicalAddress, timestamp: "1" },
		} as unknown as QueryConfig;
		await assert.rejects(sync.addQuery(missingCanonicalSource), /contract\.address is required/);
		assert.equal(sync.listQueries().length, 1);
		await sync.shutdown();
	});

	it("requires params.accountId for transaction queries without deriving it from metadata", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const common = {
			type: "Transactions" as const,
			read: query().read,
			process: { pauseAfterConsecutiveFailures: 3 },
		};
		await sync.addQuery({
			...common,
			id: 52,
			title: "transaction source",
			contract: { id: "0.0.333" },
			params: { accountId: "0.0.111", timestamp: "1" },
		});

		const stored = sync.getQuery(52);
		assert.equal(stored?.type, "Transactions");
		if (stored?.type === "Transactions") {
			assert.equal(stored.params.accountId, "0.0.111");
		}

		const missingAccountId = {
			...common,
			id: 53,
			title: "missing account id",
			contract: { id: "0.0.333" },
			params: { contractAddress: "0.0.111", timestamp: "1" },
		} as unknown as QueryConfig;
		await assert.rejects(sync.addQuery(missingAccountId), /accountId is required/);
		assert.equal(sync.listQueries().length, 1);
		await sync.shutdown();
	});

	it("canonicalizes execution defaults before duplicate checks", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(query(54));
		const explicitLogDefaults = query(55);
		explicitLogDefaults.params.index = 0;
		explicitLogDefaults.process.skipEventNames = [];
		await sync.addQuery(explicitLogDefaults);
		assert.equal(sync.listQueries().length, 1);

		const transaction = {
			type: "Transactions" as const,
			id: 56,
			title: "implicit transaction defaults",
			read: query().read,
			params: { accountId: "0.0.111", timestamp: "1" },
			process: { pauseAfterConsecutiveFailures: 3 },
		};
		await sync.addQuery(transaction);
		await sync.addQuery({
			...transaction,
			id: 57,
			title: "explicit transaction defaults",
			params: { ...transaction.params, index: 0, result: "success" },
			process: { ...transaction.process, skipTransactionTypes: [], unhandledTransactionPolicy: "error" },
		});
		assert.equal(sync.listQueries().length, 2);

		const stored = sync.getQuery(56);
		assert.equal(stored?.type, "Transactions");
		if (stored?.type === "Transactions") {
			assert.equal(stored.params.result, "success");
			assert.equal(stored.process.unhandledTransactionPolicy, "error");
		}
		await sync.shutdown();
	});

	it("releases the guarded singleton when shut down", async () => {
		const first = await HederaEventSync.create({ restClient: {} as HederaRestClient });
		await first.shutdown();
		const second = await HederaEventSync.create({ restClient: {} as HederaRestClient });
		assert.notEqual(first, second);
		await second.shutdown();
	});

	const controls = ["pauseRead", "resumeRead", "pauseProcessing", "resumeProcessing"] as const;

	it("rejects query controls while shutdown is waiting for a runtime to stop", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(query(1));
		const runtime = (sync as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.get(1)!;
		const originalStop = runtime.stop.bind(runtime);
		let signalStop!: () => void;
		const stopStarted = new Promise<void>((resolve) => { signalStop = resolve; });
		let releaseStop!: () => void;
		const stopGate = new Promise<void>((resolve) => { releaseStop = resolve; });
		runtime.stop = async () => {
			const stopping = originalStop();
			signalStop();
			await stopGate;
			await stopping;
		};

		const shutdown = sync.shutdown();
		try {
			await stopStarted;
			assert.ok(sync.getQuery(1));
			await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down/, control)));
		} finally {
			releaseStop();
			await shutdown;
		}
	});

	it("rejects query controls queued behind shutdown and after shutdown completes", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(query(1));
		const shutdown = sync.shutdown();
		try {
			await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down/, control)));
		} finally {
			await shutdown;
		}
		assert.equal(sync.getQuery(1), undefined);
		await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down/, control)));
	});

	it("still ignores missing query IDs in controls on an active instance", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		try {
			await Promise.all(controls.map((control) => sync[control](999)));
			assert.equal(sync.listQueries().length, 0);
		} finally {
			await sync.shutdown();
		}
	});

	it("keeps the guarded singleton occupied until asynchronous shutdown settles", async () => {
		const first = await HederaEventSync.create({ restClient: {} as HederaRestClient });
		await first.addQuery(query(60));
		const runtime = [...(first as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.values()][0];
		const originalStop = runtime.stop.bind(runtime);
		let signalStop!: () => void;
		const stopStarted = new Promise<void>((resolve) => { signalStop = resolve; });
		let releaseStop!: () => void;
		const stopGate = new Promise<void>((resolve) => { releaseStop = resolve; });
		runtime.stop = async () => {
			const stopping = originalStop();
			signalStop();
			await stopGate;
			await stopping;
		};

		const shutdown = first.shutdown();
		await stopStarted;
		let replacementCreated = false;
		const replacementPromise = HederaEventSync.create({ restClient: {} as HederaRestClient }).then((instance) => {
			replacementCreated = true;
			return instance;
		});
		await new Promise<void>((resolve) => setImmediate(resolve));
		assert.equal(replacementCreated, false);

		releaseStop();
		await shutdown;
		const replacement = await replacementPromise;
		assert.notEqual(replacement, first);
		assert.equal(await HederaEventSync.create({ restClient: {} as HederaRestClient }), replacement);
		await replacement.shutdown();
	});

	it("rejects guarded create from a callback awaited by singleton shutdown", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient });
		await sync.addQuery(query(69));
		let checked = false;
		sync.setHooks({
			async onQueryRemoved() {
				await assert.rejects(
					HederaEventSync.create({ restClient: {} as HederaRestClient }),
					LifecycleReentrancyError
				);
				checked = true;
			},
		});

		await sync.shutdown();
		assert.equal(checked, true);
	});

	it("rejects hook policies that JavaScript callers cannot express through the TypeScript union", async () => {
		await assert.rejects(
			HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, hookFailurePolicy: "invalid" as never }),
			/hookFailurePolicy must be one of/
		);
	});

	it("requires a decodable non-anonymous event while accepting mixed ABIs", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const anonymousOnly = query(61);
		anonymousOnly.abi = [{ type: "event", name: "Hidden", anonymous: true, inputs: [] }] as const;
		await assert.rejects(sync.addQuery(anonymousOnly), /at least one non-anonymous event/);

		const mixed = query(62);
		mixed.abi = [
			{ type: "event", name: "Hidden", anonymous: true, inputs: [] },
			{ type: "event", name: "Ping", anonymous: false, inputs: [] },
		] as const;
		await sync.addQuery(mixed);
		assert.ok(sync.getQuery(62));
		await sync.shutdown();
	});

	it("rejects only topic-filtered overlaps that cannot move the seven-day window forward", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const stalled = query(63);
		stalled.read.consistency = { overlapSeconds: 604_799.999_999_999 };
		await assert.rejects(sync.addQuery(stalled), /usable seven-day topic-query window/);

		const advancing = query(64);
		advancing.read.consistency = { overlapSeconds: 604_799.999_999_998 };
		await sync.addQuery(advancing, false);

		const unfilteredSingle = {
			...query(65),
			type: "Single-Contract-Logs" as const,
			contract: { address: "0.0.123" },
			params: { timestamp: "1" },
			read: { ...query().read, consistency: { overlapSeconds: 604_800 } },
		};
		await sync.addQuery(unfilteredSingle, false);

		const transactions = transactionQuery(66);
		transactions.read.consistency = { overlapSeconds: 604_800 };
		await sync.addQuery(transactions, false);
		assert.equal(sync.listQueries().length, 3);
		await sync.shutdown();
	});

	it("validates and canonicalizes transaction result filters at the runtime boundary", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		const uppercase = transactionQuery(67);
		uppercase.params.result = "SUCCESS";
		await sync.addQuery(uppercase);
		const stored = sync.getQuery(67);
		assert.equal(stored?.type, "Transactions");
		if (stored?.type === "Transactions") assert.equal(stored.params.result, "success");

		const invalid = transactionQuery(68);
		invalid.params.result = "successful" as never;
		await assert.rejects(sync.addQuery(invalid), /result must be success, fail, or null/);
		await sync.shutdown();
	});

	it("signals every query runtime before waiting for any one runtime to stop", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(query(58), false);
		await sync.addQuery(query(59), false);
		const runtimes = [...(sync as unknown as { queries: Map<number, { stop(): Promise<void> }> }).queries.values()];
		assert.equal(runtimes.length, 2);

		let releaseFirst!: () => void;
		const firstBlocked = new Promise<void>((resolve) => { releaseFirst = resolve; });
		let firstSignalled!: () => void;
		const firstStarted = new Promise<void>((resolve) => { firstSignalled = resolve; });
		let secondSignalled = false;
		const firstStop = runtimes[0].stop.bind(runtimes[0]);
		const secondStop = runtimes[1].stop.bind(runtimes[1]);
		runtimes[0].stop = async () => {
			const stopping = firstStop();
			firstSignalled();
			await firstBlocked;
			await stopping;
		};
		runtimes[1].stop = async () => {
			secondSignalled = true;
			await secondStop();
		};

		const shutdown = sync.shutdown();
		try {
			await firstStarted;
			await new Promise<void>((resolve) => setImmediate(resolve));
			assert.equal(secondSignalled, true);
		} finally {
			releaseFirst();
			await shutdown;
		}
	});

	it("pauses the affected read job when hookFailurePolicy is pause", async () => {
		const sync = await HederaEventSync.create({
			restClient: {} as HederaRestClient,
			singletonGuard: false,
			hookFailurePolicy: "pause",
		});
		let reportedHook: string | undefined;
		sync.setHooks({
			onReadInit() {
				throw new Error("read observer unavailable");
			},
			onHookError(failure) {
				reportedHook = failure.hook;
			},
		});
		await sync.addQuery(query(21));

		assert.equal(sync.getReadStatus(21)?.status.name, "PAUSED");
		assert.equal(reportedHook, "onReadInit");
		await sync.shutdown();
	});

	it("stops a query runtime immediately when hookFailurePolicy is fail-fast", async () => {
		const sync = await HederaEventSync.create({
			restClient: {} as HederaRestClient,
			singletonGuard: false,
			hookFailurePolicy: "fail-fast",
		});
		let failures = 0;
		sync.setHooks({
			onReadInit: async () => { throw new Error("fatal observer failure"); },
			onHookError: () => void failures++,
		});
		await sync.addQuery(query(22));

		assert.equal(failures, 1);
		assert.equal(sync.getReadStatus(22)?.status.name, "FAILED");
		assert.equal(sync.getQueueStatus(22)?.status.name, "FAILED");
		await assert.rejects(sync.resumeRead(22), /failed read job/);
		await sync.shutdown();
	});

	it("summarizes read and processing states independently", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false });
		await sync.addQuery(query(31));
		await sync.pauseRead(31);
		const summary = sync.getSummary();
		assert.equal(summary.totalQueries, 1);
		assert.equal(summary.runningQueries, 0);
		assert.equal(summary.pausedQueries, 1);
		assert.equal(summary.read.paused, 1);
		assert.equal(summary.process.running, 1);
		await sync.shutdown();
	});
});
