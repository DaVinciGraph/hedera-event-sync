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

async function fixture(options: Omit<Parameters<typeof HederaEventSync.create>[0], "querySource"> = { restClient: {} as HederaRestClient, singletonGuard: false }) {
	let catalog: readonly QueryConfig[] = [];
	const sync = await HederaEventSync.create({ ...options, querySource: () => catalog });
	return { sync, async load(...queries: QueryConfig[]) {
		catalog = queries;
		if (sync.getLifecycleState().started) await sync.reloadQueries();
		else await sync.start();
	} };
}

describe("HederaEventSync lifecycle", () => {
	it("resolves defaults, protects internal state, and rejects duplicate ids", async () => {
		const { sync, load } = await fixture();
		const input = query();
		await load(input);
		input.title = "mutated input";
		const resolved = sync.getQuery(1)!;
		assert.equal(resolved.read.fetch.pollIntervalSeconds, 30);
		assert.equal(resolved.read.fetch.batchSize, 100);
		assert.deepEqual(resolved.read.consistency, { finalityLagSeconds: 15, overlapSeconds: 30 });
		assert.equal(resolved.params.timestamp, "1.000000000");

		resolved.title = "mutated snapshot";
		assert.equal(sync.getQuery(1)?.title, "query 1");
		await assert.rejects(load(query(), query()), /occurs more than once/);
		await sync.shutdown();
	});

	it("updates hooks already held by running query runtimes", async () => {
		const { sync, load } = await fixture();
		let statusChanges = 0;
		await load(query());
		sync.setHooks({ onQueryStatusChange: () => void statusChanges++ });
		await sync.pauseRead(1);
		assert.ok(statusChanges > 0);
		await sync.shutdown();
	});

	it("announces a query before initializing its read loop", async () => {
		const { sync, load } = await fixture();
		const events: string[] = [];
		sync.setHooks({
			onQueryAdded: () => void events.push("added"),
			onReadInit: () => void events.push("read-init"),
		});
		await load(query(30));
		assert.deepEqual(events.slice(0, 2), ["added", "read-init"]);
		await sync.shutdown();
	});

	it("rejects invalid runtime values before starting a query", async () => {
		const { sync, load } = await fixture();
		const invalid = query();
		invalid.read.fetch.batchSize = Number.NaN;
		await assert.rejects(load(invalid), /batchSize/);
		await sync.shutdown();
	});

	it("rejects malformed event ABI items during start and reload", async () => {
		const { sync, load } = await fixture();
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
			await assert.rejects(load(invalid), /malformed event ABI item at index 0/);
		}

		const validWithNonEventItem = query(73);
		validWithNonEventItem.abi = [
			{ type: "function", name: "ping", stateMutability: "view", inputs: [], outputs: [] },
			...abi,
		] as const satisfies Abi;
		await load(validWithNonEventItem);

		const invalidUpdate = query(73);
		invalidUpdate.abi = [{ type: "event", anonymous: false, name: "InvalidType", inputs: [{ type: "notatype" }] }] as unknown as Abi;
		await assert.rejects(load(invalidUpdate), /malformed event ABI item at index 0/);
		assert.equal(sync.getQuery(73)?.title, "query 73");

		await sync.shutdown();
	});

	it("contains opaque ABI-validation failures inside a descriptive Error", async () => {
		const { sync, load } = await fixture();
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
			load(invalid),
			/malformed event ABI item at index 0: \[unserializable thrown value\]/i
		);
		await sync.shutdown();
	});

	it("accepts repeated topic filters in any Mirror Node topic position", async () => {
		const { sync, load } = await fixture();
		const independentTopics = query(6);
		independentTopics.params.topics = [undefined, ["0x2", "0x22"]];
		await load(independentTopics);
		const added = sync.getQuery(6);
		assert.equal(added?.type, "Multi-Contract-Logs");
		if (added?.type === "Multi-Contract-Logs") {
			assert.deepEqual(added.params.topics, [undefined, ["0x2", "0x22"]]);
		}
		await sync.shutdown();
	});

	it("rejects a duplicate ID catalog before replacing the accepted generation", async () => {
		const { sync, load } = await fixture();
		try {
			await load(transactionQuery(1));
			await assert.rejects(load(transactionQuery(2), transactionQuery(2)), /occurs more than once/);
			assert.deepEqual(sync.listQueries().map(item => item.id), [1]);
			assert.equal(sync.getReadStatus(1)?.status.name, "RUNNING");
		} finally { await sync.shutdown(); }
	});

	it("clones the accepted source catalog so later caller mutation cannot change it", async () => {
		const input = transactionQuery(1);
		const { sync, load } = await fixture();
		try {
			await load(input);
			input.id = 2;
			input.params.accountId = "0.0.456";
			await sync.restartQueries();
			const current = sync.getQuery(1);
			assert.ok(current?.type === "Transactions");
			assert.equal(current.params.accountId, "0.0.123");
			assert.equal(sync.getQuery(2), undefined);
		} finally { await sync.shutdown(); }
	});

	for (const sparseFirst of [true, false]) {
		it(`retains distinct IDs and sparse topic positions (${sparseFirst ? "sparse first" : "sparse second"})`, async () => {
			const { sync, load } = await fixture();
			try {
				const first = query(1), second = query(2);
				first.params.topics = sparseFirst ? [, "0x1"] : ["0x2", "0x1"];
				second.params.topics = sparseFirst ? ["0x2", "0x1"] : [, "0x1"];
				await load(first, second);
				assert.deepEqual(sync.listQueries().map(item => item.id), [1, 2]);
				const loaded = sync.getQuery(1);
				assert.ok(loaded?.type === "Multi-Contract-Logs");
				assert.deepEqual(loaded.params.topics, first.params.topics);
				await sync.restartQueries();
				assert.deepEqual(sync.listQueries().map(item => item.id), [1, 2]);
			} finally { await sync.shutdown(); }
		});
	}

	it("rejects reentrant lifecycle controls instead of deadlocking an active callback", async () => {
		const { sync, load } = await fixture();
		let callbackError: unknown;
		sync.setHooks({
			async onQueryAdded(added) {
				if (added.id === 40) await sync.reloadQueries();
			},
			onHookError(failure) {
				callbackError = failure.error;
			},
		});

		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				load(query(40)),
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

	it("rejects overlapping instance lifecycle work from an independent async context", async () => {
		const { sync, load } = await fixture();
		let releaseExternal!: () => void;
		const externalSignal = new Promise<void>(resolve => { releaseExternal = resolve; });
		const externalRestart = externalSignal.then(() => sync.restartQueries());
		let callbackError: unknown;
		sync.setHooks({ async onQueryAdded() {
			releaseExternal();
			try { await externalRestart; } catch (error) { callbackError = error; }
		} });
		await load(query(43));
		assert.ok(callbackError instanceof LifecycleBusyError);
		assert.ok(sync.getQuery(43));
		await sync.shutdown();
	});

	it("keeps callback activity isolated between synchronizer instances", async () => {
		const { sync: first, load: loadFirst } = await fixture();
		const { sync: second, load: loadSecond } = await fixture();
		first.setHooks({
			async onQueryAdded(added) {
				if (added.id === 44) await loadSecond(query(44));
			},
		});

		await loadFirst(query(44));
		assert.ok(first.getQuery(44));
		assert.ok(second.getQuery(44));
		await first.shutdown();
		await second.shutdown();
	});

	it("updates and removes a query without exposing an incomplete replacement", async () => {
		const { sync, load } = await fixture();
		const events: string[] = [];
		sync.setHooks({
			onQueryAdded: (added) => void events.push(`added:${added.title}`),
			onQueryRemoved: (removed) => void events.push(`removed:${removed.title}`),
		});
		await load(query(45));
		const updated = query(45);
		updated.title = "updated query";
		updated.process.holdPeriod = "weekly";
		await load(updated);

		assert.equal(sync.getQuery(45)?.title, "updated query");
		assert.deepEqual(events, ["added:query 45", "removed:query 45", "added:updated query"]);
		await load();
		assert.equal(sync.getQuery(45), undefined);
		assert.deepEqual(events, ["added:query 45", "removed:query 45", "added:updated query", "removed:updated query"]);
		await sync.shutdown();
	});

	it("rejects shutdown immediately while an independently observed hook is active", async () => {
		const { sync, load } = await fixture();
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

		const addition = load(query(46));
		await started;
		await assert.rejects(sync.shutdown(), LifecycleBusyError);
		releaseHook();
		await addition;
		await sync.shutdown();
	});

	it("does not mistake queries with different processing behavior for duplicates", async () => {
		const { sync, load } = await fixture();
		const first = query(1);
		const second = query(2);
		second.process.holdPeriod = "weekly";
		await load(first, second);
		assert.equal(sync.listQueries().length, 2);
		await sync.shutdown();
	});

	it("requires the canonical contract.address for single-contract log queries", async () => {
		const { sync, load } = await fixture();
		const canonicalAddress = "0.0.111";
		const canonical = {
			...query(50),
			type: "Single-Contract-Logs" as const,
			contract: { address: canonicalAddress },
			params: { timestamp: "1" },
		};
		await load(canonical);

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
		await assert.rejects(load(missingCanonicalSource), /contract\.address is required/);
		assert.equal(sync.listQueries().length, 1);
		await sync.shutdown();
	});

	it("requires params.accountId for transaction queries without deriving it from metadata", async () => {
		const { sync, load } = await fixture();
		const common = {
			type: "Transactions" as const,
			read: query().read,
			process: { pauseAfterConsecutiveFailures: 3 },
		};
		await load({
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
		await assert.rejects(load(missingAccountId), /accountId is required/);
		assert.equal(sync.listQueries().length, 1);
		await sync.shutdown();
	});

	it("canonicalizes execution defaults while retaining distinct source IDs", async () => {
		const { sync, load } = await fixture();
		const implicitLogDefaults = query(54);
		const explicitLogDefaults = query(55);
		explicitLogDefaults.params.index = 0;
		explicitLogDefaults.process.skipEventNames = [];
		await load(implicitLogDefaults, explicitLogDefaults);
		assert.equal(sync.listQueries().length, 2);

		const transaction = {
			type: "Transactions" as const,
			id: 56,
			title: "implicit transaction defaults",
			read: query().read,
			params: { accountId: "0.0.111", timestamp: "1" },
			process: { pauseAfterConsecutiveFailures: 3 },
		};
		await load(implicitLogDefaults, explicitLogDefaults, transaction, {
			...transaction,
			id: 57,
			title: "explicit transaction defaults",
			params: { ...transaction.params, index: 0, result: "success" },
			process: { ...transaction.process, skipTransactionTypes: [], unhandledTransactionPolicy: "error" },
		});
		assert.equal(sync.listQueries().length, 4);

		const stored = sync.getQuery(56);
		assert.equal(stored?.type, "Transactions");
		if (stored?.type === "Transactions") {
			assert.equal(stored.params.result, "success");
			assert.equal(stored.process.unhandledTransactionPolicy, "error");
		}
		await sync.shutdown();
	});

	it("releases the guarded singleton when shut down", async () => {
		const first = await HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] });
		await first.shutdown();
		const second = await HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] });
		assert.notEqual(first, second);
		await second.shutdown();
	});

	const controls = ["pauseRead", "resumeRead", "pauseProcessing", "resumeProcessing"] as const;

	it("rejects query controls while shutdown is waiting for a runtime to stop", async () => {
		const { sync, load } = await fixture();
		await load(query(1));
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
			await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down|shutdown is in progress/, control)));
		} finally {
			releaseStop();
			await shutdown;
		}
	});

	it("rejects query controls queued behind shutdown and after shutdown completes", async () => {
		const { sync, load } = await fixture();
		await load(query(1));
		const shutdown = sync.shutdown();
		try {
			await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down|shutdown is in progress/, control)));
		} finally {
			await shutdown;
		}
		assert.equal(sync.getQuery(1), undefined);
		await Promise.all(controls.map((control) => assert.rejects(sync[control](1), /has been shut down|shutdown is in progress/, control)));
	});

	it("still ignores missing query IDs in controls on an active instance", async () => {
		const { sync, load } = await fixture();
		try {
			await Promise.all(controls.map((control) => sync[control](999)));
			assert.equal(sync.listQueries().length, 0);
		} finally {
			await sync.shutdown();
		}
	});

	it("keeps the guarded singleton occupied until asynchronous shutdown settles", async () => {
		const first = await HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] });
		await first.start();
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
		const replacementPromise = HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] }).then((instance) => {
			replacementCreated = true;
			return instance;
		});
		await new Promise<void>((resolve) => setImmediate(resolve));
		assert.equal(replacementCreated, false);

		releaseStop();
		await shutdown;
		const replacement = await replacementPromise;
		assert.notEqual(replacement, first);
		assert.equal(await HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] }), replacement);
		await replacement.shutdown();
	});

	it("rejects guarded create from a callback awaited by singleton shutdown", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] });
		await sync.start();
		let checked = false;
		sync.setHooks({
			async onQueryRemoved() {
				await assert.rejects(
					HederaEventSync.create({ restClient: {} as HederaRestClient, querySource: () => [query(60)] }),
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
			HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => [], hookFailurePolicy: "invalid" as never }),
			/hookFailurePolicy must be one of/
		);
	});

	it("requires a decodable non-anonymous event while accepting mixed ABIs", async () => {
		const { sync, load } = await fixture();
		const anonymousOnly = query(61);
		anonymousOnly.abi = [{ type: "event", name: "Hidden", anonymous: true, inputs: [] }] as const;
		await assert.rejects(load(anonymousOnly), /at least one non-anonymous event/);

		const mixed = query(62);
		mixed.abi = [
			{ type: "event", name: "Hidden", anonymous: true, inputs: [] },
			{ type: "event", name: "Ping", anonymous: false, inputs: [] },
		] as const;
		await load(mixed);
		assert.ok(sync.getQuery(62));
		await sync.shutdown();
	});

	it("rejects only topic-filtered overlaps that cannot move the seven-day window forward", async () => {
		const { sync, load } = await fixture();
		const stalled = query(63);
		stalled.read.consistency = { overlapSeconds: 604_799.999_999_999 };
		await assert.rejects(load(stalled), /usable seven-day topic-query window/);

		const advancing = query(64);
		advancing.read.consistency = { overlapSeconds: 604_799.999_999_998 };


		const unfilteredSingle = {
			...query(65),
			type: "Single-Contract-Logs" as const,
			contract: { address: "0.0.123" },
			params: { timestamp: "1" },
			read: { ...query().read, consistency: { overlapSeconds: 604_800 } },
		};


		const transactions = transactionQuery(66);
		transactions.read.consistency = { overlapSeconds: 604_800 };
		await load(advancing, unfilteredSingle, transactions);
		assert.equal(sync.listQueries().length, 3);
		await sync.shutdown();
	});

	it("validates and canonicalizes transaction result filters at the runtime boundary", async () => {
		const { sync, load } = await fixture();
		const uppercase = transactionQuery(67);
		uppercase.params.result = "SUCCESS";
		await load(uppercase);
		const stored = sync.getQuery(67);
		assert.equal(stored?.type, "Transactions");
		if (stored?.type === "Transactions") assert.equal(stored.params.result, "success");

		const invalid = transactionQuery(68);
		invalid.params.result = "successful" as never;
		await assert.rejects(load(invalid), /result must be success, fail, or null/);
		await sync.shutdown();
	});

	it("signals every query runtime before waiting for any one runtime to stop", async () => {
		const { sync, load } = await fixture();
		await load(query(58), query(59));
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
		const { sync, load } = await fixture({
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
		await load(query(21));

		assert.equal(sync.getReadStatus(21)?.status.name, "PAUSED");
		assert.equal(reportedHook, "onReadInit");
		await sync.shutdown();
	});

	it("stops a query runtime immediately when hookFailurePolicy is fail-fast", async () => {
		const { sync, load } = await fixture({
			restClient: {} as HederaRestClient,
			singletonGuard: false,
			hookFailurePolicy: "fail-fast",
		});
		let failures = 0;
		sync.setHooks({
			onReadInit: async () => { throw new Error("fatal observer failure"); },
			onHookError: () => void failures++,
		});
		await assert.rejects(load(query(22)), /initializ|failed/i);

		assert.equal(failures, 1);
		assert.equal(sync.getReadStatus(22)?.status.name, "FAILED");
		assert.equal(sync.getQueueStatus(22)?.status.name, "FAILED");
		await assert.rejects(sync.resumeRead(22), /failed read job/);
		await sync.shutdown();
	});

	it("summarizes read and processing states independently", async () => {
		const { sync, load } = await fixture();
		await load(query(31));
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
