import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type BeforeReplaceContext, type QueryConfig, type TransactionTypes } from "../src/index";

type TransactionQuery = Extract<QueryConfig, { type: "Transactions" }>;

function query(transactionType?: TransactionTypes, id = 1): TransactionQuery {
	return {
		type: "Transactions", id, title: `Transaction query ${id}`,
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 10 },
			pauseAfterConsecutiveFailures: 2,
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
		},
		params: { accountId: "0.0.123", timestamp: "1700000000.123456789", index: 2,
			...(transactionType === undefined ? {} : { transactionType }) },
		process: { pauseAfterConsecutiveFailures: 2, maxQueuedProcesses: 10 },
		initialState: { read: { name: "PAUSED", info: "review" }, process: { name: "PAUSED", info: "review" } },
	};
}

describe("transaction type query configuration", () => {
	it("trims the filter without changing case, narrowing future names, or mutating caller input", async () => {
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => [] });
		try {
			for (const [inputType, expected] of [["  CONTRACTCALL\t", "CONTRACTCALL"], [" cryptotransfer ", "cryptotransfer"], ["\nFutureForkTransaction \t", "FutureForkTransaction"]]) {
				const input = query(inputType);
				input.params.result = "FAIL";
				Object.freeze(input.params);
				Object.freeze(input);
				const normalized = sync.validateQuery(input);
				assert.ok(normalized.type === "Transactions");
				assert.equal(normalized.params.transactionType, expected);
				assert.equal(normalized.params.result, "fail");
				assert.equal(normalized.params.timestamp, "1700000000.123456789");
				assert.equal(normalized.params.index, 2);
				assert.equal(input.params.transactionType, inputType);
				assert.equal(input.params.result, "FAIL");
			}
			assert.deepEqual(sync.listQueries(), []);
		} finally { await sync.shutdown(); }
	});

	it("leaves omitted filters unrestricted regardless of handler registration and skip policy", async () => {
		let catalog = [query()];
		catalog[0].process.skipTransactionTypes = ["TOKENMINT"];
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => catalog });
		sync.registry.registerTransaction("CONTRACTCALL", { handle() {} });
		try {
			const normalized = sync.validateQuery(catalog[0]);
			assert.ok(normalized.type === "Transactions");
			assert.equal(Object.hasOwn(normalized.params, "transactionType"), false);
			await sync.start();
			const explicitUndefined = query();
			Object.assign(explicitUndefined.params, { transactionType: undefined });
			catalog = [explicitUndefined];
			await sync.reloadQueries();
			let current = sync.getQuery(1);
			assert.ok(current?.type === "Transactions");
			assert.equal(current.params.transactionType, undefined);
			catalog = [query()];
			await sync.reloadQueries();
			await sync.restartQueries();
			current = sync.getQuery(1);
			assert.ok(current?.type === "Transactions");
			assert.equal(Object.hasOwn(current.params, "transactionType"), false);
		} finally { await sync.shutdown(); }
	});

	it("rejects invalid JavaScript filters before start or replacement", async () => {
		let catalog: QueryConfig[] = [];
		let replacements = 0;
		const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false,
			querySource: () => catalog, beforeReplace: () => { replacements++; } });
		try {
			for (const invalid of [null, 1, false, [], ["CONTRACTCALL"], {}, "", " \t\n "]) {
				const input = query();
				Object.assign(input.params, { transactionType: invalid });
				assert.throws(() => sync.validateQuery(input), /transactionType/);
				catalog = [input];
				await assert.rejects(sync.start(), /transactionType/);
				assert.equal(sync.getLifecycleState().started, false);
			}
			catalog = [query("CONTRACTCALL")];
			await sync.start();
			const before = sync.getQuery(1);
			const generation = sync.getLifecycleState().generation;
			const replacementCount = replacements;
			const invalid = query("CONTRACTCALL");
			Object.assign(invalid.params, { transactionType: null });
			catalog = [invalid];
			await assert.rejects(sync.reloadQueries(), /transactionType/);
			assert.deepEqual(sync.getQuery(1), before);
			assert.equal(sync.getLifecycleState().generation, generation);
			assert.equal(replacements, replacementCount);
		} finally { await sync.shutdown(); }
	});

	for (const [description, original, replacement] of [
		["adding", undefined, "CONTRACTCALL"],
		["removing", "CONTRACTCALL", undefined],
		["changing", "CONTRACTCALL", "CRYPTOTRANSFER"],
		["changing case in", "CONTRACTCALL", "contractcall"],
	] as const) {
		it(`rejects ${description} a same-ID source filter atomically and accepts a new ID`, async () => {
			let catalog = [query(original), query(undefined, 2)];
			let replacements = 0;
			let removals = 0;
			const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false,
				querySource: () => catalog, beforeReplace: () => { replacements++; } });
			sync.setHooks({ onQueryRemoved: () => { removals++; } });
			try {
				await sync.start();
				await sync.pauseAll({ scope: "read" });
				const accepted = sync.listQueries();
				const state = sync.getLifecycleState();
				const checkpoint = sync.getProcessedCheckpoint(1);
				const replacementCount = replacements;
				catalog = [{ ...query(undefined, 2), title: "Should not be installed" }, query(replacement)];
				await assert.rejects(sync.reloadQueries(), /source identity.*new query ID/);
				assert.deepEqual(sync.listQueries(), accepted);
				const { lastError, ...retainedState } = sync.getLifecycleState();
				assert.match(lastError ?? "", /source identity/);
				assert.deepEqual(retainedState, state);
				assert.deepEqual(sync.getProcessedCheckpoint(1), checkpoint);
				assert.equal(replacements, replacementCount);
				assert.equal(removals, 0);

				const newScope = query(replacement, 3);
				newScope.params.timestamp = "1700000050";
				catalog = [query(undefined, 2), newScope];
				await sync.reloadQueries();
				assert.equal(sync.getQuery(1), undefined);
				const added = sync.getQuery(3);
				assert.ok(added?.type === "Transactions");
				assert.equal(added.params.transactionType, replacement);
				assert.deepEqual(sync.getProcessedCheckpoint(3), { timestamp: "1700000050.000000000", index: 2 });
			} finally { await sync.shutdown(); }
		});
	}

	it("preserves a normalized filter and processed checkpoint through reload and restart", { timeout: 10000 }, async () => {
		let checkpointReady!: () => void;
		const checkpointReached = new Promise<void>(resolve => { checkpointReady = resolve; });
		const builder: any = new Proxy({}, { get: () => () => builder });
		const next = Object.assign(async () => null, { url: () => null });
		let reads = 0;
		const restClient = {
			useProvider() { return this; }, useNetwork() { return this; },
			transactions() { return { list(callback: (value: unknown) => void) {
				callback(builder);
				return { get: async () => ({ transactions: reads++ === 0 ? [{ name: "CONTRACTCALL",
					consensus_timestamp: "1700000001.123456789", transaction_id: "0.0.123-1700000001-123456789",
					transaction_hash: "filtered-transaction", result: "SUCCESS", nonce: 0,
					transfers: [], token_transfers: [], nft_transfers: [] }] : [], next }) };
			} }; },
		} as unknown as HederaRestClient;
		const input = query(" CONTRACTCALL ");
		delete input.initialState;
		let catalog = [input];
		let loads = 0;
		const persisted: BeforeReplaceContext[] = [];
		const sync = await HederaEventSync.create({ restClient, singletonGuard: false,
			querySource: () => { loads++; return catalog; }, beforeReplace: context => { persisted.push(context); } });
		sync.registry.registerTransaction("CONTRACTCALL", { handle() {} });
		sync.setHooks({ onProcessedCheckpoint: () => checkpointReady() });
		try {
			await sync.start();
			await checkpointReached;
			await sync.pauseAll();
			const checkpoint = sync.getProcessedCheckpoint(1)!;
			assert.equal(checkpoint.timestamp, "1700000001.123456788");
			assert.equal(input.params.transactionType, " CONTRACTCALL ");
			input.params.transactionType = "TOKENMINT";
			await sync.restartQueries();
			assert.equal(loads, 1, "restart uses the accepted catalog, not mutated caller objects");
			catalog = [{ ...query("\tCONTRACTCALL\n"), title: "Edited filtered query" }];
			await sync.reloadQueries();
			const current = sync.getQuery(1);
			assert.ok(current?.type === "Transactions");
			assert.equal(current.params.transactionType, "CONTRACTCALL");
			assert.equal(current.title, "Edited filtered query");
			assert.equal(current.params.timestamp, checkpoint.timestamp);
			assert.equal(current.params.index, checkpoint.index);
			assert.deepEqual(sync.getProcessedCheckpoint(1), checkpoint);
			assert.equal(sync.getLifecycleState().globalPaused, true);
			assert.deepEqual(persisted.map(context => context.reason), ["restart", "reload"]);
			for (const context of persisted) {
				const saved = context.checkpoints[0];
				assert.ok(saved.query.type === "Transactions");
				assert.equal(saved.query.params.transactionType, "CONTRACTCALL");
				assert.deepEqual(saved.checkpoint, checkpoint);
			}
		} finally { await sync.shutdown(); }
	});
});
