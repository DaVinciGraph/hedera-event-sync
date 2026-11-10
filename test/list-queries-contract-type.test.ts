import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { HederaEventSync, type ListQueriesOptions, type QueryConfig } from "../src/index";

const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const;
function query(id: number, contractType: string | null | undefined, network = "testnet", type: QueryConfig["type"] = "Multi-Contract-Logs"): QueryConfig {
	const common = { id, title: `Query ${id}`, read: { fetch: { network, restProvider: "public" }, pauseAfterConsecutiveFailures: 3 }, process: { pauseAfterConsecutiveFailures: 3 } };
	// Null metadata can originate from persisted JSON even though typed callers omit it.
	const contract = contractType === undefined ? {} : { type: contractType as string };
	if (type === "Transactions") return { ...common, type, contract, params: { accountId: "0.0.123" } };
	if (type === "Single-Contract-Logs") return { ...common, type, contract: { ...contract, address: "0.0.123" }, abi, params: {} };
	return { ...common, type, contract, abi, params: { topics: ["0x1"] } };
}
const insertion = [30, 10, 20, 40, 50, 60, 70, 80, 90, 100, 110];
async function fixture(t: TestContext) {
	const catalog: QueryConfig[] = [];
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => catalog });
	t.after(() => sync.shutdown());
	const absent = query(50, undefined, "testnet", "Transactions"); delete absent.contract;
	catalog.push(query(30, "STATION"), query(10, "STATION", "mainnet"), query(20, "STATION", "testnet", "Single-Contract-Logs"),
		query(40, "LAUNCH"), absent, query(60, undefined), query(70, "", "mainnet", "Single-Contract-Logs"), query(80, " \t "),
		query(90, null, "testnet", "Transactions"), query(100, "station"), query(110, " STATION "));
	await sync.start();
	return sync;
}
const ids = (items: { id: number }[]) => items.map(item => item.id);

for (const order of ["insertion", "id-asc", "id-desc"] as const) {
	test(`${order}: contract-type pagination counts, boundaries, and reclamping use the filtered set`, async t => {
		const sync = await fixture(t);
		const expected = order === "insertion" ? [30, 10, 20] : order === "id-asc" ? [10, 20, 30] : [30, 20, 10];
		const first = sync.listQueries(1, 2, { contractType: "STATION", order });
		assert.deepEqual({ ...first, items: ids(first.items) }, { items: expected.slice(0, 2), page: 1, pageSize: 2, total: 3, totalPages: 2, hasPrev: false, hasNext: true });
		const last = sync.listQueries(2, 2, { contractType: "STATION", order });
		assert.deepEqual({ ...last, items: ids(last.items) }, { items: expected.slice(2), page: 2, pageSize: 2, total: 3, totalPages: 2, hasPrev: true, hasNext: false });
		assert.deepEqual(sync.listQueries(999, 2, { contractType: "STATION", order }), last);
		const none = sync.listQueries(99, 2, { contractType: "DOES_NOT_EXIST", order });
		assert.deepEqual(none, { items: [], page: 1, pageSize: 2, total: 0, totalPages: 1, hasPrev: false, hasNext: false });
	});
	test(`${order}: contract type combines with network and source kind using AND`, async t => {
		const sync = await fixture(t);
		assert.deepEqual(ids(sync.listQueries(99, 1, { order, contractType: "STATION", network: "testnet", type: "Multi-Contract-Logs" }).items), [30]);
		assert.deepEqual(ids(sync.listQueries(1, 20, { order, contractType: "STATION", network: "mainnet" }).items), [10]);
		assert.equal(sync.listQueries(1, 20, { order, contractType: "STATION", type: "Transactions" }).total, 0);
		const unspecified = sync.listQueries(99, 1, { order, contractType: null, network: "testnet", type: "Transactions" });
		assert.equal(unspecified.total, 2); assert.equal(unspecified.totalPages, 2); assert.equal(unspecified.page, 2);
		assert.deepEqual(ids(unspecified.items), order === "id-desc" ? [50] : [90]);
	});
}

test("null selects unspecified contract metadata while string filters remain exact and case-sensitive", async t => {
	const sync = await fixture(t);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: null }).items), [50, 60, 70, 80, 90]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: "" }).items), [70]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: " \t " }).items), [80]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: "station" }).items), [100]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: " STATION " }).items), [110]);
});

test("omitted and undefined contract filters preserve unfiltered and existing-filter behavior", async t => {
	const sync = await fixture(t);
	assert.deepEqual(ids(sync.listQueries()), insertion);
	assert.deepEqual(ids(sync.listQueries(1, 20).items), insertion);
	for (const options of [{}, { type: "Transactions" }, { network: "mainnet" }, { type: "Multi-Contract-Logs", network: "testnet", order: "id-desc" }] satisfies ListQueriesOptions[]) {
		assert.deepEqual(sync.listQueries(1, 20, { ...options, contractType: undefined }), sync.listQueries(1, 20, options));
	}
	assert.deepEqual(ids(sync.listQueries(1, 20, { type: "Transactions" }).items), [50, 90]);
	assert.deepEqual(ids(sync.listQueries(1, 20, { network: "mainnet" }).items), [10, 70]);
});

test("filtered results are isolated snapshots and inspection does not change query lifecycle state", async t => {
	const sync = await fixture(t);
	const before = sync.listQueries();
	const page = sync.listQueries(1, 20, { contractType: "STATION" });
	page.items[0].title = "Changed snapshot";
	page.items[0].contract!.type = "LAUNCH";
	page.items[0].read.fetch.network = "mainnet";
	page.items.pop();
	assert.deepEqual(sync.listQueries(), before);
	assert.deepEqual(ids(sync.listQueries(1, 20, { contractType: "STATION" }).items), [30, 10, 20]);
	assert.equal(sync.listQueries(1, 20, { contractType: "LAUNCH" }).total, 1);
});

test("contract filters remain empty and clamped for an engine without queries", async t => {
	const sync = await HederaEventSync.create({ restClient: {} as HederaRestClient, singletonGuard: false, querySource: () => [] }); t.after(() => sync.shutdown());
	for (const contractType of [undefined, null, "STATION"]) for (const order of ["insertion", "id-asc", "id-desc"] as const) {
		assert.deepEqual(sync.listQueries(5, 3, { contractType, order }), { items: [], page: 1, pageSize: 3, total: 0, totalPages: 1, hasPrev: false, hasNext: false });
	}
});
