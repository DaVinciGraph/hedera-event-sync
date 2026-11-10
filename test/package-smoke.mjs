import assert from "node:assert/strict";
import { HederaEventSync, HederaRestClient, HandlerRegistry, LifecycleBusyError } from "@davincigraph/hedera-event-sync";
import { define } from "@davincigraph/hedera-event-sync/types/config";

assert.equal(typeof HederaEventSync, "function");
assert.equal(typeof HandlerRegistry, "function");
assert.equal(typeof HederaRestClient, "function");
assert.equal(typeof LifecycleBusyError, "function");
assert.equal(typeof define, "function");

// Exercise built exports without network requests, not just their names.
let catalog = [{
  id: 1, type: "Transactions", title: "Packaged query",
  params: { accountId: "0.0.98", timestamp: "1700000000.000000000", index: 2, transactionType: " CONTRACTCALL " },
  read: { fetch: { network: "testnet", restProvider: "public" }, pauseAfterConsecutiveFailures: 3 },
  process: { pauseAfterConsecutiveFailures: 3 },
  initialState: { read: { name: "PAUSED", info: "review" }, process: { name: "PAUSED", info: "review" } },
}];
let calls = 0;
const sync = await HederaEventSync.create({
  restClient: new HederaRestClient({ defaultProvider: "public", defaultNetwork: "testnet" }), singletonGuard: false,
  querySource: ({ signal, reason }) => { assert.equal(signal.aborted, false); assert.ok(["start", "reload"].includes(reason)); calls++; return catalog; },
});
assert.equal(calls, 0);
assert.equal(sync.getLifecycleState().started, false);
assert.equal("addQuery" in sync, false);
assert.equal("updateQuery" in sync, false);
assert.equal("removeQuery" in sync, false);
function assertGlobalHolds(read, process, maintenance = false) {
  const state = sync.getLifecycleState();
  assert.equal(state.globalReadPaused, read);
  assert.equal(state.globalProcessPaused, process);
  assert.equal(state.globalPaused, read && process);
  assert.equal(state.maintenanceHeld, maintenance);
}
assertGlobalHolds(false, false);
await sync.start();
assert.equal(sync.getQuery(1)?.title, "Packaged query");
assert.equal(sync.getQuery(1)?.params.transactionType, "CONTRACTCALL");
assert.equal(catalog[0].params.transactionType, " CONTRACTCALL ");
const unfilteredParams = { ...catalog[0].params };
delete unfilteredParams.transactionType;
const unfiltered = sync.validateQuery({ ...catalog[0], params: unfilteredParams });
assert.equal(Object.hasOwn(unfiltered.params, "transactionType"), false);
const futureFilter = sync.validateQuery({ ...catalog[0], params: { ...catalog[0].params, transactionType: " FutureForkTransaction " } });
assert.equal(futureFilter.params.transactionType, "FutureForkTransaction");
assert.throws(() => sync.validateQuery({ ...catalog[0], params: { ...catalog[0].params, transactionType: null } }), /transactionType/);
// Public query-list filters execute against the built package, including combined options.
const filteredPage = sync.listQueries(1, 25, {
  network: "testnet", type: "Transactions", contractType: null,
  readStatus: ["PAUSED", "PAUSING"], processStatus: "PAUSED", search: "  PACKAGED  ",
});
assert.deepEqual(filteredPage.items.map((query) => query.id), [1]);
assert.equal(filteredPage.total, 1);
assert.equal(sync.listQueries(1, 25, { search: "0.0.98" }).total, 1);
assert.equal(sync.listQueries(1, 25, { search: "1" }).total, 1);
assert.equal(sync.listQueries(1, 25, { search: "missing query" }).total, 0);
assert.equal(sync.listQueries(1, 25, { readStatus: "RUNNING", search: "Packaged" }).total, 0);
assert.equal(sync.listQueries(1, 25, { processStatus: "RUNNING", search: "Packaged" }).total, 0);
assert.equal(sync.listQueries(1, 25, { readStatus: [] }).total, 0);
assert.equal(sync.listQueries(1, 25, { processStatus: [] }).total, 0);
assert.equal(sync.listQueries(1, 25, {
  readStatus: undefined, processStatus: undefined, search: "   ",
}).total, 1);
await sync.pauseAll({ scope: "read", timeoutMs: 5_000 });
assertGlobalHolds(true, false);
await sync.restartQueries();
assert.equal(calls, 1);
assert.equal(sync.getQuery(1)?.params.transactionType, "CONTRACTCALL");
assertGlobalHolds(true, false);
catalog = [{ ...catalog[0], title: "Reloaded query" }];
await sync.reloadQueries();
assert.equal(sync.getQuery(1)?.title, "Reloaded query");
assert.equal(sync.listQueries(1, 25, { search: "packaged" }).total, 0);
assert.equal(sync.listQueries(1, 25, { search: "reloaded", readStatus: "PAUSED" }).total, 1);
assert.equal(calls, 2);
assertGlobalHolds(true, false);
const acceptedCatalog = catalog;
const acceptedGeneration = sync.getLifecycleState().generation;
catalog = [{ ...catalog[0], params: { ...catalog[0].params, transactionType: "CRYPTOTRANSFER" } }];
await assert.rejects(sync.reloadQueries(), /source identity.*new query ID/);
assert.equal(sync.getLifecycleState().generation, acceptedGeneration);
assert.equal(sync.getQuery(1)?.params.transactionType, "CONTRACTCALL");
assert.equal(calls, 3);
assertGlobalHolds(true, false);
catalog = acceptedCatalog;
await sync.resumeAll({ scope: "read" });
assertGlobalHolds(false, false);
await sync.pauseAll({ scope: "process" });
assertGlobalHolds(false, true);
await sync.resumeAll({ scope: "process", timeoutMs: 5_000 });
assertGlobalHolds(false, false);
await sync.pauseAll({ scope: "both" });
assertGlobalHolds(true, true);
await sync.resumeAll({ scope: "read" });
assertGlobalHolds(false, true);
await sync.resumeAll({ scope: "both" });
assertGlobalHolds(false, false);
// Keep the no-argument lifecycle calls covered for existing consumers.
await sync.pauseAll();
await sync.restartQueries();
assert.equal(calls, 3);
assertGlobalHolds(true, true);
await sync.resumeAll();
assertGlobalHolds(false, false);
assert.equal(sync.getReadStatus(1)?.status.name, "PAUSED");
assert.equal(sync.getQueueStatus(1)?.status.name, "PAUSED");
assert.deepEqual(sync.getProcessedCheckpoint(1), { timestamp: "1700000000.000000000", index: 2 });
assert.equal(sync.getQuery(1)?.params.transactionType, "CONTRACTCALL");
await sync.shutdown();
assert.equal(sync.getLifecycleState().closed, true);
