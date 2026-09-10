import assert from "node:assert/strict";
import { HederaRestClient, RestClient } from "../dist/index.js";

const client = new HederaRestClient({
  defaultProvider: "public",
  defaultNetwork: "testnet",
});
const rest = new RestClient(client);
const nowSeconds = Math.floor(Date.now() / 1000);
const page = await rest.fetchGlobalLogs({
  provider: "public",
  network: "testnet",
  cursor: { timestamp: `${nowSeconds - 120}.000000000`, index: 0 },
  timestampGte: `${nowSeconds - 120}.000000000`,
  timestampLte: `${nowSeconds - 15}.000000000`,
  batchSize: 1,
});

assert.ok(Array.isArray(page.logs));
assert.equal(typeof page.next, "function");

const transactions = await rest.fetchTransactions({
  provider: "public",
  network: "testnet",
  accountId: "0.0.98",
  timestampGte: `${nowSeconds - 120}.000000000`,
  timestampLte: `${nowSeconds - 15}.000000000`,
  pageSize: 1,
});
assert.ok(Array.isArray(transactions.transactions));
assert.equal(typeof transactions.next, "function");

console.log(JSON.stringify({
  ok: true,
  logs: page.logs.length,
  logNext: page.next.url(),
  transactions: transactions.transactions.length,
  transactionNext: transactions.next.url(),
}));
