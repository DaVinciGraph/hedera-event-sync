import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { HederaRestClient } from "@davincigraph/hedera-rest-client";
import type { ContractLogsPage, RequestOptions, TransactionTypes } from "@davincigraph/hedera-rest-client";
import { RestClient } from "../src/eventSync/rest/RestClient";
import type { ContractLogTopicsFilter } from "../src/eventSync/types/config";

function emptyLogsPage(next: (options?: RequestOptions) => Promise<ContractLogsPage | null>): ContractLogsPage {
	return {
		logs: [],
		links: { next: null },
		next: Object.assign(next, { url: () => null }),
	} as ContractLogsPage;
}

describe("RestClient cancellation", () => {
	it("passes AbortSignal through the initial operation and every next-page request", async () => {
		let initialOptions: RequestOptions | undefined;
		let nextOptions: RequestOptions | undefined;
		const terminal = emptyLogsPage(async () => null);
		const first = emptyLogsPage(async (options) => {
			nextOptions = options;
			return terminal;
		});
		const builder: any = new Proxy({}, { get: () => () => builder });
		const operation = { get: async (options?: RequestOptions) => ((initialOptions = options), first) };
		const client = {
			useProvider: () => ({
				useNetwork: () => ({
					contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), operation) }),
				}),
			}),
		} as unknown as HederaRestClient;

		const rest = new RestClient(client);
		const initialController = new AbortController();
		const nextController = new AbortController();
		const page = await rest.fetchGlobalLogs({
			provider: "custom",
			network: "private-network",
			cursor: { timestamp: "0.000000000", index: 0 },
			batchSize: 10,
			signal: initialController.signal,
		});
		await page.next({ signal: nextController.signal });

		assert.equal(initialOptions?.signal, initialController.signal);
		assert.equal(nextOptions?.signal, nextController.signal);
	});
});

describe("RestClient transaction filters", () => {
	it("omits transactiontype by default and preserves transfers on mixed transaction types", async (t) => {
		const requests: URL[] = [];
		const transactions = ["CRYPTOTRANSFER", "CONTRACTCALL", "TOKENMINT"].map((name, index) => ({
			name,
			consensus_timestamp: `${index + 1}.000000001`,
			transfers: [{ account: "0.0.1", amount: -100 - index }, { account: "0.0.98", amount: 100 + index }],
			token_transfers: [{ token_id: "0.0.2", account: "0.0.1", amount: index + 1 }],
		}));
		t.mock.method(globalThis, "fetch", async (input: string | URL | Request) => {
			requests.push(new URL(input instanceof Request ? input.url : input));
			return Response.json({ transactions, links: { next: null } });
		});
		const rest = new RestClient(new HederaRestClient({ defaultProvider: "custom", provider: { custom: { testnet: { url: "https://mirror.example" } } } }));

		const page = await rest.fetchTransactions({
			provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000", pageSize: 10,
		});

		assert.equal(requests.length, 1);
		assert.equal(requests[0].pathname, "/api/v1/transactions");
		assert.deepEqual([...requests[0].searchParams.entries()].sort(), [
			["account.id", "0.0.1"], ["limit", "10"], ["order", "asc"], ["result", "success"], ["timestamp", "gte:0.000000000"],
		]);
		assert.deepEqual(page.transactions.map(({ name, transfers, token_transfers }) => ({ name, transfers, token_transfers })),
			transactions.map(({ name, transfers, token_transfers }) => ({ name, transfers, token_transfers })));
	});

	for (const transactionType of ["CRYPTOTRANSFER", "CONTRACTCALL", "TOKENMINT"] as const satisfies readonly TransactionTypes[]) {
		it(`serializes explicit ${transactionType} filtering and preserves the provider's next-page query`, async (t) => {
			const requests: URL[] = [];
			const nextLink = `/api/v1/transactions?account.id=0.0.1&timestamp=gt:5.000000000&timestamp=lte:10.000000000&limit=10&order=asc&transactiontype=${transactionType}`;
			t.mock.method(globalThis, "fetch", async (input: string | URL | Request) => {
				requests.push(new URL(input instanceof Request ? input.url : input));
				return Response.json({ transactions: [], links: { next: requests.length === 1 ? nextLink : null } });
			});
			const rest = new RestClient(new HederaRestClient({ defaultProvider: "custom", provider: { custom: { testnet: { url: "https://mirror.example" } } } }));
			const first = await rest.fetchTransactions({
				provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000",
				timestampLte: "10.000000000", pageSize: 10, result: null, transactionType,
			});
			await first.next();

			assert.equal(requests.length, 2);
			assert.equal(requests[0].searchParams.get("transactiontype"), transactionType);
			assert.deepEqual(requests[0].searchParams.getAll("timestamp"), ["gte:0.000000000", "lte:10.000000000"]);
			assert.equal(requests[0].searchParams.get("account.id"), "0.0.1");
			assert.equal(requests[0].searchParams.get("limit"), "10");
			assert.equal(requests[0].searchParams.get("order"), "asc");
			assert.equal(requests[0].searchParams.has("result"), false);
			const expectedNext = new URL(nextLink, "https://mirror.example");
			assert.equal(requests[1].origin + requests[1].pathname, expectedNext.origin + expectedNext.pathname);
			assert.deepEqual([...requests[1].searchParams.entries()], [...expectedNext.searchParams.entries()]);
			assert.equal(first.syncWindowEnd, "10.000000000");
		});
	}

	it("rejects malformed explicit type filters before making a request", async () => {
		const rest = new RestClient({} as HederaRestClient);
		for (const transactionType of [null, "", "   ", [], {}, 42]) {
			await assert.rejects(rest.fetchTransactions({
				provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000", pageSize: 10,
				transactionType: transactionType as TransactionTypes,
			}), /transactionType must be a non-empty string when provided/);
		}
	});

	it("trims explicit type filters without changing their case", async () => {
		const appliedTypes: unknown[] = [];
		const builder: any = new Proxy({}, { get: (_target, property) => (value?: unknown) => {
			if (property === "transactionType") appliedTypes.push(value);
			return builder;
		} });
		const client = {
			useProvider: () => ({ useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => {
				initialize(builder);
				return { get: async () => ({ transactions: [], links: { next: null }, next: Object.assign(async () => null, { url: () => null }) }) };
			} }) }) }),
		} as unknown as HederaRestClient;
		await new RestClient(client).fetchTransactions({
			provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000", pageSize: 10,
			transactionType: "  customType  ",
		});
		assert.deepEqual(appliedTypes, ["customType"]);
	});

	it("forwards signals and propagates request failures with an explicit type filter", async () => {
		const requests: Array<RequestOptions | undefined> = [];
		const initialError = new Error("initial request failed");
		const nextError = new Error("next request failed");
		let failInitial = true;
		const builder: any = new Proxy({}, { get: () => () => builder });
		const page = { transactions: [], links: { next: "next-page" }, next: Object.assign(async (options?: RequestOptions) => {
			requests.push(options);
			throw nextError;
		}, { url: () => "next-page" }) };
		const client = {
			useProvider: () => ({ useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => {
				initialize(builder);
				return { get: async (options?: RequestOptions) => {
					requests.push(options);
					if (failInitial) throw initialError;
					return page;
				} };
			} }) }) }),
		} as unknown as HederaRestClient;
		const rest = new RestClient(client);
		const initialController = new AbortController();
		const nextController = new AbortController();
		const params = {
			provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000",
			pageSize: 10, transactionType: "CONTRACTCALL", signal: initialController.signal,
		} as const;

		await assert.rejects(rest.fetchTransactions(params), (error) => error === initialError);
		failInitial = false;
		const first = await rest.fetchTransactions(params);
		await assert.rejects(first.next({ signal: nextController.signal }), (error) => error === nextError);
		assert.deepEqual(requests.map((options) => options?.signal), [initialController.signal, initialController.signal, nextController.signal]);
	});

	it("keeps successful-only as the default and allows callers to request all results", async () => {
		const appliedResults: unknown[] = [];
		const builder: any = new Proxy(
			{},
			{
				get: (_target, property) => (value?: unknown) => {
					if (property === "result") appliedResults.push(value);
					return builder;
				},
			}
		);
		const page = { transactions: [], links: { next: null }, next: Object.assign(async () => null, { url: () => null }) };
		const operation = { get: async () => page };
		const client = {
			useProvider: () => ({
				useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => (initialize(builder), operation) }) }),
			}),
		} as unknown as HederaRestClient;
		const rest = new RestClient(client);
		const common = { provider: "custom", network: "testnet", accountId: "0.0.1", timestampGte: "0.000000000", pageSize: 10 } as const;

		await rest.fetchTransactions(common);
		await rest.fetchTransactions({ ...common, result: null });
		assert.deepEqual(appliedResults, ["success"]);
	});
});

describe("RestClient cursor windows", () => {
	it("rejects more than four topic positions before issuing a Mirror Node request", async () => {
		const rest = new RestClient({} as HederaRestClient);
		const topics = ["0x1", "0x2", "0x3", "0x4", "0x5"] as unknown as ContractLogTopicsFilter;

		await assert.rejects(
			rest.fetchGlobalLogs({
				provider: "custom",
				network: "testnet",
				topics,
				cursor: { timestamp: "0.000000000", index: 0 },
				batchSize: 100,
			}),
			/at most four topic positions/
		);
		await assert.rejects(
			rest.fetchContractLogs({
				provider: "custom",
				network: "testnet",
				address: "0.0.123",
				topics,
				cursor: { timestamp: "0.000000000", index: 0 },
				batchSize: 100,
			}),
			/at most four topic positions/
		);
	});

	it("uses a timestamp range and never combines timestamp and index comparators", async () => {
		const calls: Array<[string, unknown]> = [];
		const builder: any = new Proxy({}, {
			get: (_target, property) => (value?: unknown) => {
				calls.push([String(property), value]);
				return builder;
			},
		});
		const page = emptyLogsPage(async () => null);
		const operation = { get: async () => page };
		const client = {
			useProvider: () => ({
				useNetwork: () => ({ contracts: () => ({ logs: (initialize: (query: unknown) => unknown) => (initialize(builder), operation) }) }),
			}),
		} as unknown as HederaRestClient;

		await new RestClient(client).fetchContractLogs({
			provider: "custom",
			network: "testnet",
			address: "0.0.123",
			cursor: { timestamp: "20.000000000", index: 9 },
			timestampGte: "10.000000000",
			timestampLte: "30.000000000",
			batchSize: 100,
		});

		assert.ok(calls.some(([name, value]) => name === "greaterThanOrEqualTo" && value === "10.000000000"));
		assert.ok(calls.some(([name, value]) => name === "lessThanOrEqualTo" && value === "30.000000000"));
		assert.equal(calls.some(([name]) => name === "index" || name === "greaterThan"), false);
	});

	it("bounds single-contract topic searches and forwards repeated values for independent positions", async () => {
		const calls: Array<{ name: string; values: unknown[] }> = [];
		const builder: any = new Proxy({}, {
			get: (_target, property) => (...values: unknown[]) => {
				calls.push({ name: String(property), values });
				return builder;
			},
		});
		const operation = { get: async () => emptyLogsPage(async () => null) };
		const client = {
			useProvider: () => ({
				useNetwork: () => ({ contracts: () => ({ logs: (initialize: (query: unknown) => unknown) => (initialize(builder), operation) }) }),
			}),
		} as unknown as HederaRestClient;

		const page = await new RestClient(client).fetchContractLogs({
			provider: "custom",
			network: "testnet",
			address: "0.0.123",
			topics: [undefined, ["0x1", "0x2"]],
			cursor: { timestamp: "0.000000000", index: 0 },
			timestampGte: "100.000000000",
			timestampLte: "999999.000000000",
			batchSize: 100,
		});

		assert.deepEqual(calls.find((call) => call.name === "topic1")?.values, ["0x1", "0x2"]);
		assert.deepEqual(calls.find((call) => call.name === "lessThanOrEqualTo")?.values, ["604899.999999999"]);
		assert.equal(page.syncWindowEnd, "604899.999999999");
	});

	it("rejects explicit ranges whose end precedes their start", async () => {
		const rest = new RestClient({} as HederaRestClient);
		const range = { timestampGte: "20.000000000", timestampLte: "10.000000000" } as const;

		await assert.rejects(
			rest.fetchGlobalLogs({ provider: "custom", network: "testnet", topics: ["0x1"], cursor: { timestamp: "0.0", index: 0 }, batchSize: 1, ...range }),
			/timestamp.*(?:before|earlier).*start/i
		);
		await assert.rejects(
			rest.fetchContractLogs({ provider: "custom", network: "testnet", address: "0.0.1", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1, ...range }),
			/timestamp.*(?:before|earlier).*start/i
		);
		await assert.rejects(
			rest.fetchTransactions({ provider: "custom", network: "testnet", accountId: "0.0.1", pageSize: 1, ...range }),
			/timestamp.*(?:before|earlier).*start/i
		);
	});
});

describe("RestClient response validation", () => {
	it("accepts processable log and transaction rows when optional metadata is absent", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const next = Object.assign(async () => null, { url: () => null });
		const logPage = {
			logs: [{
				address: `0x${"1".repeat(40)}`,
				data: "0x",
				index: 0,
				timestamp: "1.0",
				topics: [],
			}],
			links: { next: null },
			next,
		};
		const transactionPage = {
			transactions: [{
				consensus_timestamp: "1.0",
				name: "CRYPTOTRANSFER",
				nonce: 0,
				nft_transfers: [],
				token_transfers: [],
				transfers: [],
			}],
			links: { next: null },
			next,
		};
		const client = {
			useProvider: () => ({
				useNetwork: () => ({
					contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => logPage }) }),
					transactions: () => ({ list: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => transactionPage }) }),
				}),
			}),
		} as unknown as HederaRestClient;
		const rest = new RestClient(client);

		const logs = await rest.fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 });
		const transactions = await rest.fetchTransactions({ provider: "custom", network: "private", accountId: "0.0.1", timestampGte: "0.0", pageSize: 1 });

		assert.equal(logs.logs[0]?.block_number, undefined);
		assert.equal(logs.logs[0]?.transaction_hash, undefined);
		assert.equal(transactions.transactions[0]?.transaction_hash, undefined);
		assert.equal(transactions.transactions[0]?.transaction_id, undefined);
	});

	it("canonicalizes EVM hexadecimal log fields without changing their meaning", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const next = Object.assign(async () => null, { url: () => null });
		const logPage = {
			logs: [{
				address: `0x${"Ab".repeat(20)}`,
				block_number: null,
				data: "0xABcd",
				index: 0,
				timestamp: "1.0",
				topics: [`0x${"Ab".repeat(32)}`],
				transaction_hash: `0x${"Cd".repeat(20)}`,
			}],
			links: { next: null },
			next,
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => logPage }) }) }) }),
		} as unknown as HederaRestClient;

		const result = await new RestClient(client).fetchGlobalLogs({
			provider: "custom",
			network: "private",
			cursor: { timestamp: "0.0", index: 0 },
			batchSize: 1,
		});
		const normalized = result.logs[0];

		assert.equal(normalized?.address, `0x${"ab".repeat(20)}`);
		assert.equal(normalized?.block_number, undefined);
		assert.equal(normalized?.data, "0xabcd");
		assert.deepEqual(normalized?.topics, [`0x${"ab".repeat(32)}`]);
		assert.equal(normalized?.transaction_hash, `0x${"cd".repeat(20)}`);
	});

	it("normalizes the Mirror Node empty transaction-hash sentinel to absent metadata", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const next = Object.assign(async () => null, { url: () => null });
		const logPage = {
			logs: [{
				address: `0x${"1".repeat(40)}`,
				block_number: null,
				data: "0x",
				index: 0,
				timestamp: "1.0",
				topics: [],
				transaction_hash: "0x",
			}],
			links: { next: null },
			next,
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => logPage }) }) }) }),
		} as unknown as HederaRestClient;

		const result = await new RestClient(client).fetchGlobalLogs({
			provider: "custom",
			network: "private",
			cursor: { timestamp: "0.0", index: 0 },
			batchSize: 1,
		});

		assert.equal(result.logs[0]?.block_number, undefined);
		assert.equal(result.logs[0]?.transaction_hash, undefined);
	});

	it("rejects optional log and transaction metadata when present but invalid", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const next = Object.assign(async () => null, { url: () => null });
		const logRow = {
			address: `0x${"1".repeat(40)}`,
			block_number: -1,
			data: "0x",
			index: 0,
			timestamp: "1.0",
			topics: [],
			transaction_hash: `0x${"2".repeat(64)}`,
		};
		const logPage = {
			logs: [logRow],
			links: { next: null },
			next,
		};
		const transactionRow = {
			consensus_timestamp: "1.0",
			name: "CRYPTOTRANSFER",
			transaction_hash: "",
			transaction_id: "0.0.1@1.0",
			nonce: 0,
			nft_transfers: [],
			token_transfers: [],
			transfers: [],
		};
		const transactionPage = {
			transactions: [transactionRow],
			links: { next: null },
			next,
		};
		const logClient = {
			useProvider: () => ({ useNetwork: () => ({ contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => logPage }) }) }) }),
		} as unknown as HederaRestClient;
		const transactionClient = {
			useProvider: () => ({ useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => transactionPage }) }) }) }),
		} as unknown as HederaRestClient;

		await assert.rejects(
			new RestClient(logClient).fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 }),
			/invalid block_number/i
		);
		logRow.block_number = 1;
		logRow.transaction_hash = "0x1";
		await assert.rejects(
			new RestClient(logClient).fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 }),
			/invalid transaction_hash/i
		);
		logRow.transaction_hash = "12";
		await assert.rejects(
			new RestClient(logClient).fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 }),
			/invalid transaction_hash/i
		);
		await assert.rejects(
			new RestClient(transactionClient).fetchTransactions({ provider: "custom", network: "private", accountId: "0.0.1", timestampGte: "0.0", pageSize: 1 }),
			/invalid transaction_hash/i
		);
		transactionRow.transaction_hash = "base64-hash";
		transactionRow.transaction_id = "";
		await assert.rejects(
			new RestClient(transactionClient).fetchTransactions({ provider: "custom", network: "private", accountId: "0.0.1", timestampGte: "0.0", pageSize: 1 }),
			/invalid transaction_id/i
		);
	});

	it("rejects malformed log payloads from custom mirrors", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const malformed = {
			logs: [{ address: `0x${"1".repeat(40)}`, block_number: "1", data: 7, index: 0, timestamp: "1.0", topics: [], transaction_hash: `0x${"2".repeat(64)}` }],
			links: { next: null },
			next: Object.assign(async () => null, { url: () => null }),
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => malformed }) }) }) }),
		} as unknown as HederaRestClient;

		await assert.rejects(
			new RestClient(client).fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 }),
			/invalid data/i
		);
	});

	it("rejects incorrectly formatted EVM log fields", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const malformed = {
			logs: [{
				address: "0.0.1",
				block_number: 1,
				data: "0x",
				index: 0,
				timestamp: "1.0",
				topics: [`0x${"1".repeat(64)}`],
				transaction_hash: `0x${"2".repeat(64)}`,
			}],
			links: { next: null },
			next: Object.assign(async () => null, { url: () => null }),
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ contracts: () => ({ globalLogs: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => malformed }) }) }) }),
		} as unknown as HederaRestClient;

		await assert.rejects(
			new RestClient(client).fetchGlobalLogs({ provider: "custom", network: "private", cursor: { timestamp: "0.0", index: 0 }, batchSize: 1 }),
			/invalid address/i
		);
	});

	it("rejects malformed transaction arrays and nonce values", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const malformed = {
			transactions: [{ consensus_timestamp: "1.0", name: "CRYPTOTRANSFER", transaction_hash: "hash", transaction_id: "id", nonce: "1", nft_transfers: [], token_transfers: [], transfers: [] }],
			links: { next: null },
			next: Object.assign(async () => null, { url: () => null }),
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => malformed }) }) }) }),
		} as unknown as HederaRestClient;

		await assert.rejects(
			new RestClient(client).fetchTransactions({ provider: "custom", network: "private", accountId: "0.0.1", timestampGte: "0.0", pageSize: 1 }),
			/invalid nonce/i
		);
	});

	it("rejects NFT serial number zero", async () => {
		const builder: any = new Proxy({}, { get: () => () => builder });
		const malformed = {
			transactions: [{
				consensus_timestamp: "1.0",
				name: "CRYPTOTRANSFER",
				transaction_hash: "base64-hash",
				transaction_id: "0.0.1@1.0",
				nonce: 0,
				nft_transfers: [{ is_approval: false, receiver_account_id: "0.0.2", sender_account_id: "0.0.1", serial_number: 0, token_id: "0.0.3" }],
				token_transfers: [],
				transfers: [],
			}],
			links: { next: null },
			next: Object.assign(async () => null, { url: () => null }),
		};
		const client = {
			useProvider: () => ({ useNetwork: () => ({ transactions: () => ({ list: (initialize: (query: unknown) => unknown) => (initialize(builder), { get: async () => malformed }) }) }) }),
		} as unknown as HederaRestClient;

		await assert.rejects(
			new RestClient(client).fetchTransactions({ provider: "custom", network: "private", accountId: "0.0.1", timestampGte: "0.0", pageSize: 1 }),
			/invalid nft_transfers/i
		);
	});
});
