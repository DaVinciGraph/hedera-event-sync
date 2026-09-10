# Queries and handlers

[Back to the README](../README.md) · [Application integrations](integrations.md) · [API reference](reference.md)

A query selects records. A handler performs your application work for each accepted record. Start with the source you need:

| Query type | Source | What you supply |
| --- | --- | --- |
| `Single-Contract-Logs` | One contract's events | Contract identifier, event ABI, registered log handlers |
| `Multi-Contract-Logs` | Matching events across contracts | Topic filters, compatible event ABI, registered log handlers |
| `Transactions` | Transactions involving an account or contract | Account/contract identifier, registered transaction handlers |

The examples below are alternatives for an application that already created `sync` as shown in the README. Install hooks first. Do not add the same query repeatedly: adding starts it, and query IDs must be unique.

## Single-contract events with types

A log handler has two stages:

1. `normalize(args, context)` prepares a payload while the reader ingests the record. It can return a value or a promise. Keep it pure or replay-safe.
2. `handle(data, context)` performs application work when the queue reaches that record. Return or await asynchronous work so the engine can observe success, failure, and cancellation.

This TypeScript example uses the same ERC-20-style event as the README. Its ABI, argument type, topic, and handler are defined together; no helper package is required.

```ts
import type { LogHandler, LogHandlerContext } from "@davincigraph/hedera-event-sync";

export const transferAbi = [{
  type: "event",
  name: "Transfer",
  anonymous: false,
  inputs: [
    { name: "from", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "value", type: "uint256", indexed: false },
  ],
}] as const;
export const transferTopic =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

type TransferArgs = { from: string; to: string; value: bigint };
export type Transfer = { from: string; to: string; amount: string };

export function createTransferHandler(
  writeTransfer: (transfer: Transfer, context: LogHandlerContext) => void | Promise<void>,
): LogHandler<Transfer, TransferArgs> {
  return {
    normalize(args) {
      return { from: args.from, to: args.to, amount: args.value.toString() };
    },
    async handle(transfer, context) {
      context.addStep?.({ title: "Saving transfer" });
      await writeTransfer(transfer, context);
      context.addStep?.({ title: "Transfer saved", type: "success" });
    },
  };
}
```

`writeTransfer` is your application's operation. For an initial test, use a console callback; for a database-backed implementation, use the [PostgreSQL recipe](integrations.md).

The following registration/query uses the preceding definitions and your existing `sync`. Replace the contract ID:

```ts
sync.registry.registerLog("Transfer", createTransferHandler((transfer, context) => {
  console.log("Transfer", transfer, context.raw.timestamp, context.raw.index);
}));

await sync.addQuery({
  type: "Single-Contract-Logs",
  id: 10,
  title: "Typed token transfers",
  contract: { address: "0.0.123", type: "TOKEN", version: 1 },
  abi: transferAbi,
  params: {
    topics: [transferTopic],
    timestamp: `${Math.floor(Date.now() / 1000)}.000000000`,
    index: 0,
  },
  read: {
    fetch: { restProvider: "public", network: "testnet" },
    pauseAfterConsecutiveFailures: 5,
  },
  process: { pauseAfterConsecutiveFailures: 5 },
});
```

`contract.address` is the Mirror Node endpoint identifier. Despite the property name, it accepts a contract ID such as `0.0.123`, not only an EVM hex address.

### Type and data boundaries

`LogHandler<Normalized, DecodedArguments>` pairs the normalizer's output with its handler input. `registerLog()` infers those types from an object or class instance. The registry uses string keys, so it cannot prove that your argument type matches the ABI: keep the ABI and that type aligned.

The second generic parameter defaults to `unknown`, useful when the normalizer validates its own input. The known `Transfer` declaration above decodes its `uint256` to `bigint`. Converting it to a decimal string is an application choice; it does not apply token decimals. Do not convert large values to `number` and lose precision. Use a bigint-aware serializer when serializing raw records or payloads that retain bigint values.

A step marked `"error"` is only a diagnostic. Throw or reject to fail the attempt. A handler can also return synchronous `void`.

## Transactions

Transactions use `params.accountId`, with no ABI and no normalization method. Handlers receive the standard validated Mirror Node transaction shape; registering a transaction name narrows its existing `name` field, not its other fields.

This example intentionally processes only types with registered handlers:

```ts
sync.registry.registerTransaction("CRYPTOTRANSFER", {
  handle(transaction, context) {
    console.log("Crypto transfer", transaction, context.network);
    context.addStep?.({ title: "Transfer observed", type: "success" });
  },
});

await sync.addQuery({
  type: "Transactions",
  id: 12,
  title: "Crypto transfers only",
  params: {
    accountId: "0.0.98", // Replace with your account or contract.
    result: "success",
    timestamp: `${Math.floor(Date.now() / 1000)}.000000000`,
    index: 0,
  },
  read: {
    fetch: { restProvider: "public", network: "testnet" },
    pauseAfterConsecutiveFailures: 5,
  },
  process: {
    pauseAfterConsecutiveFailures: 5,
    unhandledTransactionPolicy: "skip", // Deliberately discard otherwise-unhandled types.
  },
});
```

Registering `CRYPTOTRANSFER` does not filter other transaction types out of the Mirror Node response. For complete processing, leave `unhandledTransactionPolicy` at its default `"error"` and supply the needed handlers. Use `skipTransactionTypes` to exclude only specifically named types.

`params.result` accepts `"success"` (default), `"fail"`, or `null` to remove that filter. Transaction handlers have no `normalize()` method.

## Multiple contracts by topic

Global queries select logs by indexed topic values. The ABI must describe every selected log unambiguously; attaching metadata later does not change which logs were selected or how they decode.

You can supply topics already available from your contract tooling. The optional example below uses `viem` to calculate them. Only applications importing these helpers need to declare that dependency:

```bash
npm install viem@2
```

This is an illustrative application event, not a built-in Hedera event. Replace it and the account addresses with your application's actual values.

```ts
import { encodeAbiParameters, parseAbi, toEventSelector } from "viem";
import type { ContractLogTopicsFilter } from "@davincigraph/hedera-event-sync";

const positionAbi = parseAbi([
  "event PositionUpdated(address indexed owner, uint256 indexed positionId, uint256 amount)",
]);
const positionTopic = toEventSelector(positionAbi[0]);
const ownerA = encodeAbiParameters(
  [{ type: "address" }],
  ["0x1111111111111111111111111111111111111111"],
);
const ownerB = encodeAbiParameters(
  [{ type: "address" }],
  ["0x2222222222222222222222222222222222222222"],
);

sync.registry.registerLog("PositionUpdated", {
  normalize(args) { return args; },
  handle(position) { console.log("Position updated", position); },
});

const topics: ContractLogTopicsFilter = [positionTopic, [ownerA, ownerB]];
await sync.addQuery({
  type: "Multi-Contract-Logs",
  id: 11,
  title: "Positions for selected owners",
  params: {
    topics,
    timestamp: `${Math.floor(Date.now() / 1000)}.000000000`,
    index: 0,
    contractResolver: (address) => ({ address, type: "POSITION", version: 1 }),
  },
  abi: positionAbi,
  read: {
    fetch: { restProvider: "public", network: "testnet" },
    pauseAfterConsecutiveFailures: 5,
  },
  process: { pauseAfterConsecutiveFailures: 5 },
});
```

Topic positions are `[topic0, topic1, topic2, topic3]`. For non-anonymous events, `topic0` is the signature; subsequent positions are the indexed arguments in declaration order. Alternatives within one position are OR-ed, while filters at different positions are AND-ed.

Use `undefined` to leave a position unfiltered. With the preceding event, these filters select position 42 for any owner:

```ts
const positionIdTopic = encodeAbiParameters([{ type: "uint256" }], [42n]);
const anyOwnerTopics: ContractLogTopicsFilter = [positionTopic, undefined, positionIdTopic];
```

These examples encode static `address` and `uint256` values. Indexed dynamic values such as strings have different hashing rules; use ABI-aware tooling for those.

`contractResolver(address)` adds metadata before handler selection. It is **not a filter**: returning `undefined` does not discard the log.

Topic-filtered requests use windows of at most seven days. The overlap must be shorter than the usable window. This applies to every global query and to single-contract queries with topic filters, not to transactions or unfiltered single-contract queries.

For the optional `viem` examples, strict NodeNext type checking works with TypeScript 5.9+ and compatible Node declarations. Older compilers can report `TS1479` in `viem`/`ox` declarations; strict ESNext/Bundler checking also works on TypeScript 5.5. The core examples and the engine's public types do not require those helper imports.

## Starting position

Each query has an exclusive starting cursor: a position already passed, not a record to process again.

- Use the current timestamp with `index: 0` for live-only processing.
- Use an earlier timestamp/index for a historical backfill.
- Restore both fields of the saved **processed checkpoint** after restart.
- Omitted timestamp/index default to `"0.000000000"` and `0`; the `params` object is still required.

Numeric timestamps must be non-negative safe integers representing whole seconds. Use decimal strings for fractional seconds or exact nanosecond precision.

Use the [checkpoint integration](integrations.md) to resume safely. The ingestion cursor exposed by read hooks is not a substitute for a processed checkpoint.

## ABI matching and exclusions

Logs are decoded strictly before explicit exclusions are applied. Unknown signatures, malformed payloads, incompatible topic counts, missing handlers, and normalization failures fail the read cycle; a malformed page is not partially committed.

`process.skipEventNames` excludes known events **after successful decoding**. It cannot turn an unknown event into a silent skip. Without topic filters, describe all selected events in the ABI and either register their handlers or explicitly exclude them.

Do not combine declarations sharing a signature but using incompatible indexed-parameter layouts. For example, ERC-20 and ERC-721 `Transfer` events share a signature topic but have different layouts. A global topic alone cannot distinguish them, and putting both declarations in one ABI is unsupported. Use separately scoped queries with compatible ABIs.

Anonymous Solidity events are unsupported. A log-query ABI must contain at least one non-anonymous event; encountering an anonymous log still fails closed.

## Providers and networks

The REST client owns provider URLs, credentials, network names, rate limits, retries, and failover. The query selects a configured pair through `read.fetch.restProvider` and `read.fetch.network`.

This replaces the README's REST-client construction **before the first `create()` call**:

```ts
import { HederaRestClient } from "@davincigraph/hedera-event-sync";

const mirrorUrl = process.env.MIRROR_BASE_URL;
if (!mirrorUrl) throw new Error("Set MIRROR_BASE_URL to your mainnet Mirror Node URL");
const apiKey = process.env.MIRROR_API_KEY;

const restClient = new HederaRestClient({
  defaultProvider: "custom",
  defaultNetwork: "mainnet",
  provider: {
    custom: {
      // Use your provider's actual header; x-api-key is only an example.
      headers: apiKey ? { "x-api-key": apiKey } : {},
      mainnet: { url: mirrorUrl },
    },
  },
});
```

Pass that client to `HederaEventSync.create({ restClient })`, then use `read.fetch: { restProvider: "custom", network: "mainnet" }`. Select the contract/account and checkpoint for that network, not those from the testnet example.

A URL can be an origin, custom path prefix, or complete `/api/v1` base. It must be absolute HTTP(S), without embedded credentials, query parameters, or fragments. Configure authentication through headers; do not commit credentials.

Providing a `provider` map replaces the built-in map, so include every provider you intend to select. For a custom network name, use `provider.custom.networks.<name>` and the same name in query settings. Distinct ledgers need distinct network names.

Custom quotas are not inferred. Set limits to match the provider plan and enable retries/failover explicitly when wanted. See the [REST client documentation](https://www.npmjs.com/package/@davincigraph/hedera-rest-client) for these options. Event Sync inherits the client's behavior.

Polling does not opt into cached reads, even when the REST client's cache is enabled. The client's normal cache-write policy still applies.
