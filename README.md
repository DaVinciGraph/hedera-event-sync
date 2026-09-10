# @davincigraph/hedera-event-sync

Turn Hedera contract events and transactions into application work you can track, retry, and resume.

## Why this package exists

Suppose your contract emits a `Transfer` event. Your application needs to save it in a database, update an admin dashboard, and notify an operator if processing keeps failing.

Fetching the event from a Mirror Node is only one part of that job. A running application also needs to keep polling, follow pages, remember progress, handle late-indexed records, retry failed work, and explain what it is doing.

Hedera Event Sync coordinates that ongoing work. You tell it **what to read**, **how to handle each record**, and **which lifecycle events to observe**.

Typical uses include:

- Maintaining database records derived from contract events.
- Backfilling historical activity, then continuing to follow new records.
- Watching transactions involving an account or contract.
- Showing administrators what is being read, queued, processed, or retried.
- Sending operational alerts when reading or processing needs attention.

It is a Node.js library that runs **inside your application**, not a hosted service or a separate worker you must deploy. It has no required database, email service, UI framework, or DaVinciGraph application.

## How the pieces fit together

A synchronizer manages one or more **queries**. Each query has its own reader and processing queue:

```text
Mirror Node → read and prepare records → in-memory queue → your handler
                    │                          │                │
                    └──────────────── hooks ────────────────────┘
                                       │
                           your logs, database, UI, alerts
```

| You configure | Its job |
| --- | --- |
| **REST client** | Where requests go: provider, network, authentication, rate limits, and failover |
| **Query** | What to follow: one contract, matching contract-log topics, or an account's transactions; also where to start and when to pause |
| **Handler** | What your application does with one accepted record, such as updating a database |
| **Hooks** | What your application does when synchronization reaches a lifecycle point, such as starting an attempt, adding a step, or reaching a failure threshold |

For contract logs, the reader decodes the event using your ABI and calls the handler's `normalize()` function to prepare its data. The queue then calls `handle()` to perform the application work. Transaction handlers receive the standard Mirror Node transaction record directly; they have no normalization stage.

Each queued record is a **process**. Executing it creates an **attempt**; a failed execution can create another attempt for that same process. A handler can add **steps** to describe its progress. Those steps are diagnostics, not separately executed tasks.

Queries process their queues independently. Within a query, one handler runs at a time; the reader can continue filling the queue while it runs. See the [ordering and replay guarantees](docs/reference.md#delivery-guarantees) for late-indexed records.

### What stays in your application

You own database connections, business logic, notification clients, and deployment. The engine does not store durable history, send email by itself, or submit transactions to Hedera.

The [REST client](https://www.npmjs.com/package/@davincigraph/hedera-rest-client) makes Mirror Node requests. **Event Sync uses that client to continuously turn the responses into managed application work.** It supports public Mirror Nodes and compatible private providers or forks configured through that client.

## Install

Requires Node.js 18+ and ESM. TypeScript is optional; TypeScript consumers need version 5.4+.

```bash
npm install @davincigraph/hedera-event-sync
```

That is the only installation required for the example below. The compatible REST client is already included and re-exported. An application may also declare its own compatible `@davincigraph/hedera-rest-client` 1.x dependency and pass the same configured client to Event Sync.

## Run your first synchronization

This example follows **ERC-20-style Transfer events from one testnet contract**. Supply a real contract ID with the event declaration shown below. For other contracts, use their actual ABI and matching event handlers; this is not an ERC-721 example.

Save as `sync.mjs`:

```js
import { HederaEventSync, HederaRestClient } from "@davincigraph/hedera-event-sync";

const contractId = process.argv[2];
if (!contractId) throw new Error("Pass your testnet contract ID as the first argument");

const restClient = new HederaRestClient({
  defaultProvider: "public",
  defaultNetwork: "testnet",
});
const sync = await HederaEventSync.create({ restClient });

// Hooks observe the engine. Install them before starting a query.
sync.setHooks({
  onReadCycleCompleted(query, result) {
    console.log("Read cycle", query.id, {
      success: result.success,
      eligibleRecords: result.itemCount,
    });
  },
  onReadFailure(query, error) {
    console.error("Read failed", query.id, error);
  },
  onProcessTryFailed(query, record, attempt, error) {
    console.error("Attempt failed", query.id, record.sourceKey, attempt.number, error);
  },
  onHookError(failure) {
    console.error("Observer failed", failure.hook, failure.error);
  },
});

// Handlers perform application work. This first handler simply prints the event.
sync.registry.registerLog("Transfer", {
  normalize(args) {
    return args;
  },
  handle(transfer, context) {
    console.log("Transfer handled", transfer, context.raw.timestamp);
    context.addStep?.({ title: "Transfer printed", type: "success" });
  },
});

const shutdown = () => {
  void sync.shutdown().catch((error) => {
    console.error("Shutdown failed", error);
    process.exitCode = 1;
  });
};
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

// The standard selector for Transfer(address,address,uint256).
const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

await sync.addQuery({
  type: "Single-Contract-Logs",
  id: 1,
  title: "Token transfers",
  contract: { address: contractId },
  abi: [{
    type: "event",
    name: "Transfer",
    anonymous: false,
    inputs: [
      { name: "from", type: "address", indexed: true },
      { name: "to", type: "address", indexed: true },
      { name: "value", type: "uint256", indexed: false },
    ],
  }],
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

console.log("Watching transfers. Press Ctrl+C to stop.");
```

Run it, replacing `0.0.123` with your contract ID:

```bash
node sync.mjs 0.0.123
```

You will see read-cycle messages even when no matching transfers are available. This query starts **from now**, not from the beginning of history. The default reader stays 15 seconds behind the local clock and polls about every 30 seconds, with startup delay and jitter, so immediate event output is not expected.

There is no additional start command: `addQuery()` starts synchronization. Register handlers and install hooks before calling it. `create()` intentionally reuses one process-local singleton by default; later calls do not replace its configuration.

Next, replace the printing handler with your application logic. For typed handlers, other event ABIs, topic filters, and transaction queries, follow [Queries and handlers](docs/queries.md).

## Connect it to your application

The important distinction is **work that must succeed for a record to be processed** versus **observations about that work**.

| What you need | Where it belongs | Why |
| --- | --- | --- |
| Update business data in PostgreSQL, MongoDB, or another store | Handler | A failed write should fail the attempt, remain retryable, and not count as successful processing |
| Keep an operational history of reads, attempts, and steps | Hooks | History storage can be independent of business processing |
| Save a restart position | `onProcessedCheckpoint` hook | It supplies a safe processed cursor, rather than the reader's potentially newer position |
| Show current activity through WebSockets or server-sent events | Read/process/step/status hooks | The UI can receive transitions while work is happening |
| Send an operator email when failures reach a threshold | Failure-threshold hooks | Notify on a condition that needs attention rather than on every retry |
| Record metrics, traces, or send observational webhooks | Hooks matching the event of interest | Your monitoring library receives the lifecycle information it needs |

### Database writes belong in handlers when they are the work

Return or await your database operation from `handle()`. If it throws, the attempt fails and the engine retries according to the query's policy. Keep `normalize()` focused on preparing data: it runs during reading and can run again during overlap replay.

Make writes idempotent. For example, a transfer table can have a unique key using the network, consensus timestamp, and log index. Reprocessing the same record then does not insert another transfer. Your application can use its existing database driver or ORM; Event Sync does not choose one.

The [integration guide](docs/integrations.md) includes an actual PostgreSQL schema and handler, not just an unspecified `saveToDatabase()` placeholder.

### Hooks connect progress to other services

Hooks are callbacks you implement with your own service clients. They can store an audit trail, publish live updates, notify an operator, or combine those purposes.

For example, this TypeScript factory adapts an application's email-sending function into a processing-failure observer:

```ts
import type { SyncHooks } from "@davincigraph/hedera-event-sync";

export function createFailureAlerts(
  sendEmail: (message: { subject: string; text: string }) => Promise<unknown>,
): SyncHooks {
  return {
    async onProcessConsecutiveFailuresReached(query, record, failures) {
      await sendEmail({
        subject: `Processing paused: ${query.title}`,
        text: `Query ${query.id}, record ${record.sourceKey}: ${failures} consecutive failures.`,
      });
    },
    onHookError(failure) {
      console.error("Observer failed", failure.hook, failure.error);
    },
  };
}
```

`sendEmail` is supplied by your application, not by Event Sync. The [integration guide](docs/integrations.md) shows how to connect a real Nodemailer transport, alongside checkpoint storage and live-progress hooks.

You can install several hook objects together with `sync.setHooks([historyHooks, alertHooks, dashboardHooks])`. These names represent your application's observers. `setHooks()` replaces the current set; it does not append.

By default, rejected hooks are reported through `onHookError` and synchronization continues. This is useful when an optional history database or notification service is unavailable. Hooks are still awaited, so give external requests appropriate timeouts: `"continue"` handles a rejection, not a promise that never settles.

A success hook failing does **not** rerun a handler that already succeeded. If an email, webhook, or downstream task is a required business effect, perform it through the handler or write a durable outbox entry with your database changes. Observer hooks alone do not provide guaranteed external delivery.

### Make progress visible

Use `onProcessStarted` to show an active attempt, `onStepCreated` to append steps as they occur, and success/failure hooks to finish that attempt in your UI. `onQueryStatusChange` distinguishes reading from processing.

`context.addStep()` records a diagnostic immediately; it does not execute the named task or print anything by itself. Await the actual work in your handler and add steps where they describe that work. A fast handler may finish between UI polls; hooks expose its transitions without artificial delays.

See the [integration guide](docs/integrations.md) for the UI event mapping and [hook reference](docs/reference.md#hook-reference) for every available callback.

## Backfill and resume after a restart

Choose an initial `params.timestamp` and `params.index` for each query:

- **Live-only:** start at the current timestamp, as in the example.
- **Historical:** start at an earlier cursor. Omitted fields default to timestamp zero and index zero, which starts a backfill.
- **Restart:** restore both fields from your saved processed checkpoint.

The engine's queue, history, and checkpoints live in memory. To resume after a restart, your application loads a saved checkpoint, registers its handlers and hooks, and adds the query again with that cursor.

Persist `onProcessedCheckpoint` or `getProcessedCheckpoint(id)`—**not the read cursor**. Reading can be ahead of handlers. A processed checkpoint also deliberately retains an overlap horizon for late-indexed records, so it may stay unchanged across several successful events.

The [checkpoint recipe](docs/integrations.md) shows saving and loading both fields in PostgreSQL. A checkpoint belongs to the same logical query and network; it is not interchangeable between mainnet and testnet. Restart replay is expected, which is why handlers need idempotent side effects.

## Where to go next

| Goal | Guide |
| --- | --- |
| Define typed log handlers, transaction handlers, or multi-contract topic queries | [Queries and handlers](docs/queries.md) |
| Use a private mirror, credentials, or another network | [Provider configuration](docs/queries.md#providers-and-networks) |
| Persist business data and checkpoints, send email, or drive a dashboard | [Application integrations](docs/integrations.md) |
| Tune polling, capacity, retention, and failure behavior | [Configuration reference](docs/reference.md#configuration-reference) |
| Pause, resume, inspect, replace, or shut down queries | [Runtime management](docs/reference.md#runtime-management) |
| Understand replay, hook ordering, and lifecycle restrictions | [Guarantees and lifecycle reference](docs/reference.md) |
| Investigate missing output or paused processing | [Troubleshooting](docs/reference.md#troubleshooting) |

No settings need to be changed merely to enable third-party integrations. The handlers and hooks are the integration points; your application supplies the services behind them.

## License

MIT
