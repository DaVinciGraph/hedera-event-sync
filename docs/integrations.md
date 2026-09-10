# Connect synchronization to your application

[Overview](../README.md) · [Query recipes](queries.md) · [API reference](reference.md)

The synchronizer reads records and manages their processing. Your application supplies the business effects, storage, notifications, and UI transport. These examples are application code, not built-in adapters or required dependencies.

## Choose the right integration point

| Work | Put it here | Why |
| --- | --- | --- |
| Save a transfer or update business records | `handler.handle()` | Rejecting retries the process and prevents its successful checkpoint advancement |
| Convert decoded arguments into your payload | `handler.normalize()` | Runs during reading; must remain pure or replay-safe |
| Save a restart position | `onProcessedCheckpoint` | Receives the restart-safe cursor, not the potentially-ahead read cursor |
| Update a dashboard or optional history store | Hooks | Observes reading, attempts, steps, outcomes, and statuses |
| Alert an operator about repeated failures | Threshold hooks | Reports actionable failure thresholds instead of every retry |

Do not put required business writes only in `onProcessSucceeded`: the handler has already succeeded, and an observer failure does not retry that handler. Conversely, an unavailable optional history database should not make your business handler fail.

## PostgreSQL: store transfers and resume after restart

This example indexes **ERC-20-style Transfer events from one contract**. It starts a historical backfill on the first run and resumes from a stored checkpoint thereafter. Use a contract whose event matches the ABI; this is not an ERC-721 decoder.

Install these in **your application**, in addition to the synchronizer:

```bash
npm install pg
npm install --save-dev @types/pg
```

The TypeScript snippets below form one application module, for example `postgres-sync.mts`. Use your application's TypeScript/Node tooling. Keep database credentials in environment configuration, and configure verified TLS as required by your deployment.

### 1. Create application tables

Apply this example SQL through your application's migration workflow, not on every poll. The synchronizer does not create or manage these tables.

```sql
CREATE TABLE example_transfers (
  network text NOT NULL,
  consensus_timestamp text NOT NULL,
  log_index bigint NOT NULL CHECK (log_index >= 0),
  contract_address text NOT NULL,
  sender text NOT NULL,
  recipient text NOT NULL,
  amount numeric(78, 0) NOT NULL CHECK (amount >= 0),
  PRIMARY KEY (network, consensus_timestamp, log_index)
);

CREATE TABLE example_sync_checkpoints (
  network text NOT NULL,
  query_key text NOT NULL,
  cursor_timestamp text NOT NULL,
  cursor_index bigint NOT NULL CHECK (cursor_index >= 0),
  PRIMARY KEY (network, query_key)
);
```

The transfer key uses the ledger plus the canonical log identity. It deliberately excludes runtime process IDs, provider names, and transaction hashes. `numeric(78, 0)` preserves an unsigned 256-bit integer without converting it to a JavaScript `number`; token decimals are a separate application concern.

### 2. Define the handler and checkpoint adapter

Values are passed as [PostgreSQL query parameters](https://node-postgres.com/features/queries), not interpolated into SQL.

```ts
import { Pool } from "pg";
import {
  HederaEventSync, HederaRestClient,
  type LogHandler, type ProcessedCheckpoint, type SyncHooks,
} from "@davincigraph/hedera-event-sync";

type TransferArgs = { from: string; to: string; value: bigint };
type Transfer = { from: string; to: string; amount: string };

function postgresIntegration(pool: Pool, network: string, queryKey: string, queryId: number) {
  const handler: LogHandler<Transfer, TransferArgs> = {
    normalize(args) {
      return { from: args.from, to: args.to, amount: args.value.toString() };
    },
    async handle(transfer, context) {
      const result = await pool.query(
        `INSERT INTO example_transfers
           (network, consensus_timestamp, log_index, contract_address, sender, recipient, amount)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (network, consensus_timestamp, log_index) DO NOTHING`,
        [context.network, context.raw.timestamp, context.raw.index, context.raw.address,
         transfer.from, transfer.to, transfer.amount],
      );
      context.addStep?.({
        title: result.rowCount === 1 ? "Transfer saved" : "Transfer already saved",
        type: "success",
      });
      // A database rejection propagates: this attempt must not count as successful.
    },
  };

  async function loadCheckpoint(): Promise<ProcessedCheckpoint | undefined> {
    const result = await pool.query<{ cursor_timestamp: string; cursor_index: string }>(
      `SELECT cursor_timestamp, cursor_index::text AS cursor_index
       FROM example_sync_checkpoints WHERE network = $1 AND query_key = $2`,
      [network, queryKey],
    );
    const row = result.rows[0];
    if (!row) return undefined;
    const index = Number(row.cursor_index);
    if (!/^\d+\.\d{9}$/.test(row.cursor_timestamp) || !Number.isSafeInteger(index) || index < 0) {
      throw new Error("Stored checkpoint is invalid; inspect it before starting synchronization");
    }
    return { timestamp: row.cursor_timestamp, index };
  }

  const hooks: SyncHooks = {
    async onProcessedCheckpoint(query, checkpoint) {
      if (query.id !== queryId || query.read.fetch.network !== network) return;
      await pool.query(
        `INSERT INTO example_sync_checkpoints
           (network, query_key, cursor_timestamp, cursor_index)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (network, query_key) DO UPDATE
         SET cursor_timestamp = EXCLUDED.cursor_timestamp, cursor_index = EXCLUDED.cursor_index`,
        [network, queryKey, checkpoint.timestamp, checkpoint.index],
      );
    },
  };
  return { handler, hooks, loadCheckpoint };
}
```

The single transfer insert is atomic and replay-safe. A crash after that insert but before the checkpoint write causes replay; the unique key makes the repeated insert harmless. A timeout can leave the caller uncertain whether a write committed, so idempotency matters even without a process restart.

For additional business-table updates, perform the deduplication insert and updates in **one transaction**, using one checked-out client. Apply the updates only when the insert is new. Separate `pool.query()` calls do not form a transaction. See [node-postgres transactions](https://node-postgres.com/features/transactions).

### 3. Restore before adding the query

Append this factory to the same module. It owns one engine and one pool; call it once during application startup, not alongside another owner of the guarded singleton.

```ts
export async function startPostgresTransfers(options: {
  databaseUrl: string;
  contractId: string;
  network: "mainnet" | "testnet";
  observers?: readonly SyncHooks[];
}) {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    max: 4,
    connectionTimeoutMillis: 5_000,
    statement_timeout: 10_000,
    query_timeout: 12_000,
  });
  pool.on("error", (error) => console.error("Idle PostgreSQL connection failed", error));
  const queryId = 1;
  const storage = postgresIntegration(
    pool, options.network, `erc20:${options.contractId}:v1`, queryId,
  );
  let sync: HederaEventSync | undefined;
  try {
    const saved = await storage.loadCheckpoint(); // Failure stops startup; never silently reset to zero.
    const engine = await HederaEventSync.create({
      restClient: new HederaRestClient({ defaultProvider: "public", defaultNetwork: options.network }),
      hookFailurePolicy: "continue",
    });
    sync = engine;
    engine.registry.registerLog("Transfer", storage.handler);
    const diagnostics: SyncHooks = {
      onReadFailure: (query, error) => console.error("Read failed", query.id, error),
      onProcessTryFailed: (query, record, attempt, error) =>
        console.error("Attempt failed", query.id, record.sourceKey, attempt.number, error),
      onHookError: (failure) => console.error("Observer failed", failure.hook, failure.error),
    };
    engine.setHooks([diagnostics, storage.hooks, ...(options.observers ?? [])]);
    await engine.addQuery({
      type: "Single-Contract-Logs", id: queryId, title: "Persisted transfers",
      read: {
        fetch: { restProvider: "public", network: options.network },
        pauseAfterConsecutiveFailures: 5,
      },
      contract: { address: options.contractId },
      params: {
        ...(saved ?? { timestamp: "0.000000000", index: 0 }),
        topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
      },
      abi: [{ type: "event", name: "Transfer", inputs: [
        { name: "from", type: "address", indexed: true },
        { name: "to", type: "address", indexed: true },
        { name: "value", type: "uint256", indexed: false },
      ] }],
      process: { pauseAfterConsecutiveFailures: 5 },
    });
    return {
      sync: engine,
      async close() {
        await engine.shutdown(); // Handlers and checkpoint hooks still need the pool.
        await pool.end();
      },
    };
  } catch (error) {
    await sync?.shutdown();
    await pool.end();
    throw error;
  }
}
```

Pass your database URL, real contract ID, and network to `startPostgresTransfers()`. On application shutdown, await the returned `close()` outside package callbacks. The connection/query limits and pool error listener use the documented [client settings](https://node-postgres.com/apis/client) and [pool lifecycle](https://node-postgres.com/apis/pool).

Use one active owner for each checkpoint key. The table is not a distributed lock. Change the logical key when changing the query's meaning; compatible providers on the same network may share it, different ledgers must not. Do not substitute the read cursor for the processed checkpoint.

Here, checkpoint writes are observational: with `"continue"`, failure is reported but synchronization proceeds. The next restart reuses an older checkpoint and may replay more work. Required transfer writes remain in the handler and must succeed. If checkpoint persistence must stop progress on failure, explicitly choose a different hook-failure policy; understand that optional observers share that engine-level policy.

## Optional email alerts

Email can tell an operator that reading or processing reached its failure threshold. It should not normally determine whether a transfer was successfully recorded. Install the following only in an application that wants SMTP alerts:

```bash
npm install nodemailer
npm install --save-dev @types/nodemailer
```

Save this separate factory as `email-alerts.mts`. Supply credentials from application configuration, never from committed literals. The SMTP options follow [Nodemailer's transport documentation](https://nodemailer.com/smtp).

```ts
import nodemailer from "nodemailer";
import type { SyncHooks } from "@davincigraph/hedera-event-sync";

export function createEmailAlerts(options: {
  host: string; port: number; user: string; password: string; from: string; to: string;
}) {
  const transport = nodemailer.createTransport({
    host: options.host, port: options.port,
    secure: options.port === 465,
    requireTLS: true,
    auth: { user: options.user, pass: options.password },
    dnsTimeout: 5_000, connectionTimeout: 5_000,
    greetingTimeout: 5_000, socketTimeout: 10_000,
  });
  async function send(subject: string, text: string): Promise<void> {
    await transport.sendMail({ from: options.from, to: options.to, subject, text });
  }
  const hooks: SyncHooks = {
    async onReadConsecutiveFailuresReached(query, count) {
      await send(`Read failures: query ${query.id}`,
        `${query.title} on ${query.read.fetch.network} reached ${count} consecutive read failures.`);
    },
    async onProcessConsecutiveFailuresReached(query, record, count) {
      await send(`Processing failures: query ${query.id}`,
        `${query.title} on ${query.read.fetch.network}: ${record.sourceKey} failed ${count} times.`);
    },
  };
  return { hooks, close: () => transport.close() };
}
```

Pass the returned `hooks` in the PostgreSQL factory's `observers` array. After the synchronizer finishes shutting down, close the email transport. `setHooks([diagnostics, storage.hooks, alerts.hooks])` combines observers; calling `setHooks(alerts.hooks)` alone would replace storage and diagnostics.

The example uses implicit TLS on port 465 and requires STARTTLS on other ports; configure the port your provider specifies. Sending errors reach `onHookError`; an SMTP acceptance is not proof of inbox delivery. Alerts may repeat after an operator resumes a failing query or after restart, and they are not durably retried by this package.

If an email is a **required business outcome**, write a durable outbox entry in the same database transaction as the transfer. A separate sender can retry delivery without re-running transfer processing. Keep a stable notification key and account for duplicate sends: PostgreSQL and SMTP do not share one atomic transaction. Do not build this guarantee by sending email first and then inserting the transfer.

## Feed a dashboard

Hooks expose activity; your application provides the HTTP/WebSocket/SSE transport and authentication. An initial snapshot and live updates serve different purposes:

| UI information | Package source |
| --- | --- |
| Reading/idle indicator | `onReadCycleStarted` and `onReadCycleCompleted` |
| Current page activity | `onReadPage`; its count is fetched records, not new processed records |
| New queued record | `onProcessEnqueued` |
| Active attempt and incremental steps | `onProcessStarted`, `onStepCreated` |
| Failed attempt / terminal failed process | `onProcessTryFailed` / `onProcessConsecutiveFailuresReached` |
| Successful processing | `onProcessSucceeded` |
| Independent read/queue states and reasons | `onQueryStatusChange` |
| Initial/reconnect snapshot | `getReadStatus`, `getQueueStatus`, `getLastReadCycle`, `getProcessingTimeline`, `getStatistics` |

Use network/query scope plus `sourceKey` to reconcile a process across live updates and history. Distinguish attempt numbers and step order; a failed attempt does not always mean the process has exhausted retries. Do not rely on polling to catch short-lived processing states, and do not slow handlers just to animate a UI.

Serialize only the fields your UI needs. Raw payloads can contain `bigint`, and diagnostics may contain private application data. Retained in-memory counts are not lifetime totals; use your own durable storage for history. Reconcile snapshots with incoming events on reconnect rather than assuming hooks replay past observations.

## Keep optional services from blocking progress

`hookFailurePolicy: "continue"` contains **rejections**; it does not make hooks fire-and-forget or stop awaiting a promise that never settles. Use client-side connection/operation timeouts, server-side database limits, and bounded queues for external services. SMTP connection/idle timeouts bound individual phases, not a guaranteed total delivery deadline.

For high-volume telemetry or slow notifications, enqueue work into an existing bounded worker or durable queue and let another component deliver it. Handle enqueue failures explicitly. Do not launch untracked promises from handlers or hooks: the synchronizer cannot observe their failures, wait for their completion, or safely relate them to checkpoints.

Keep routine history/telemetry optional, keep required business writes in the handler, and reserve stronger hook-failure policies for observers whose failure should actually pause or fail the query. See the [reference](reference.md) for hook ordering, lifecycle controls, and failure policy details.
