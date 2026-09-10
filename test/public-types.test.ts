import type { Abi } from "abitype";
import {
	ArgsLogHandler,
	HandlerRegistry,
	HookBase,
	TxHandler,
	registerLogs,
	registerTxs,
	type ContractLogTopicsFilter,
	type HederaRestClientConfig,
	type HookFailurePolicy,
	type HederaEventSync,
	type HandlerContext,
	type HandlerKey,
	type LogPrepareOptions,
	type LogHandler,
	type NetworkName,
	type Process,
	type ProcessWithoutPayload,
	type ProviderName,
	type QueryConfig,
	type ReadCycleAudit,
	type ReadonlyCallbackValue,
	type RestClientOptions,
	type SyncHooks,
	type SyncContractLog,
	type SyncReadItem,
	type SyncTransaction,
	type TransactionHandler,
} from "../src/index";

const customNetwork: NetworkName = "private-mirror-network";
const restConfig: HederaRestClientConfig = { defaultNetwork: customNetwork };
const facadeOptions: RestClientOptions = { debug: true };
const hookFailurePolicy: HookFailurePolicy = "pause";
const topics: ContractLogTopicsFilter = [undefined, ["0x2", "0x22"] as const, "0x3"];
const abi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const satisfies Abi;

const handler: LogHandler<number> = {
	async normalize() {
		return 1;
	},
	async handle(_value, context) {
		context.addStep?.({ title: "primitive data", data: 0 });
		context.raw.address;
	},
};

const transactionHandler: TransactionHandler = {
	async handle(transaction, context) {
		const transactionName: string = transaction.name;
		transaction.consensus_timestamp;
		context.raw.consensus_timestamp;
		void transactionName;
		// @ts-expect-error Transaction records are recursively read-only handler inputs.
		transaction.name = "CONTRACTCALL";
		// @ts-expect-error Nested transaction arrays are read-only handler inputs.
		transaction.transfers.push({} as never);
	},
};

const standalonePrepareOptions: LogPrepareOptions = {
	abi,
	contractResolver(log) {
		const address: string = log.address;
		// @ts-expect-error Standalone resolver source logs are recursively read-only.
		log.address = "0x0000000000000000000000000000000000000000";
		// @ts-expect-error Nested resolver topic arrays are read-only.
		log.topics.push("0x01");
		return { address };
	},
};

type MutableLogHandlerContext = Omit<HandlerContext<SyncContractLog>, "raw"> & {
	readonly raw: SyncContractLog;
};

const invalidMutableLogHandler: LogHandler = {
	// @ts-expect-error Log normalizers cannot narrow their read-only context to a mutable raw log.
	normalize: (_decoded: unknown, _context: MutableLogHandlerContext) => undefined,
	// @ts-expect-error Log handlers cannot narrow their read-only context to a mutable raw log.
	handle: (_data: unknown, _context: MutableLogHandlerContext) => {},
};

type MutableUnhandledItem = {
	kind: "transaction";
	itemType: string;
	sourceKey: string;
	raw: SyncTransaction;
};

const invalidMutableHooks: SyncHooks = {
	// @ts-expect-error Hook callbacks cannot narrow a read-only transaction to a mutable one.
	onUnhandledItem: (_query, item: MutableUnhandledItem) => {
		item.raw.transfers.push({} as never);
	},
};

const synchronousLogHandler: LogHandler<number> = {
	normalize: () => 1,
	handle: () => {},
};
const synchronousTransactionHandler: TransactionHandler = { handle: () => {} };

const contractCallHandler: TransactionHandler<"CONTRACTCALL"> = {
	handle(transaction) {
		const transactionName: "CONTRACTCALL" = transaction.name;
		void transactionName;
		transaction.consensus_timestamp;
		// @ts-expect-error Name specialization does not invent transaction fields.
		void transaction.requiredAtRuntime;
	},
};

const cryptoTransferHandler: TransactionHandler<"CRYPTOTRANSFER"> = {
	handle(transaction, context) {
		const transactionName: "CRYPTOTRANSFER" = transaction.name;
		const rawTransactionName: "CRYPTOTRANSFER" = context.raw.name;
		void transactionName;
		void rawTransactionName;
	},
};

const customTransactionHandler: TransactionHandler<"FORKCUSTOMTRANSACTION"> = {
	handle(transaction) {
		const transactionName: "FORKCUSTOMTRANSACTION" = transaction.name;
		void transactionName;
	},
};

const stringLogHandler: LogHandler<string> = {
	normalize: () => "normalized",
	handle(value) {
		value.toUpperCase();
	},
};

function assertReadonlyLogContext(context: Parameters<LogHandler["handle"]>[1]): void {
	const address: string = context.raw.address;
	context.addStep?.({ title: address });

	// @ts-expect-error Handler source records are recursively read-only views.
	context.raw.address = "0x0000000000000000000000000000000000000000";
	// @ts-expect-error Nested source-record arrays are read-only views.
	context.raw.topics.push("0x01");
	// @ts-expect-error Resolved contract metadata is read-only in handlers.
	context.contract.address = "0.0.2";
}

function assertReadonlyTransactionContext(context: Parameters<TransactionHandler["handle"]>[1]): void {
	context.raw.transfers.length;
	// @ts-expect-error Nested transaction arrays are read-only views.
	context.raw.transfers.push({ account: "0.0.2", amount: 1 });
}

const readonlyHooks: SyncHooks = {
	onQueryAdded(query) {
		const kind: QueryConfig["type"] = query.type;
		const network: NetworkName = query.read.fetch.network;
		const provider: ProviderName = query.read.fetch.restProvider;
		void kind;
		void network;
		void provider;
		if (query.type === "Multi-Contract-Logs") {
			// Callable configuration values retain their callable API.
			void query.params.contractResolver?.("0x0000000000000000000000000000000000000000");
		}
		// @ts-expect-error Hook query snapshots are recursively read-only.
		query.title = "changed";
		// @ts-expect-error Nested query settings are read-only.
		query.read.fetch.batchSize = 1;
	},
	onReadCycleCompleted(_query, result) {
		result.cursor.timestamp;
		const first: ReadonlyCallbackValue<SyncReadItem> | undefined = result.items[0];
		if (first && "consensus_timestamp" in first) {
			const timestamp: string = first.consensus_timestamp;
			void timestamp;
		} else if (first) {
			const timestamp: string = first.timestamp;
			void timestamp;
		}
		// @ts-expect-error Hook result arrays are read-only.
		result.items.push(null);
		// @ts-expect-error Nested result cursors are read-only.
		result.cursor.index = 1;
	},
	onProcessStarted(_query, process, processTry) {
		process.sourceKey;
		processTry.outcome;
		// @ts-expect-error Process snapshots are recursively read-only.
		process.status = "Failed";
		// @ts-expect-error Nested attempt arrays are read-only.
		processTry.steps.push({} as never);
	},
	onQueryStatusChange(_query, payload) {
		// @ts-expect-error Status payloads are recursively read-only.
		payload.status.name = "PAUSED";
	},
	onUnhandledItem(_query, item) {
		const timestamp: string = item.raw.consensus_timestamp;
		void timestamp;
		// @ts-expect-error Unhandled transaction source records are recursively read-only.
		item.raw.name = "CONTRACTCALL";
		// @ts-expect-error Nested source-record arrays are read-only.
		item.raw.transfers.push({} as never);
	},
};

function assertTypedAuditItems(audit: ReadCycleAudit): void {
	const first: ReadonlyCallbackValue<SyncReadItem> | undefined = audit.items?.[0];
	if (first && "consensus_timestamp" in first) first.consensus_timestamp;
	else if (first) first.timestamp;
}

function assertProcessInspectionTypes(sync: HederaEventSync, includePayload: boolean): void {
	const activeWithoutPayload = sync.getInFlightProcess(1);
	const explicitActiveWithoutPayload = sync.getInFlightProcess(1, { includePayload: false });
	const activeWithPayload = sync.getInFlightProcess(1, { includePayload: true });
	const dynamicActive = sync.getInFlightProcess(1, { includePayload });
	const failedWithoutPayload = sync.getFailedProcess(1);
	const explicitFailedWithoutPayload = sync.getFailedProcess(1, { includePayload: false });
	const failedWithPayload = sync.getFailedProcess(1, { includePayload: true });
	const dynamicFailed = sync.getFailedProcess(1, { includePayload });
	const activeMetadata: ProcessWithoutPayload | undefined = activeWithoutPayload;
	const failedMetadata: ProcessWithoutPayload | undefined = failedWithoutPayload;
	const activeProcess: Process | undefined = activeWithPayload;
	const failedProcess: Process | undefined = failedWithPayload;

	if (activeWithoutPayload) {
		activeWithoutPayload.status;
		// @ts-expect-error Payload fields are absent unless inclusion is explicitly requested.
		activeWithoutPayload.data;
	}
	if (activeWithPayload) activeWithPayload.data;
	if (explicitActiveWithoutPayload) {
		// @ts-expect-error Explicit false has the same payload-free contract as the default.
		explicitActiveWithoutPayload.rawData;
	}
	if (dynamicActive) {
		// @ts-expect-error A dynamic option must be narrowed before payload fields are available.
		dynamicActive.data;
	}
	if (failedWithoutPayload) {
		// @ts-expect-error Raw payload is absent from the default failed-process result.
		failedWithoutPayload.rawData;
	}
	if (failedWithPayload) failedWithPayload.rawData;
	if (explicitFailedWithoutPayload) {
		// @ts-expect-error Explicit false omits failed-process payload fields.
		explicitFailedWithoutPayload.data;
	}
	if (dynamicFailed) {
		// @ts-expect-error A dynamic failed-process result does not promise payload fields.
		dynamicFailed.rawData;
	}

	void activeMetadata;
	void failedMetadata;
	void activeProcess;
	void failedProcess;
}

class SynchronousArgsLogHandler extends ArgsLogHandler<unknown, number> {
	protected readonly mapArgs = (): number => 1;
	readonly handle: LogHandler<number>["handle"] = () => {};
}

class InvalidMutableArgsLogHandler extends ArgsLogHandler<unknown, number> {
	// @ts-expect-error ArgsLogHandler mapping contexts remain recursively read-only.
	protected readonly mapArgs = (_args: unknown, _context: MutableLogHandlerContext): number => 1;
	// @ts-expect-error ArgsLogHandler execution contexts remain recursively read-only.
	readonly handle = (_data: number, _context: MutableLogHandlerContext): void => {};
}

class SynchronousTxHandler extends TxHandler {
	readonly handle: TransactionHandler["handle"] = (_transaction) => {};
}

class InvalidMutableTxHandler extends TxHandler {
	// @ts-expect-error TxHandler subclasses cannot narrow the read-only input to a mutable transaction.
	readonly handle = (_transaction: SyncTransaction): void => {};
}

class SynchronousHookBase extends HookBase {
	readonly onUnhandledItem: NonNullable<SyncHooks["onUnhandledItem"]> = () => {};
}

class InvalidMethodHookBase extends HookBase {
	// @ts-expect-error HookBase callbacks are properties so method bivariance cannot weaken their inputs.
	onUnhandledItem(
		_query: Parameters<NonNullable<SyncHooks["onUnhandledItem"]>>[0],
		_item: MutableUnhandledItem
	): void {}
}

const typedHandlerRegistry = new HandlerRegistry();

type TokenAssociatedArguments = { token: `0x${string}` };
type TokenAssociation = { tokenAddress: string };

// Real consumers implement concrete event arguments as class methods.
class TokenAssociatedHandler implements LogHandler<TokenAssociation, TokenAssociatedArguments> {
	async normalize(raw: TokenAssociatedArguments): Promise<TokenAssociation> {
		return { tokenAddress: raw.token };
	}
	handle(data: TokenAssociation): void { void data.tokenAddress; }
}

class TypedArgsHandler extends ArgsLogHandler<TokenAssociatedArguments, TokenAssociation> {
	protected readonly mapArgs = (args: TokenAssociatedArguments): TokenAssociation => ({ tokenAddress: args.token });
	readonly handle: LogHandler<TokenAssociation>["handle"] = () => {};
}

const typedArgumentsHandler = new TokenAssociatedHandler();
typedHandlerRegistry.registerLog("TokenAssociated", typedArgumentsHandler);
typedHandlerRegistry.registerLog("TypedArgs", new TypedArgsHandler());
typedHandlerRegistry.registerLog("InlineTypedArgs", {
	normalize(raw: TokenAssociatedArguments, context) {
		const rawLog: HandlerContext<SyncContractLog>["raw"] = context.raw;
		void rawLog;
		return raw.token;
	},
	handle(address: string, context) { void address; void context.raw; },
});
const typedArgumentRegistrations = [
	["TokenAssociated", typedArgumentsHandler],
	["Number", handler],
	["TypedArgs", new TypedArgsHandler()],
] as const;
registerLogs(typedHandlerRegistry, typedArgumentRegistrations);
const reusableTypedArgumentRegistrations: readonly (
	| readonly [HandlerKey, LogHandler<TokenAssociation, TokenAssociatedArguments>]
	| readonly [HandlerKey, LogHandler<number>]
)[] = typedArgumentRegistrations;
registerLogs(typedHandlerRegistry, reusableTypedArgumentRegistrations);
// @ts-expect-error A typed normalizer cannot promise to accept arbitrary unknown input.
const invalidUnknownArguments: LogHandler<TokenAssociation> = typedArgumentsHandler;
// @ts-expect-error The typed normalizer requires the ABI's token argument.
void typedArgumentsHandler.normalize({ amount: 1n });
typedHandlerRegistry.registerLog("MismatchedTypedArgs", {
	// @ts-expect-error A specific argument type must not weaken output/handler correlation.
	normalize: (_raw: TokenAssociatedArguments) => 1,
	handle: (_data: string) => {},
});
const mismatchedTypedArgumentRegistrations = [["Mismatched", {
	normalize: (_raw: TokenAssociatedArguments) => 1,
	handle: (_data: string) => {},
}]] as const;
// @ts-expect-error Reusable bulk entries also correlate typed normalization output with the handler.
registerLogs(typedHandlerRegistry, mismatchedTypedArgumentRegistrations);
void invalidUnknownArguments;

// Direct registration retains normalized log data and transaction-name specialization.
typedHandlerRegistry.registerLog("Number", handler);
typedHandlerRegistry.registerLog("Args", new SynchronousArgsLogHandler());
typedHandlerRegistry.registerTransaction("CONTRACTCALL", contractCallHandler);
typedHandlerRegistry.registerTransaction("FORKCUSTOMTRANSACTION", customTransactionHandler);
typedHandlerRegistry.registerTransaction("TOKENBURN", synchronousTransactionHandler);
typedHandlerRegistry.registerTransaction("TOKENMINT", {
	handle(transaction, context) {
		const transactionName: "TOKENMINT" = transaction.name;
		const rawTransactionName: "TOKENMINT" = context.raw.name;
		void transactionName;
		void rawTransactionName;
	},
});
// @ts-expect-error The registration key must match the handler's narrowed transaction name.
typedHandlerRegistry.registerTransaction("CRYPTOTRANSFER", contractCallHandler);

// Bulk helpers infer every tuple independently, including heterogeneous payloads.
registerLogs(typedHandlerRegistry, [
	["Number", handler],
	["String", stringLogHandler],
	["Args", new SynchronousArgsLogHandler()],
]);
registerTxs(typedHandlerRegistry, [
	["CONTRACTCALL", contractCallHandler],
	["CRYPTOTRANSFER", cryptoTransferHandler],
	["TOKENMINT", synchronousTransactionHandler],
	["FORKCUSTOMTRANSACTION", customTransactionHandler],
]);
// @ts-expect-error Bulk registration also correlates each key with its handler's transaction name.
registerTxs(typedHandlerRegistry, [["CONTRACTCALL", cryptoTransferHandler]]);

const reusableLogRegistrations: readonly (
	| readonly [HandlerKey, LogHandler<number>]
	| readonly [HandlerKey, LogHandler<string>]
)[] = [
	["Number", handler],
	["String", stringLogHandler],
];
const homogeneousLogRegistrations: readonly (readonly [HandlerKey, LogHandler<number>])[] = [
	["Number", handler],
	["Args", new SynchronousArgsLogHandler()],
];
const reusableTransactionRegistrations: readonly (
	| readonly ["CONTRACTCALL", TransactionHandler<"CONTRACTCALL">]
	| readonly ["CRYPTOTRANSFER", TransactionHandler<"CRYPTOTRANSFER">]
)[] = [
	["CONTRACTCALL", contractCallHandler],
	["CRYPTOTRANSFER", cryptoTransferHandler],
];
const homogeneousTransactionRegistrations: readonly (readonly ["CONTRACTCALL", TransactionHandler<"CONTRACTCALL">])[] = [
	["CONTRACTCALL", contractCallHandler],
];

registerLogs(typedHandlerRegistry, reusableLogRegistrations);
registerLogs(typedHandlerRegistry, homogeneousLogRegistrations);
registerTxs(typedHandlerRegistry, reusableTransactionRegistrations);
registerTxs(typedHandlerRegistry, homogeneousTransactionRegistrations);

typedHandlerRegistry.registerLog("Mismatched", {
	// @ts-expect-error The normalized value must match what the consumer accepts.
	normalize: () => 1,
	handle: (_value: string) => {},
});
// @ts-expect-error Bulk registration rejects mismatched normalize/handle payloads.
registerLogs(typedHandlerRegistry, [
	["MismatchedBulk", {
		normalize: () => 1,
		handle: (_value: string) => {},
	}],
]);

const invalidReusableLogRegistrations: readonly (readonly [HandlerKey, {
	normalize: () => number;
	handle: (value: string) => void;
}])[] = [["MismatchedReusable", { normalize: () => 1, handle: () => {} }]];
// @ts-expect-error Reusable arrays must also preserve normalize/handle correlation.
registerLogs(typedHandlerRegistry, invalidReusableLogRegistrations);

const invalidReusableTransactionRegistrations: readonly (readonly [string, {
	handle: (value: number) => void;
}])[] = [["INVALID", { handle: () => {} }]];
// @ts-expect-error Transaction handlers must consume a SyncTransaction specialization.
registerTxs(typedHandlerRegistry, invalidReusableTransactionRegistrations);

// @ts-expect-error A specialized log handler cannot be unsafely erased by assignment.
const invalidErasedLogHandler: LogHandler = handler;
// @ts-expect-error A specialized transaction handler cannot handle every transaction.
const invalidErasedTransactionHandler: TransactionHandler = contractCallHandler;

// @ts-expect-error Transaction handlers specialize only by a transaction-name string.
const invalidTransactionHandler: TransactionHandler<{ imaginary: true }> = { async handle() {} };

// @ts-expect-error Handler-specific fields cannot be invented because Mirror Node transactions are not normalized.
const invalidRefinedTransactionHandler: TransactionHandler<SyncTransaction & { requiredAtRuntime: string }> = {
	handle() {},
};

const config: QueryConfig = {
	type: "Multi-Contract-Logs",
	id: 1,
	title: "types",
	read: { fetch: { network: customNetwork, restProvider: "custom-provider" }, pauseAfterConsecutiveFailures: 2 },
	params: { topics, contractResolver: async (address) => ({ address, id: "0.0.1", type: "Example", version: 0 }) },
	abi,
	process: { pauseAfterConsecutiveFailures: 2, skipEventNames: ["Ignored"] as const },
};

const singleTopics: ContractLogTopicsFilter = [undefined, "0x2"];
const singleConfig: QueryConfig = {
	type: "Single-Contract-Logs",
	id: 2,
	title: "single",
	read: { fetch: { network: customNetwork, restProvider: "custom-provider" }, pauseAfterConsecutiveFailures: 2 },
	contract: { address: "0.0.123" },
	params: { topics: singleTopics },
	abi,
	process: { pauseAfterConsecutiveFailures: 2 },
};
const transactionConfig: QueryConfig = {
	type: "Transactions",
	id: 3,
	title: "account transactions",
	read: { fetch: { network: customNetwork, restProvider: "custom-provider" }, pauseAfterConsecutiveFailures: 2 },
	params: { accountId: "0.0.98", timestamp: "1", index: 2 },
	process: { pauseAfterConsecutiveFailures: 2, skipTransactionTypes: ["TOKENMINT"] as const },
};

void handler;
void config;
void facadeOptions;
void restConfig;
void hookFailurePolicy;
void singleConfig;
void transactionConfig;
void transactionHandler;
void standalonePrepareOptions;
void invalidMutableLogHandler;
void invalidMutableHooks;
void synchronousLogHandler;
void synchronousTransactionHandler;
void contractCallHandler;
void cryptoTransferHandler;
void customTransactionHandler;
void stringLogHandler;
void new SynchronousArgsLogHandler();
void InvalidMutableArgsLogHandler;
void new SynchronousTxHandler();
void InvalidMutableTxHandler;
void new SynchronousHookBase();
void InvalidMethodHookBase;
void typedHandlerRegistry;
void reusableLogRegistrations;
void homogeneousLogRegistrations;
void reusableTransactionRegistrations;
void homogeneousTransactionRegistrations;
void invalidReusableLogRegistrations;
void invalidReusableTransactionRegistrations;
void invalidErasedLogHandler;
void invalidErasedTransactionHandler;
void invalidTransactionHandler;
void invalidRefinedTransactionHandler;
void assertReadonlyLogContext;
void assertReadonlyTransactionContext;
void readonlyHooks;
void assertTypedAuditItems;
void assertProcessInspectionTypes;

// @ts-expect-error Mirror Node log endpoints support no more than four topic positions.
const tooManyTopics: ContractLogTopicsFilter = ["1", "2", "3", "4", "5"];
void tooManyTopics;
