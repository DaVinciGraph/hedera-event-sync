import type { ContractLogTopicsFilter, QueryConfig, ListQueriesOptions, Page, RuntimeQueryConfig, GlobalPauseScope, GlobalPauseOptions, LifecycleState, TransactionsParams, TransactionTypes, SyncHooks } from "@davincigraph/hedera-event-sync";
import { HederaEventSync, ArgsLogHandler, HandlerRegistry, registerLogs, type LogHandler, type LogHandlerContext } from "@davincigraph/hedera-event-sync";
import type { ContractLogTopicsFilter as SubpathTopicsFilter, TransactionsParams as SubpathTransactionsParams, TransactionTypes as SubpathTransactionTypes } from "@davincigraph/hedera-event-sync/types/config";
import type { FailureRecoveryOptions, HederaEventSyncOptions, QueryStatus } from "@davincigraph/hedera-event-sync";

const recoveryPolicies: FailureRecoveryOptions[] = [{ enabled: false }, { enabled: true }, { enabled: true, delaySeconds: 3600 }];
const recoveryStatus: QueryStatus = { name: "PAUSED", failureRecovery: { retryAt: Date.now() + 3600000 } };
function configureRecovery(options: HederaEventSyncOptions) {
	for (const failureRecovery of recoveryPolicies) void HederaEventSync.create({ ...options, failureRecovery });
}
// @ts-expect-error Enabling automatic recovery requires an explicit boolean.
const invalidRecovery: FailureRecoveryOptions = { enabled: "true" };
void configureRecovery; void recoveryStatus; void invalidRecovery;

const knownTransactionType: TransactionTypes = "CONTRACTCALL";
const futureTransactionType: SubpathTransactionTypes = "FutureForkTransaction";
const transactionFilters: readonly TransactionsParams[] = [
	{ accountId: "0.0.98" },
	{ accountId: "0.0.98", transactionType: knownTransactionType },
	{ accountId: "0.0.98", transactionType: futureTransactionType },
];
const subpathTransactionFilters: readonly SubpathTransactionsParams[] = transactionFilters;
const filteredTransactionQuery: QueryConfig = {
	type: "Transactions", id: 2, title: "Filtered account transactions",
	params: transactionFilters[1],
	read: { fetch: { network: "testnet", restProvider: "public" }, pauseAfterConsecutiveFailures: 3 },
	process: { pauseAfterConsecutiveFailures: 3 },
};
const transactionHooks: SyncHooks = {
	onQueryAdded(query) {
		if (query.type !== "Transactions") return;
		const filter: TransactionTypes | undefined = query.params.transactionType;
		void filter;
		// @ts-expect-error Published callback query filters are read-only.
		query.params.transactionType = "TOKENMINT";
	},
};
// @ts-expect-error Only one upstream transaction type can be requested.
const arrayTransactionType: TransactionsParams = { accountId: "0.0.98", transactionType: ["CONTRACTCALL"] };
// @ts-expect-error Null is not an omitted transaction filter.
const nullTransactionType: SubpathTransactionsParams = { accountId: "0.0.98", transactionType: null };
// @ts-expect-error Numbers are not transaction type names.
const numericTransactionType: TransactionsParams = { accountId: "0.0.98", transactionType: 1 };
void subpathTransactionFilters;
void filteredTransactionQuery;
void transactionHooks;
void arrayTransactionType;
void nullTransactionType;
void numericTransactionType;

// Compile the built public declarations as a consumer with exact optional properties.
const listOptions: readonly ListQueriesOptions[] = [
	{}, { contractType: undefined }, { contractType: null }, { contractType: "STATION" },
	{ contractType: "LAUNCH", network: "testnet", type: "Single-Contract-Logs", order: "id-desc" },
	{ readStatus: undefined, processStatus: undefined, search: undefined },
	{ readStatus: "PAUSED", processStatus: "RUNNING", search: "station" },
	{ readStatus: ["PAUSED", "PAUSING"] as const, processStatus: ["RUNNING", "FAILED"] as const },
	{ readStatus: [] as const, processStatus: [] as const, search: "" },
];
function inspectContractQueries(sync: HederaEventSync) {
	for (const options of listOptions) {
		const page: Page<RuntimeQueryConfig> = sync.listQueries(1, 25, options);
		void page;
	}
	const all: RuntimeQueryConfig[] = sync.listQueries();
	void all;
	// @ts-expect-error Contract type filters accept strings or null, not numbers.
	sync.listQueries(1, 25, { contractType: 1 });
	// @ts-expect-error Reader statuses use exact QueryStatus names.
	sync.listQueries(1, 25, { readStatus: "paused" });
	// @ts-expect-error Every processor status in an array must be valid.
	sync.listQueries(1, 25, { processStatus: ["PAUSED", "UNKNOWN"] as const });
	// @ts-expect-error Reader status filters do not accept numeric values.
	sync.listQueries(1, 25, { readStatus: 1 });
	// @ts-expect-error Processor status filters do not accept null.
	sync.listQueries(1, 25, { processStatus: null });
	// @ts-expect-error Search accepts literal text, not regular expressions.
	sync.listQueries(1, 25, { search: /station/i });
	// @ts-expect-error Query IDs must be converted to text for search.
	sync.listQueries(1, 25, { search: 1 });
}
void inspectContractQueries;

function configureWithoutStarting(sync: HederaEventSync, input: QueryConfig) {
	const releaseRecovery: () => void = sync.holdFailureRecovery();
	releaseRecovery();
	const normalized: RuntimeQueryConfig = sync.validateQuery(input);
	void normalized;
	void sync.start({ timeoutMs: 5_000 });
	void sync.pauseAll();
	void sync.resumeAll();
	const scopes: readonly GlobalPauseScope[] = ["read", "process", "both"];
	for (const scope of scopes) {
		const options: GlobalPauseOptions = { scope, timeoutMs: 5_000 };
		void sync.pauseAll(options);
		void sync.resumeAll(options);
	}
	void sync.pauseAll({ timeoutMs: 5_000 });
	void sync.resumeAll({ timeoutMs: 5_000 });
	// @ts-expect-error Global pause scopes are read, process, or both.
	void sync.pauseAll({ scope: "invalid" });
	// @ts-expect-error Global resume scopes are read, process, or both.
	void sync.resumeAll({ scope: "invalid" });
	void sync.restartQueries();
	void sync.reloadQueries();
	const lifecycle: LifecycleState = sync.getLifecycleState();
	const generation: number = lifecycle.generation;
	const recoveryRequired: boolean = lifecycle.recoveryRequired;
	const globalReadPaused: boolean = lifecycle.globalReadPaused;
	const globalProcessPaused: boolean = lifecycle.globalProcessPaused;
	const globalPaused: boolean = lifecycle.globalPaused;
	const maintenanceHeld: boolean = lifecycle.maintenanceHeld;
	const operationScope: GlobalPauseScope | undefined = lifecycle.operation?.scope;
	void globalReadPaused;
	void globalProcessPaused;
	void globalPaused;
	void maintenanceHeld;
	void operationScope;
	void recoveryRequired;
	void generation;
	void sync.pauseRead(input.id, { preserveExistingPause: true });
	void sync.pauseProcessing(input.id, { preserveExistingPause: true });
	// @ts-expect-error Initial terminal/transition states are not supported.
	const invalid: QueryConfig = { ...input, initialState: { process: { name: "FAILED" } } };
	void invalid;
	// @ts-expect-error v2 catalog configuration comes only from querySource.
	void sync.addQuery(input);
	// @ts-expect-error v2 reload replaces the complete accepted catalog.
	void sync.updateQuery(input);
	// @ts-expect-error Removing an item means omitting it from the loader result.
	void sync.removeQuery(input.id);
}
void configureWithoutStarting;

const filters = [
	["0x1"],
	[undefined, "0x2"],
	[undefined, undefined, "0x3"],
	[undefined, undefined, undefined, ["0x4", "0x44"]],
	["0x1", undefined, undefined, undefined],
	[, "0x2"],
] as const satisfies readonly ContractLogTopicsFilter[];

const subpathFilters: readonly SubpathTopicsFilter[] = filters;
const query: QueryConfig = {
	type: "Multi-Contract-Logs",
	id: 1,
	title: "Independent topic positions",
	read: {
		fetch: { network: "testnet", restProvider: "public" },
		pauseAfterConsecutiveFailures: 3,
	},
	params: {
		topics: filters[1],
		async logFilter(log, signal) {
			const cancellation: AbortSignal | undefined = signal;
			// @ts-expect-error Published filter declarations preserve read-only topics.
			log.topics.push("0x01");
			return !cancellation?.aborted && log.address.length > 0;
		},
	},
	abi: [{ type: "event", name: "Ping", anonymous: false, inputs: [] }],
	process: { pauseAfterConsecutiveFailures: 3 },
};

// @ts-expect-error There are at most four topic positions.
const tooManyTopics: ContractLogTopicsFilter = ["1", "2", "3", "4", "5"];
// @ts-expect-error Only whole topic positions may be omitted, not values inside alternatives.
const missingAlternative: ContractLogTopicsFilter = [[undefined, "0x1"]];
// @ts-expect-error Topic values are strings, not numbers.
const numericTopic: ContractLogTopicsFilter = [1];
// @ts-expect-error An omitted position is undefined, not null.
const nullTopic: ContractLogTopicsFilter = [null];

void subpathFilters;
void query;
void tooManyTopics;
void missingAlternative;
void numericTopic;
void nullTopic;

// Concrete consumer handlers must work against published declarations, not only source aliases.
type TokenArguments = { token: `0x${string}` };
type NormalizedAssociation = { tokenAddress: string };

class TokenAssociatedHandler implements LogHandler<NormalizedAssociation, TokenArguments> {
	async normalize(raw: TokenArguments, context: LogHandlerContext): Promise<NormalizedAssociation> {
		void context.raw;
		return { tokenAddress: raw.token };
	}
	handle(data: NormalizedAssociation): void { void data.tokenAddress; }
}

class AssociationArgsHandler extends ArgsLogHandler<TokenArguments, NormalizedAssociation> {
	protected readonly mapArgs = (args: TokenArguments): NormalizedAssociation => ({ tokenAddress: args.token });
	readonly handle: LogHandler<NormalizedAssociation>["handle"] = () => {};
}

const registry = new HandlerRegistry();
const tokenHandler = new TokenAssociatedHandler();
registry.registerLog("TokenAssociated", tokenHandler);
registry.registerLog("TokenAssociated@TOKEN", new AssociationArgsHandler());
const entries = [["TokenAssociated", tokenHandler], ["TokenAssociated@TOKEN", new AssociationArgsHandler()]] as const;
registerLogs(registry, entries);

registry.registerLog("Incompatible", {
	// @ts-expect-error Input specialization must not allow incompatible normalized output.
	normalize: (_args: TokenArguments) => 123,
	handle: (_value: string) => {},
});
// @ts-expect-error Specific ABI argument types cannot accept arbitrary input.
const unknownArgumentHandler: LogHandler<NormalizedAssociation> = tokenHandler;
void unknownArgumentHandler;

// Public create options require an authoritative loader, including for empty/static catalogs.
function catalogTypes() {
	void HederaEventSync.create({ restClient: {} as never, querySource: () => [query] });
	void HederaEventSync.create({ restClient: {} as never,
		querySource: async ({ signal, reason }) => {
			const operation: "start" | "reload" = reason;
			void operation; signal.throwIfAborted();
			return [{ ...query, initialState: { read: { name: "PAUSED", info: "review" } } }];
		},
		beforeReplace: async ({ signal, reason, checkpoints }) => {
			const operation: "restart" | "reload" | "shutdown" = reason;
			void operation; signal.throwIfAborted();
			for (const { query: config, checkpoint } of checkpoints) {
				const id: number = config.id;
				const timestamp: string = checkpoint.timestamp;
				void id; void timestamp;
			}
		},
	});
	// @ts-expect-error v2 requires querySource even for a static configuration.
	void HederaEventSync.create({ restClient: {} as never });
	// @ts-expect-error Loader results contain complete query configurations.
	void HederaEventSync.create({ restClient: {} as never, querySource: () => [{ id: 1 }] });
}
void catalogTypes;
