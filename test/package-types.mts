import type { ContractLogTopicsFilter, QueryConfig } from "@davincigraph/hedera-event-sync";
import { ArgsLogHandler, HandlerRegistry, registerLogs, type LogHandler, type LogHandlerContext } from "@davincigraph/hedera-event-sync";
import type { ContractLogTopicsFilter as SubpathTopicsFilter } from "@davincigraph/hedera-event-sync/types/config";

// Compile the built public declarations as a consumer with exact optional properties.
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
	params: { topics: filters[1] },
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
