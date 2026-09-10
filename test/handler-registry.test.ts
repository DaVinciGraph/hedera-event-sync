import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	ArgsLogHandler,
	HandlerRegistry,
	registerLogs,
	registerTxs,
	type HandlerKey,
	type LogHandler,
	type LogHandlerContext,
	type SyncTransaction,
	type TransactionHandler,
	type TransactionHandlerContext,
} from "../src/index";

class NumericLogHandler extends ArgsLogHandler<unknown, number> {
	protected readonly mapArgs = (): number => 41;
	readonly handled: number[] = [];
	readonly handle: LogHandler<number>["handle"] = (value) => {
		this.handled.push(value + 1);
	};
}

describe("HandlerRegistry generic registration", () => {
	it("registers concrete decoded arguments without wrapping or losing the handler receiver", async () => {
		type Args = { token: string };
		type Normalized = { tokenAddress: string };
		class TokenAssociatedHandler implements LogHandler<Normalized, Args> {
			readonly seen: string[] = [];
			async normalize(raw: Args): Promise<Normalized> { return { tokenAddress: raw.token }; }
			handle(data: Normalized): void { this.seen.push(data.tokenAddress); }
		}
		const registry = new HandlerRegistry();
		const handler = new TokenAssociatedHandler();
		registerLogs(registry, [["TokenAssociated", handler]] as const);
		const resolved = registry.resolveLog("TokenAssociated")!;
		assert.strictEqual(resolved, handler);
		const normalized = await resolved.normalize({ token: "0x1234" }, {} as LogHandlerContext);
		await resolved.handle(normalized, {} as LogHandlerContext);
		assert.deepEqual(handler.seen, ["0x1234"]);
	});

	it("preserves handler identity and keeps normalized output paired with its handler", async () => {
		const registry = new HandlerRegistry();
		const handler = new NumericLogHandler();

		registry.registerLog("Numeric", handler);
		const resolved = registry.resolveLog("Numeric");

		assert.strictEqual(resolved, handler);
		assert.ok(resolved instanceof NumericLogHandler);
		const normalized = await resolved.normalize({}, {} as LogHandlerContext);
		await resolved.handle(normalized, {} as LogHandlerContext);
		assert.deepEqual(handler.handled, [42]);
	});

	it("preserves specialized transaction handlers through direct and heterogeneous bulk registration", async () => {
		const registry = new HandlerRegistry();
		const seen: string[] = [];
		const specialized: TransactionHandler<"CONTRACTCALL"> = {
			handle: (transaction) => { seen.push(transaction.name); },
		};
		const general: TransactionHandler = { handle: (transaction) => { seen.push(transaction.name); } };
		const numeric = new NumericLogHandler();
		const textual: LogHandler<string> = {
			normalize: () => "text",
			handle: (value) => { assert.equal(value.toUpperCase(), "TEXT"); },
		};
		const reusableLogs: readonly (
			| readonly [HandlerKey, LogHandler<number>]
			| readonly [HandlerKey, LogHandler<string>]
		)[] = [
			["Numeric", numeric],
			["Textual", textual],
		];
		const reusableTransactions: readonly (
			| readonly ["CONTRACTCALL", TransactionHandler<"CONTRACTCALL">]
			| readonly ["CRYPTOTRANSFER", TransactionHandler]
		)[] = [
			["CONTRACTCALL", specialized],
			["CRYPTOTRANSFER", general],
		];

		registry.registerTransaction("CONTRACTCALL", specialized);
		assert.strictEqual(registry.resolveTransaction("CONTRACTCALL"), specialized);

		registerLogs(registry, reusableLogs);
		registerTxs(registry, reusableTransactions);

		assert.strictEqual(registry.resolveLog("Numeric"), numeric);
		assert.strictEqual(registry.resolveLog("Textual"), textual);
		assert.strictEqual(registry.resolveTransaction("CONTRACTCALL"), specialized);
		assert.strictEqual(registry.resolveTransaction("CRYPTOTRANSFER"), general);

		await registry.resolveTransaction("CONTRACTCALL")!.handle(
			{ name: "CONTRACTCALL" } as SyncTransaction & { readonly name: "CONTRACTCALL" },
			{} as TransactionHandlerContext<"CONTRACTCALL">
		);
		assert.deepEqual(seen, ["CONTRACTCALL"]);
	});
});
