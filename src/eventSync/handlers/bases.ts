import type { LogHandler, LogHandlerContext, TransactionHandler } from "./types";
import type { SyncTransaction } from "../types/mirror";

/** Convenience base for synchronously mapping ABI arguments before handling a log. */
export abstract class ArgsLogHandler<Args, Normalized> implements LogHandler<Normalized, Args> {
	/** Maps ABI-decoded arguments into the process payload retained by the queue. */
	protected abstract readonly mapArgs: (args: Args, ctx: LogHandlerContext) => Normalized;

	/** Executes the application side effect for a normalized event. */
	abstract readonly handle: LogHandler<Normalized>["handle"];

	/** Implements `LogHandler.normalize` by delegating to `mapArgs`. */
	readonly normalize: LogHandler<Normalized, Args>["normalize"] = (raw, ctx) => this.mapArgs(raw, ctx);
}

/** Convenience base for transaction handlers, which do not have a normalization stage. */
export abstract class TxHandler<Name extends SyncTransaction["name"] = SyncTransaction["name"]>
	implements TransactionHandler<Name> {
	/**
	 * Executes the application side effect for a validated transaction.
	 * A function-valued property preserves strict parameter variance so subclasses
	 * cannot accidentally replace the read-only callback input with a mutable one.
	 */
	abstract readonly handle: TransactionHandler<Name>["handle"];
}
