import type { SyncTransaction } from "../types/mirror";
import type { HandlerKey, LogHandler, TransactionHandler } from "./types";

type ErasedLogHandler = LogHandler;
type ErasedTransactionHandler = TransactionHandler;

/** Prevent a handler parameter from widening the name inferred from its registration key. */
type NotInferred<Value> = [Value][Value extends unknown ? 0 : never];

/**
 * Erase a log handler only after the generic registration boundary has checked
 * its normalize/handle payload relationship. Keeping the original object is
 * intentional: resolution historically preserves identity (including class
 * prototypes), while the unsafe assertion remains confined to private storage.
 */
function eraseLogHandler<Normalized, DecodedArguments>(handler: LogHandler<Normalized, DecodedArguments>): ErasedLogHandler {
	return handler as unknown as ErasedLogHandler;
}

/** Erase a name-specialized transaction handler without replacing its object identity. */
function eraseTransactionHandler<Name extends SyncTransaction["name"]>(handler: TransactionHandler<Name>): ErasedTransactionHandler {
	return handler as unknown as ErasedTransactionHandler;
}

/** Mutable handler registry shared by every query owned by a synchronizer. */
export class HandlerRegistry {
	private logs = new Map<HandlerKey, ErasedLogHandler>();
	private txs = new Map<string, ErasedTransactionHandler>();

	/** Registers or replaces a log handler for an event-selection key. */
	registerLog<Normalized, DecodedArguments = unknown>(key: HandlerKey, handler: LogHandler<Normalized, DecodedArguments>): void {
		this.logs.set(key, eraseLogHandler(handler));
	}

	/** Removes the log handler currently registered for `key`. */
	unregisterLog(key: HandlerKey): void {
		this.logs.delete(key);
	}

	/** Registers or replaces a handler for a Mirror Node transaction type. */
	registerTransaction<const Name extends SyncTransaction["name"]>(
		type: Name,
		handler: TransactionHandler<NotInferred<Name>>
	): void {
		this.txs.set(type, eraseTransactionHandler(handler));
	}

	/** Removes the handler currently registered for a transaction type. */
	unregisterTransaction(type: string): void {
		this.txs.delete(type);
	}

	/**
	 * Resolves the most specific matching log handler: type and version, then
	 * type, then the unqualified event name.
	 */
	resolveLog(eventName: string, contractType?: string, version?: string | number): LogHandler | undefined {
		if (contractType && version !== undefined && version !== null) {
			const h = this.logs.get(`${eventName}@${contractType}:${version}`);
			if (h) return h;
		}
		if (contractType) {
			const h = this.logs.get(`${eventName}@${contractType}`);
			if (h) return h;
		}
		return this.logs.get(eventName);
	}

	/** Resolves a transaction handler by the exact Mirror Node transaction type. */
	resolveTransaction<const Name extends SyncTransaction["name"]>(type: Name): TransactionHandler<Name> | undefined {
		return this.txs.get(type) as TransactionHandler<Name> | undefined;
	}
}
