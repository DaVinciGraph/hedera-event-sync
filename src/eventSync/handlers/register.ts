import type { HandlerKey, LogHandler, TransactionHandler } from "./types";
import type { HandlerRegistry } from "./HandlerRegistry";
import type { SyncTransaction } from "../types/mirror";

type ValidLogRegistration<Entry> = Entry extends readonly [HandlerKey, infer Handler]
	? Handler extends LogHandler<infer _Normalized, infer _DecodedArguments> ? Entry : never
	: never;

type ValidLogRegistrations<Entries extends readonly unknown[]> = {
	[Index in keyof Entries]: ValidLogRegistration<Entries[Index]>;
};

type NotInferred<Value> = [Value][Value extends unknown ? 0 : never];

type ValidTransactionRegistration<Entry> = Entry extends readonly [
	infer Name extends SyncTransaction["name"],
	infer Handler,
]
	? Handler extends TransactionHandler<NotInferred<Name>> ? Entry : never
	: never;

type ValidTransactionRegistrations<Entries extends readonly unknown[]> = {
	[Index in keyof Entries]: ValidTransactionRegistration<Entries[Index]>;
};

/**
 * Registers log-handler entries in declaration order. The tuple overload gives
 * inline literals contextual types per position; the distributive overload also
 * accepts reusable arrays whose element is a union of specialized handlers.
 */
export function registerLogs<const Normalized extends readonly unknown[]>(
	reg: HandlerRegistry,
	entries: readonly [...{ [Index in keyof Normalized]: readonly [HandlerKey, LogHandler<Normalized[Index]>] }]
): void;
export function registerLogs<const Entries extends readonly unknown[]>(
	reg: HandlerRegistry,
	entries: Entries & ValidLogRegistrations<Entries>
): void;
export function registerLogs(reg: HandlerRegistry, entries: readonly unknown[]): void {
	for (const entry of entries) {
		const [key, handler] = entry as readonly [HandlerKey, LogHandler<never>];
		reg.registerLog(key, handler);
	}
}

/**
 * Registers transaction-handler entries in declaration order, including
 * reusable heterogeneous arrays of specialized transaction handlers.
 */
export function registerTxs<const Names extends readonly SyncTransaction["name"][]>(
	reg: HandlerRegistry,
	entries: readonly [...{ [Index in keyof Names]: readonly [Names[Index], TransactionHandler<NotInferred<Names[Index]>>] }]
): void;
export function registerTxs<const Entries extends readonly unknown[]>(
	reg: HandlerRegistry,
	entries: Entries & ValidTransactionRegistrations<Entries>
): void;
export function registerTxs(reg: HandlerRegistry, entries: readonly unknown[]): void {
	for (const entry of entries) {
		const [type, handler] = entry as readonly [SyncTransaction["name"], TransactionHandler];
		reg.registerTransaction(type, handler);
	}
}
