/** Raised when a Mirror Node log does not satisfy the supported ABI decoding rules. */
export class DecodeError extends Error {
	constructor(msg: string) {
		super(msg);
		this.name = "DecodeError";
	}
}

/** Raised when a log handler's normalization stage rejects. */
export class NormalizationError extends Error {
	constructor(msg: string) {
		super(msg);
		this.name = "NormalizationError";
	}
}

/** Raised when no suitable handler exists or handler preparation is invalid. */
export class HandlerError extends Error {
	constructor(msg: string) {
		super(msg);
		this.name = "HandlerError";
	}
}

/**
 * Raised when lifecycle control is called from a callback that the same
 * lifecycle operation would need to await.
 */
export class LifecycleReentrancyError extends Error {
	readonly operation: string;
	readonly callback: { kind: string; name: string; queryId: number | null };

	constructor(operation: string, callback: { kind: string; name: string; queryId: number | null }) {
		super(
			`Cannot call ${operation} while ${callback.kind} callback ${callback.name} is active` +
				(callback.queryId === null ? "" : ` for query ${callback.queryId}`) +
				"; schedule the lifecycle operation after the callback settles"
		);
		this.name = "LifecycleReentrancyError";
		this.operation = operation;
		this.callback = { kind: callback.kind, name: callback.name, queryId: callback.queryId };
	}
}

/** Raised when a second lifecycle operation targets a query already changing state. */
export class LifecycleBusyError extends Error {
	readonly operation: string;
	readonly queryId: number;
	readonly activeOperation: string;

	constructor(operation: string, queryId: number, activeOperation: string) {
		super(`Cannot ${operation} query ${queryId} while ${activeOperation} is in progress`);
		this.name = "LifecycleBusyError";
		this.operation = operation;
		this.queryId = queryId;
		this.activeOperation = activeOperation;
	}
}
