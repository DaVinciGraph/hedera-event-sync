import type { HookFailure, HookFailurePolicy, HookFailureScope, ReadCycleResult, SyncHookName, SyncHooks } from "./SyncHooks";
import { snapshotValue } from "../utils/snapshot";
import { runInUserCallback } from "../utils/userCallback";

type HookQuery = Parameters<NonNullable<SyncHooks["onQueryAdded"]>>[0];

/**
 * Per-event serial observer dispatcher with per-observer snapshots and a
 * contained failure boundary. Distinct events may overlap. Most applications
 * interact with it through `HederaEventSync`.
 */
export class CompositeHooks implements SyncHooks {
	private hooks: readonly SyncHooks[];
	private readonly policy: HookFailurePolicy;
	private readonly onFailure?: (failure: HookFailure) => void;

	constructor(
		hooks: readonly SyncHooks[],
		options: { policy?: HookFailurePolicy; onFailure?: (failure: HookFailure) => void } = {}
	) {
		this.hooks = [...hooks];
		this.policy = options.policy ?? "continue";
		this.onFailure = options.onFailure;
	}

	setHooks(hooks: readonly SyncHooks[]): void {
		this.hooks = [...hooks];
	}

	readonly onQueryAdded: NonNullable<SyncHooks["onQueryAdded"]> = async (query) => {
		await this.dispatch("onQueryAdded", "query", query, (h) => h.onQueryAdded?.(snapshotValue(query)));
	};
	readonly onQueryRemoved: NonNullable<SyncHooks["onQueryRemoved"]> = async (query) => {
		await this.dispatch("onQueryRemoved", "query", query, (h) => h.onQueryRemoved?.(snapshotValue(query)));
	};

	readonly onReadInit: NonNullable<SyncHooks["onReadInit"]> = async (query, poll) => {
		await this.dispatch("onReadInit", "read", query, (h) => h.onReadInit?.(snapshotValue(query), poll));
	};
	readonly onReadCycleStarted: NonNullable<SyncHooks["onReadCycleStarted"]> = async (query, cursor, batchSize) => {
		await this.dispatch("onReadCycleStarted", "read", query, (h) => h.onReadCycleStarted?.(snapshotValue(query), snapshotValue(cursor), batchSize));
	};
	readonly onReadPage: NonNullable<SyncHooks["onReadPage"]> = async (query, meta) => {
		await this.dispatch("onReadPage", "read", query, (h) => h.onReadPage?.(snapshotValue(query), snapshotValue(meta)));
	};
	readonly onReadCycleCompleted: NonNullable<SyncHooks["onReadCycleCompleted"]> = async (query, result: ReadCycleResult) => {
		await this.dispatch("onReadCycleCompleted", "read", query, (h) => h.onReadCycleCompleted?.(snapshotValue(query), snapshotValue(result)));
	};
	readonly onReadFailure: NonNullable<SyncHooks["onReadFailure"]> = async (query, error, consecutiveFailures) => {
		await this.dispatch("onReadFailure", "read", query, (h) => h.onReadFailure?.(snapshotValue(query), error, consecutiveFailures));
	};
	readonly onReadConsecutiveFailuresReached: NonNullable<SyncHooks["onReadConsecutiveFailuresReached"]> = async (query, consecutiveFailures) => {
		await this.dispatch("onReadConsecutiveFailuresReached", "read", query, (h) => h.onReadConsecutiveFailuresReached?.(snapshotValue(query), consecutiveFailures));
	};
	readonly onUnhandledItem: NonNullable<SyncHooks["onUnhandledItem"]> = async (query, item) => {
		await this.dispatch("onUnhandledItem", "read", query, (h) => h.onUnhandledItem?.(snapshotValue(query), snapshotValue(item)));
	};

	readonly onProcessEnqueued: NonNullable<SyncHooks["onProcessEnqueued"]> = async (query, process) => {
		await this.dispatch("onProcessEnqueued", "process", query, (h) => h.onProcessEnqueued?.(snapshotValue(query), snapshotValue(process)));
	};
	readonly onProcessStarted: NonNullable<SyncHooks["onProcessStarted"]> = async (query, process, processTry) => {
		await this.dispatch("onProcessStarted", "process", query, (h) => h.onProcessStarted?.(snapshotValue(query), snapshotValue(process), snapshotValue(processTry)));
	};
	readonly onProcessTryFailed: NonNullable<SyncHooks["onProcessTryFailed"]> = async (query, process, processTry, error) => {
		await this.dispatch("onProcessTryFailed", "process", query, (h) => h.onProcessTryFailed?.(snapshotValue(query), snapshotValue(process), snapshotValue(processTry), error));
	};
	readonly onProcessSucceeded: NonNullable<SyncHooks["onProcessSucceeded"]> = async (query, process) => {
		await this.dispatch("onProcessSucceeded", "process", query, (h) => h.onProcessSucceeded?.(snapshotValue(query), snapshotValue(process)));
	};
	readonly onProcessedCheckpoint: NonNullable<SyncHooks["onProcessedCheckpoint"]> = async (query, cursor, process) => {
		await this.dispatch("onProcessedCheckpoint", "process", query, (h) => h.onProcessedCheckpoint?.(snapshotValue(query), snapshotValue(cursor), snapshotValue(process)));
	};
	readonly onProcessConsecutiveFailuresReached: NonNullable<SyncHooks["onProcessConsecutiveFailuresReached"]> = async (query, process, attempts) => {
		await this.dispatch("onProcessConsecutiveFailuresReached", "process", query, (h) => h.onProcessConsecutiveFailuresReached?.(snapshotValue(query), snapshotValue(process), attempts));
	};
	readonly onStepCreated: NonNullable<SyncHooks["onStepCreated"]> = async (query, process, processTry, step) => {
		await this.dispatch("onStepCreated", "process", query, (h) => h.onStepCreated?.(snapshotValue(query), snapshotValue(process), snapshotValue(processTry), snapshotValue(step)));
	};

	readonly onQueryStatusChange: NonNullable<SyncHooks["onQueryStatusChange"]> = async (query, payload) => {
		await this.dispatch("onQueryStatusChange", payload.kind, query, (h) => h.onQueryStatusChange?.(snapshotValue(query), snapshotValue(payload)));
	};

	private async dispatch(
		hook: SyncHookName,
		scope: HookFailureScope,
		query: HookQuery,
		invoke: (hooks: SyncHooks) => void | Promise<void> | undefined
	): Promise<void> {
		const hooks = this.hooks;
		for (let hookIndex = 0; hookIndex < hooks.length; hookIndex++) {
			try {
				await runInUserCallback(
					{ kind: "hook", name: hook, queryId: query.id },
					() => invoke(hooks[hookIndex]),
					this
				);
			} catch (error) {
				const failure: HookFailure = {
					hook,
					hookIndex,
					policy: this.policy,
					scope,
					query: this.toHookContext(query),
					error,
				};
				try {
					// Runtime control must not depend on diagnostic observers settling.
					// In particular, a blocked onHookError reporter must not postpone a
					// configured pause or fail-fast transition.
					this.onFailure?.(failure);
				} catch {
					// Runtime-control callbacks are part of the same containment boundary.
				}
				await this.reportFailure(failure, hooks);
			}
		}
	}

	private async reportFailure(failure: HookFailure, observers: readonly SyncHooks[]): Promise<void> {
		for (const hooks of observers) {
			try {
				await runInUserCallback(
					{ kind: "hook", name: "onHookError", queryId: failure.query.id },
					() => hooks.onHookError?.(snapshotValue(failure)),
					this
				);
			} catch {
				// The error boundary must never recurse or leak an unhandled rejection.
			}
		}
	}

	private toHookContext(query: HookQuery): HookFailure["query"] {
		return {
			id: query.id,
			title: query.title,
			type: query.type,
			contract: query.contract ? { ...query.contract } : undefined,
		};
	}
}
