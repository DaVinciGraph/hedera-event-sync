import { clearTimeout as clearNodeTimeout, setTimeout as setNodeTimeout } from "node:timers";
import { AsyncMutex } from "../../utils/mutex";
import type { Process, ProcessedCheckpoint, ProcessStep, ProcessStepInputType, ProcessStepType, ProcessTry, ProcessWithoutPayload } from "../../types/domain";
import type { HandlerRegistry } from "../../handlers/HandlerRegistry";
import type { QueryQueueAndJobStatus, QueryStatus, RuntimeQueryConfig } from "../../types/config";
import { HandlerError } from "../../errors";
import { SyncHooks } from "../../hooks/SyncHooks";
import { RestClient } from "../../rest/RestClient";
import { LogEventProcessor } from "../logs/LogEventProcessor";
import type { LogHandler, ProcessHandler, TransactionHandler } from "../../handlers/types";
import { ProcessStore } from "./ProcessStore";
import { nextRetentionDelay } from "../../utils/retention";
import { snapshotProcess, snapshotProcessStep, snapshotProcessTry, snapshotQuery } from "../../utils/snapshot";
import type { SyncContractLog, SyncTransaction } from "../../types/mirror";
import { maxCursor } from "../../utils/comparators";
import {
	formatHederaTimestampNs,
	normalizeHederaTimestamp,
	parseHederaTimestampNs,
	subtractHederaTimestampSeconds,
} from "../../utils/timestamp";
import { associateUserCallbackOwner, runInUserCallback } from "../../utils/userCallback";
import { errorDetails, serializeError } from "../../utils/error";
import { sleepWithSignal } from "../../utils/timer";

function createRevocableCallback<T>(initialTarget: ((value: T) => void) | undefined): {
	callback: (value: T) => void;
	revoke: () => void;
} {
	let target = initialTarget;
	// Returned callbacks retain this helper's isolated environment. Clear the
	// parameter binding now so revocation removes the only reference to target.
	initialTarget = undefined;
	return {
		callback: (value) => target?.(value),
		revoke: () => { target = undefined; },
	};
}

export class ProcessQueue {
	private status: QueryStatus;
	private pauseReason?: string;
	private lastNotifiedStatus: QueryQueueAndJobStatus;
	private q: Process[] = [];
	private readonly mutex = new AsyncMutex();
	private consecutiveFailuresFor = new Map<number, number>();
	private stopped = false;
	private readonly lifecycleController = new AbortController();
	private readonly nextDelayMs: number;
	private readonly holdPeriod: RuntimeQueryConfig["process"]["holdPeriod"];
	private readonly retainedUntil = new Map<number, number>();
	private retentionTimer?: ReturnType<typeof setNodeTimeout>;
	private retentionTimerDeadline?: number;
	private disposed = false;
	private readonly assignedHandlers = new Map<number, ProcessHandler>();
	private inFlight: { procId: number; tryNo: number; startedAt: number } | null = null;
	private readonly maxQueuedProcesses: number;
	private readonly initialProcessedCheckpoint: ProcessedCheckpoint;
	private successfulHighWater: ProcessedCheckpoint;
	private processedCheckpoint: ProcessedCheckpoint;
	private pendingMinimums: { id: number; timestampNs: bigint }[] = [];
	private pendingMinimumHead = 0;
	private stepHookChain: Promise<void> = Promise.resolve();
	private statusHookChain: Promise<void> = Promise.resolve();

	constructor(
		private readonly query: RuntimeQueryConfig,
		private readonly registry: HandlerRegistry,
		private readonly store: ProcessStore,
		private readonly hooks: SyncHooks,
		private readonly rest: RestClient,
		private readonly callbackOwner?: object,
		private readonly getReplayFloor?: () => string | undefined
	) {
		this.status = { ...query.process.status };
		this.lastNotifiedStatus = this.status.name;
		this.holdPeriod = this.query.process.holdPeriod ?? "daily";
		this.nextDelayMs = Math.max(0, this.query.process.nextDelayMs ?? 0);
		this.maxQueuedProcesses = this.query.process.maxQueuedProcesses;
		this.initialProcessedCheckpoint = {
			timestamp: normalizeHederaTimestamp(this.query.params.timestamp ?? 0),
			index: this.query.params.index ?? 0,
		};
		this.successfulHighWater = { ...this.initialProcessedCheckpoint };
		this.processedCheckpoint = { ...this.initialProcessedCheckpoint };
	}

	length(): number {
		return this.occupiedSlots();
	}

	/**
	 * Return a monotonic restart cursor that retains the configured replay window.
	 * It deliberately trails the raw successful high-water mark so a restart can
	 * still discover late records, including unfinished replay pages and queued
	 * records that are older than the current forward-pagination window.
	 */
	getProcessedCheckpoint(): ProcessedCheckpoint {
		// An empty or duplicate-only replay can release its barrier without a new
		// process succeeding. Recompute so inspection can observe that progress.
		this.processedCheckpoint = this.toReplaySafeCheckpoint(this.successfulHighWater);
		return { ...this.processedCheckpoint };
	}

	stop(reason = "Query runtime stopped"): void {
		this.stopped = true;
		this.setStatus("STOPPED", reason);
		this.lifecycleController.abort();
	}

	fail(reason: string): void {
		this.stopped = true;
		this.setStatus("FAILED", reason);
		this.lifecycleController.abort();
	}

	/** Clear timers/queues so GC can reclaim everything once removed */
	dispose(): void {
		this.disposed = true;
		if (this.retentionTimer) clearNodeTimeout(this.retentionTimer);
		this.retentionTimer = undefined;
		this.retentionTimerDeadline = undefined;
		this.retainedUntil.clear();
		this.assignedHandlers.clear();
		this.q = [];
		this.pendingMinimums = [];
		this.pendingMinimumHead = 0;
		this.consecutiveFailuresFor.clear();
		this.inFlight = null;
	}

	async enqueue(proc: Process, handler?: ProcessHandler): Promise<void> {
		const release = await this.mutex.lock();
		try {
			if (this.occupiedSlots() >= this.maxQueuedProcesses) {
				throw new Error(`Process queue capacity ${this.maxQueuedProcesses} exceeded`);
			}
			if (handler) this.assignedHandlers.set(proc.id, handler);
			this.q.push(proc);
			this.trackPendingTimestamp(proc);

			// Keep the item ineligible for processing until its enqueue notification has settled.
			await this.hooks.onProcessEnqueued?.(this.query, proc);
		} finally {
			release();
		}
	}

	async enqueueBatch(items: readonly { process: Process; handler: ProcessHandler }[]): Promise<void> {
		const release = await this.mutex.lock();
		try {
			if (this.occupiedSlots() + items.length > this.maxQueuedProcesses) {
				throw new Error(`Process queue capacity ${this.maxQueuedProcesses} exceeded`);
			}
			for (const item of items) {
				if (item.handler) this.assignedHandlers.set(item.process.id, item.handler);
				this.q.push(item.process);
				this.trackPendingTimestamp(item.process);
			}

			// Preserve batch order and do not expose any item to the processor until all enqueue notifications settle.
			for (const item of items) await this.hooks.onProcessEnqueued?.(this.query, item.process);
		} finally {
			release();
		}
	}

	async waitForCapacity(itemCount: number, signal: AbortSignal): Promise<void> {
		if (itemCount > this.maxQueuedProcesses) {
			throw new Error(`A batch of ${itemCount} processes cannot fit in queue capacity ${this.maxQueuedProcesses}`);
		}
		while (this.occupiedSlots() + itemCount > this.maxQueuedProcesses) {
			this.throwIfCapacityWaitAborted(signal);
			await this.waitForCapacityTick(signal);
		}
		this.throwIfCapacityWaitAborted(signal);
	}

	getStatus(): QueryStatus {
		return { ...this.status };
	}

	async runLoop(): Promise<void> {
		while (!this.stopped) {
			await this.settleStatusTransitions();

			if (this.status.name !== "RUNNING") {
				await this.wait(1000);
				continue;
			}
			const release = await this.mutex.lock();
			let proc: Process | undefined;
			try {
				// Lifecycle state can change while the lock is pending; never dequeue after a pause or stop has settled.
				if (!this.stopped && this.status.name === "RUNNING") proc = this.q.shift();
			} finally {
				release();
			}
			if (!proc) {
				await this.wait(1000);
				continue;
			}
			await this.processOne(proc);
			// A handler or hook may request a pause while this item is active. Publish
			// the settled state as soon as the attempt finishes instead of making
			// pause callers wait through the optional inter-process delay.
			await this.settleStatusTransitions();
			await this.delayNext();
			await this.settleStatusTransitions();
		}
	}

	async processOne(proc: Process): Promise<void> {
		const startedAt = Date.now();
		const triesBeforeAttempt = [...proc.tries];
		proc.status = "Processing";
		proc.updatedAt = startedAt;
		proc.startTs = new Date(startedAt).toISOString();
		this.store.set(proc);

		const tryNumber = proc.tries.reduce((maximum, item) => Math.max(maximum, item.number), 0) + 1;
		const tryRec = this.store.createTry(proc.id, {
			number: tryNumber,
			startTs: new Date(startedAt).toISOString(),
			durationSec: 0,
			outcome: "PROCESSING",
			steps: [],
		});
		proc.tries = [...proc.tries, tryRec];
		this.store.set(proc);

		let lastStepTs = startedAt;
		const steps: ProcessStep[] = [];
		const recordStep = (step: { title: string; desc?: string; type?: ProcessStepInputType; data?: unknown }) => {
			const nowTs = Date.now();
			const durationSec = (nowTs - lastStepTs) / 1000;
			const stepRec = this.store.createStep(tryRec.id, {
				orderNo: steps.length + 1,
				title: step.title,
				desc: step.desc,
				type: this.normalizeStepType(step.type),
				data: step.data,
				createdTs: new Date(nowTs).toISOString(),
				durationSec: steps.length === 0 ? 0 : durationSec,
			});
			steps.push(stepRec);
			lastStepTs = nowTs;

			// Keep the in-memory process copy up to date so observers (status/timeline) get the latest step
			tryRec.steps = [...steps];
			this.updateTry(proc, tryRec);
			this.store.set(proc);

			this.enqueueStepHook(proc, tryRec, stepRec);
			return stepRec;
		};
		const handlerStepRecorder = createRevocableCallback(recordStep);
		const addHandlerStep = handlerStepRecorder.callback;

		this.inFlight = { procId: proc.id, tryNo: tryRec.number, startedAt: Date.now() };
		let attemptFailure: unknown;

		try {
			await this.hooks.onProcessStarted?.(this.query, proc, tryRec);
			if (this.stopped || this.lifecycleController.signal.aborted) {
				proc.tries = triesBeforeAttempt;
				proc.status = "Queued";
				proc.updatedAt = Date.now();
				this.store.set(proc);
				this.q.unshift(proc);
				return;
			}
			recordStep({ title: "Processing started", type: "Info" });
			recordStep({ title: "Entered handler execution", type: "Info" });
			const contract = proc.contract ?? this.query.contract;
			const contractType = contract?.type;
			const version = contract?.version;
			const contractAddress = contract?.address ?? this.resolveContractAddress(proc.rawData);

			const isLogsOnly = this.isLogsQuery();
			const handler = this.assignedHandlers.get(proc.id) ?? this.resolveHandler(proc.eventName, contractType, version);

			if (!handler || !this.isConcreteHandler(handler)) {
				throw new HandlerError(`No handler registered for ${proc.eventName} (contractType=${contractType ?? "none"}, version=${version ?? "none"})`);
			}

			try {
				if (isLogsOnly) {
					const processor = new LogEventProcessor(associateUserCallbackOwner({
						queryId: proc.queryId,
						registry: this.registry,
						restClient: this.rest.client,
						network: this.query.read.fetch.network,
						provider: this.query.read.fetch.restProvider,
						now: () => Date.now(),
					}, this.callbackOwner));
					await processor.execute(
						{
							eventName: proc.eventName,
							data: proc.data,
							rawLog: proc.rawData as SyncContractLog,
							contract: { ...contract, address: contractAddress, type: contractType, version },
							handler: handler as LogHandler,
						},
						{
							signal: this.lifecycleController.signal,
							addStep: addHandlerStep,
						}
					);
				} else {
					await runInUserCallback(
						{ kind: "handler", name: proc.eventName, queryId: proc.queryId },
						() => (handler as TransactionHandler).handle(proc.data as SyncTransaction, {
							queryId: proc.queryId,
							contract: { id: this.query.contract?.id, address: contractAddress, type: contractType, version },
							restClient: this.rest.client,
							network: this.query.read.fetch.network,
							provider: this.query.read.fetch.restProvider,
							now: () => Date.now(),
							raw: proc.rawData as SyncTransaction,
							signal: this.lifecycleController.signal,
							addStep: addHandlerStep,
						}),
						this.callbackOwner
					);
				}
			} finally {
				// A callback retained by user code belongs only to this awaited attempt.
				// Revoking its recorder also releases the process, attempt, store, and
				// queue captured by recordStep instead of retaining that object graph.
				handlerStepRecorder.revoke();
			}
			this.throwIfAttemptAborted();

			// Success path
			tryRec.outcome = "SUCCEEDED";
			const ended = Date.now();
			tryRec.endTs = new Date(ended).toISOString();
			tryRec.durationSec = (ended - startedAt) / 1000;
			recordStep({ title: "The process succeeded", type: "Success", desc: "succeeded" });
			tryRec.steps = steps;
			this.updateTry(proc, tryRec);

			proc.status = "Processed";
			proc.totalDurationSec = (ended - startedAt) / 1000;
			proc.updatedAt = ended;
			this.store.set(proc);
			this.assignedHandlers.delete(proc.id);

			this.consecutiveFailuresFor.delete(proc.id);

			await this.flushStepHooks();
			await this.hooks.onProcessSucceeded?.(this.query, proc);
			this.successfulHighWater = maxCursor(this.successfulHighWater, { timestamp: proc.pointerTS, index: proc.pointerIDX });
			this.releasePendingTimestamp(proc.id);
			this.processedCheckpoint = this.toReplaySafeCheckpoint(this.successfulHighWater);
			await this.hooks.onProcessedCheckpoint?.(this.query, this.processedCheckpoint, proc);
			this.scheduleProcessRetention(proc);
		} catch (err) {
			attemptFailure = err;
			// Failure path
			const ended = Date.now();
			tryRec.endTs = new Date(ended).toISOString();
			tryRec.durationSec = (ended - startedAt) / 1000;
			tryRec.outcome = "FAILED";
			const failure = errorDetails(err);
			tryRec.error = serializeError(err);

			recordStep({ title: "The process failed", type: "Error", desc: "failed", data: { error: failure.message } });
			tryRec.steps = steps;
			this.updateTry(proc, tryRec);
			await this.flushStepHooks();
			await this.hooks.onProcessTryFailed?.(this.query, proc, tryRec, err);

			// Persist timing so observers immediately see the failure as retries accumulate
			proc.totalDurationSec = (ended - startedAt) / 1000;
			proc.updatedAt = ended;

			if (this.stopped || this.lifecycleController.signal.aborted) {
				proc.status = "Queued";
				this.store.set(proc);
				this.q.unshift(proc);
				return;
			}

			const prev = this.consecutiveFailuresFor.get(proc.id) ?? 0;
			const now = prev + 1;
			this.consecutiveFailuresFor.set(proc.id, now);

			// Pause after threshold, otherwise re-enqueue at head (strict ordering)
			if (now >= this.query.process.pauseAfterConsecutiveFailures) {
				proc.status = "Failed";
				this.store.set(proc);
				this.pauseReason = `Processing Queue is paused due to ${this.query.process.pauseAfterConsecutiveFailures} consecutive failures`;
				this.setStatus("PAUSING", this.pauseReason);
				await this.hooks.onProcessConsecutiveFailuresReached?.(this.query, proc, now);
				this.q.unshift(proc);
			} else {
				// Reset status so the retried item doesn't remain marked as Processing if the queue pauses
				proc.status = "Queued";
				this.store.set(proc);
				this.q.unshift(proc);
			}
		} finally {
			// The recorder can escape through user code, so revoke it on every path,
			// including failures before handler invocation.
			handlerStepRecorder.revoke();
			try {
				// If failure reporting or step recording itself rejected, the ordinary
				// failure path may not have reached a terminal process state. Restore a
				// retry-safe invariant before allowing that internal error to escape.
				if (proc.status === "Processing") {
					this.recoverInterruptedAttempt(proc, tryRec, steps, startedAt, attemptFailure);
				}
				if (proc.status !== "Processed" && !this.q.some((queued) => queued.id === proc.id)) {
					this.q.unshift(proc);
				}
			} finally {
				this.inFlight = null;
			}
		}
	}

	private recoverInterruptedAttempt(
		proc: Process,
		processTry: ProcessTry,
		steps: readonly ProcessStep[],
		startedAt: number,
		error: unknown
	): void {
		const endedAt = Date.now();
		processTry.endTs = new Date(endedAt).toISOString();
		processTry.durationSec = (endedAt - startedAt) / 1000;
		processTry.outcome = "FAILED";
		if (!processTry.error) {
			try {
				processTry.error = serializeError(error);
			} catch {
				processTry.error = '{"name":"Error","message":"Attempt cleanup failed"}';
			}
		}
		processTry.steps = [...steps];
		this.updateTry(proc, processTry);

		proc.totalDurationSec = (endedAt - startedAt) / 1000;
		proc.updatedAt = endedAt;
		if (this.stopped || this.lifecycleController.signal.aborted) {
			proc.status = "Queued";
		} else {
			const failures = (this.consecutiveFailuresFor.get(proc.id) ?? 0) + 1;
			this.consecutiveFailuresFor.set(proc.id, failures);
			if (failures >= this.query.process.pauseAfterConsecutiveFailures) {
				proc.status = "Failed";
				this.pauseReason = `Processing Queue is paused due to ${this.query.process.pauseAfterConsecutiveFailures} consecutive failures`;
				this.setStatus("PAUSING", this.pauseReason);
			} else {
				proc.status = "Queued";
			}
		}
		this.store.set(proc);
	}

	getInFlight(): ProcessWithoutPayload | undefined;
	getInFlight(includePayload: false): ProcessWithoutPayload | undefined;
	getInFlight(includePayload: true): Process | undefined;
	getInFlight(includePayload: boolean): Process | ProcessWithoutPayload | undefined;
	getInFlight(includePayload = false): Process | ProcessWithoutPayload | undefined {
		const cur = this.inFlight;
		if (!cur) return undefined;

		const proc = this.store.get(cur.procId);
		if (!proc) return undefined;

		return snapshotProcess(proc, includePayload);
	}

	async pause(reason?: string): Promise<void> {
		this.assertControllable("pause");
		this.pauseReason = reason ?? "admin";

		if (this.status.name === "RUNNING") {
			this.setStatus("PAUSING", this.pauseReason);
		} else if (this.status.name !== "PAUSED" && this.status.name !== "PAUSING") {
			this.setStatus("PAUSED", this.pauseReason);
		}

		await this.settleStatusTransitions();
		await this.waitForStatus("PAUSED");
	}

	async resume(): Promise<void> {
		this.assertControllable("resume");
		if (this.status.name === "PAUSED" || this.status.name === "PAUSING") {
			// Always settle through RESUMING so retry state is reset even when a
			// threshold hook still has the queue in the PAUSING transition.
			this.setStatus("RESUMING");
		}

		await this.settleStatusTransitions();
		await this.waitForStatus("RUNNING");
	}

	private resolveHandler(eventName: string, type?: string, version?: string | number): ProcessHandler | undefined {
		const isLogsOnly = this.isLogsQuery();

		if (isLogsOnly) {
			const logHandler = this.registry.resolveLog(eventName, type, version);
			return this.isConcreteHandler(logHandler) ? logHandler : undefined;
		}

		const txHandler = this.registry.resolveTransaction(eventName);
		if (this.isConcreteHandler(txHandler)) return txHandler;
		return undefined;
	}

	requestHookPause(failure: { hook: string }): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		this.pauseReason = `Processing queue paused because ${failure.hook} failed`;
		if (this.status.name !== "PAUSED" && this.status.name !== "PAUSING") {
			this.setStatus("PAUSING", this.pauseReason, false);
		}
	}

	private isLogsQuery(): boolean {
		return this.query.type !== "Transactions";
	}

	private isConcreteHandler(h: ProcessHandler | undefined): h is ProcessHandler {
		return !!h && typeof h.handle === "function";
	}

	private resolveContractAddress(rawData: unknown): string | undefined {
		return this.query.contract?.address ?? this.resolveRawLogAddress(rawData);
	}

	/**
	 * An active item keeps one slot reserved because every non-success path puts
	 * it back at the queue head. Counting only waiting items could admit a full
	 * page and leave no room for that ordered retry.
	 */
	private occupiedSlots(): number {
		return this.q.length + (this.inFlight ? 1 : 0);
	}

	private resolveRawLogAddress(rawData: unknown): string | undefined {
		if (!rawData || typeof rawData !== "object") return undefined;
		const address = (rawData as { address?: unknown }).address;
		return typeof address === "string" && address ? address : undefined;
	}

	private normalizeStepType(type?: ProcessStepInputType): ProcessStepType {
		if (!type) return "Info";
		return `${type.charAt(0).toUpperCase()}${type.slice(1).toLowerCase()}` as ProcessStepType;
	}

	private async settleStatusTransitions(): Promise<void> {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		if (this.status.name === "PAUSING" && !this.inFlight) {
			this.setStatus("PAUSED", this.pauseReason);
		} else if (this.status.name === "RESUMING") {
			this.consecutiveFailuresFor.clear();
			this.setStatus("RUNNING");
		}

		if (this.status.name !== this.lastNotifiedStatus) {
			if (this.status.name === "PAUSED") {
				this.pauseReason = undefined;
			}

			this.lastNotifiedStatus = this.status.name;
		}
	}

	private setStatus(name: QueryQueueAndJobStatus, info?: string, notify = true): void {
		if ((this.status.name === "FAILED" || this.status.name === "STOPPED") && name !== "FAILED" && name !== "STOPPED") return;
		if (this.status.name === name && this.status.info === info) return;
		this.status = { name, info: name === "RUNNING" ? undefined : info };
		this.query.process.status = this.status;
		if (notify) this.enqueueStatusHook(this.status);
	}

	private updateTry(proc: Process, processTry: ProcessTry): void {
		proc.tries = proc.tries.map((existing) => (existing.id === processTry.id ? processTry : existing));
		this.store.set(proc);
	}

	private enqueueStepHook(proc: Process, processTry: ProcessTry, step: ProcessStep): void {
		const query = snapshotQuery(this.query);
		const process = snapshotProcess(proc);
		const attempt = snapshotProcessTry(processTry);
		const processStep = snapshotProcessStep(step);
		this.stepHookChain = this.stepHookChain
			.then(async () => this.hooks.onStepCreated?.(query, process, attempt, processStep))
			.catch(() => {});
	}

	private async flushStepHooks(): Promise<void> {
		await this.stepHookChain;
	}

	private enqueueStatusHook(status: QueryStatus): void {
		const query = snapshotQuery(this.query);
		const statusSnapshot = { ...status };
		this.statusHookChain = this.statusHookChain
			.then(async () => this.hooks.onQueryStatusChange?.(query, { kind: "process", status: statusSnapshot }))
			.catch(() => {});
	}

	async flushHooks(): Promise<void> {
		await Promise.all([this.stepHookChain, this.statusHookChain]);
	}

	private assertControllable(operation: string): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") {
			throw new Error(`Cannot ${operation} a ${this.status.name.toLowerCase()} processing queue`);
		}
	}

	private throwIfCapacityWaitAborted(signal: AbortSignal): void {
		if (!signal.aborted && !this.lifecycleController.signal.aborted && !this.stopped) return;
		const error = new Error("Process queue capacity wait was aborted");
		error.name = "AbortError";
		throw error;
	}

	private async waitForCapacityTick(signal: AbortSignal): Promise<void> {
		await new Promise<void>((resolve) => {
			const lifecycleSignal = this.lifecycleController.signal;
			let settled = false;
			const finish = () => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				signal.removeEventListener("abort", finish);
				lifecycleSignal.removeEventListener("abort", finish);
				resolve();
			};
			const timer = setTimeout(finish, 25);
			signal.addEventListener("abort", finish, { once: true });
			lifecycleSignal.addEventListener("abort", finish, { once: true });
		});
	}

	private scheduleProcessRetention(proc: Process): void {
		if (this.disposed) return;
		const durations: Record<NonNullable<typeof this.holdPeriod>, number> = {
			none: 0,
			daily: 24 * 60 * 60 * 1000,
			weekly: 7 * 24 * 60 * 60 * 1000,
			monthly: 30 * 24 * 60 * 60 * 1000,
		};

		const hold = this.holdPeriod ?? "none";
		const delay = durations[hold];

		if (delay <= 0) {
			this.store.delete(proc.id);
			this.retainedUntil.delete(proc.id);
			return;
		}

		const deadline = Date.now() + delay;
		this.retainedUntil.set(proc.id, deadline);
		if (this.retentionTimer && this.retentionTimerDeadline !== undefined && this.retentionTimerDeadline <= deadline) return;
		this.scheduleRetentionSweep();
	}

	private scheduleRetentionSweep(): void {
		if (this.retainedUntil.size === 0 || this.disposed) {
			if (this.retentionTimer) clearNodeTimeout(this.retentionTimer);
			this.retentionTimer = undefined;
			this.retentionTimerDeadline = undefined;
			return;
		}

		let nearestDeadline = Number.POSITIVE_INFINITY;
		for (const deadline of this.retainedUntil.values()) nearestDeadline = Math.min(nearestDeadline, deadline);
		if (this.retentionTimer && this.retentionTimerDeadline !== undefined && this.retentionTimerDeadline <= nearestDeadline) return;
		if (this.retentionTimer) clearNodeTimeout(this.retentionTimer);
		this.retentionTimerDeadline = nearestDeadline;
		this.retentionTimer = setNodeTimeout(() => {
			this.retentionTimer = undefined;
			this.retentionTimerDeadline = undefined;
			const now = Date.now();
			for (const [processId, deadline] of this.retainedUntil) {
				if (deadline > now) continue;
				this.store.delete(processId);
				this.retainedUntil.delete(processId);
			}
			this.scheduleRetentionSweep();
		}, nextRetentionDelay(nearestDeadline));
		this.retentionTimer.unref();
	}

	private throwIfAttemptAborted(): void {
		if (!this.stopped && !this.lifecycleController.signal.aborted) return;
		const error = new Error("Process handler execution was aborted");
		error.name = "AbortError";
		throw error;
	}

	private async waitForStatus(target: QueryQueueAndJobStatus): Promise<void> {
		while (this.status.name !== target) {
			const current = this.getStatus();
			if (current.name === "FAILED" || current.name === "STOPPED") {
				throw this.terminalTransitionError(target, current);
			}
			await this.settleStatusTransitions();
			if (this.status.name === target) break;
			try {
				await this.wait(50);
			} catch (error) {
				// An abortable wait may reject at the same instant fail()/stop() wins
				// the lifecycle race. Preserve the terminal state and reason for the
				// caller; unrelated timer failures must still propagate unchanged.
				const latest = this.getStatus();
				if (latest.name === "FAILED" || latest.name === "STOPPED") {
					throw this.terminalTransitionError(target, latest);
				}
				throw error;
			}
		}
	}

	private terminalTransitionError(target: QueryQueueAndJobStatus, terminal: QueryStatus): Error {
		return new Error(
			`Processing queue could not reach ${target} because it entered ${terminal.name}` +
				(terminal.info ? `: ${terminal.info}` : "")
		);
	}

	private async delayNext(): Promise<void> {
		if (this.nextDelayMs <= 0) return;
		await this.wait(this.nextDelayMs);
	}

	private async wait(milliseconds: number): Promise<void> {
		await sleepWithSignal(milliseconds, this.lifecycleController.signal);
	}

	/** Maintain the oldest unfinished timestamp with amortized constant work per item. */
	private trackPendingTimestamp(proc: Process): void {
		const timestampNs = parseHederaTimestampNs(proc.pointerTS);
		// Processing is FIFO. A later item with an equal or older timestamp stays
		// unfinished at least as long, so it subsumes the earlier item's barrier.
		while (this.pendingMinimums.length > this.pendingMinimumHead &&
			this.pendingMinimums[this.pendingMinimums.length - 1].timestampNs >= timestampNs) {
			this.pendingMinimums.pop();
		}
		this.pendingMinimums.push({ id: proc.id, timestampNs });
	}

	private releasePendingTimestamp(processId: number): void {
		if (this.pendingMinimums[this.pendingMinimumHead]?.id !== processId) return;
		this.pendingMinimumHead++;
		// Compact geometrically to keep storage bounded without shifting an array
		// on every successful process. Failed attempts retain their barrier.
		if (this.pendingMinimumHead * 2 >= this.pendingMinimums.length) {
			this.pendingMinimums = this.pendingMinimums.slice(this.pendingMinimumHead);
			this.pendingMinimumHead = 0;
		}
	}

	private toReplaySafeCheckpoint(highWater: ProcessedCheckpoint): ProcessedCheckpoint {
		const replayStart = subtractHederaTimestampSeconds(highWater.timestamp, this.query.read.consistency.overlapSeconds);
		let replayStartNs = parseHederaTimestampNs(replayStart);
		const replayFloor = this.getReplayFloor?.();
		if (replayFloor !== undefined) {
			const replayFloorNs = parseHederaTimestampNs(replayFloor);
			if (replayFloorNs < replayStartNs) replayStartNs = replayFloorNs;
		}
		const pendingMinimum = this.pendingMinimums[this.pendingMinimumHead];
		if (pendingMinimum && pendingMinimum.timestampNs < replayStartNs) {
			replayStartNs = pendingMinimum.timestampNs;
		}
		const candidate: ProcessedCheckpoint = {
			// Read jobs use a strict lower bound. Persisting one nanosecond before
			// the replay boundary admits every index at the boundary after restart.
			timestamp: formatHederaTimestampNs(replayStartNs > 0n ? replayStartNs - 1n : 0n),
			index: 0,
		};
		return { ...maxCursor(this.processedCheckpoint, maxCursor(this.initialProcessedCheckpoint, candidate)) };
	}
}
