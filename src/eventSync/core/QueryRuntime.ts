import type { RuntimeQueryConfig } from "../types/config";
import { ProcessStore } from "./queue/ProcessStore";
import { ProcessQueue } from "./queue/ProcessQueue";
import type { HandlerRegistry } from "../handlers/HandlerRegistry";
import { LogsReadJob } from "./jobs/LogsReadJob";
import { TransactionsReadJob } from "./jobs/TransactionsReadJob";
import type { Process, ProcessedCheckpoint, ProcessWithoutPayload, ReadCycleAudit } from "../types/domain";
import { DEFAULTS } from "../constants";
import { SyncHooks } from "../hooks/SyncHooks";
import { RestClient } from "../rest/RestClient";
import { snapshotProcess, snapshotQuery } from "../utils/snapshot";
import type { HookFailure } from "../hooks/SyncHooks";
import type { StagedProcess } from "./queue/types";
import type { GlobalPauseScope } from "./HederaEventSync";
import { errorDetails } from "../utils/error";

export class QueryRuntime {
	readonly store = new ProcessStore();
	readonly queue: ProcessQueue;
	private readLoop?: Promise<void>;
	private queueLoop?: Promise<void>;

	private readJob: LogsReadJob | TransactionsReadJob;

	private stopped = false;
	private halted = false;
	private stoppedCheckpoint?: ProcessedCheckpoint;

	constructor(
		readonly query: RuntimeQueryConfig,
		private readonly rest: RestClient,
		private readonly registry: HandlerRegistry,
		private readonly hooks: SyncHooks,
		private readonly callbackOwner?: object
	) {
		this.queue = new ProcessQueue(query, registry, this.store, this.hooks, this.rest, callbackOwner, () => this.readJob?.getReplayFloor());

		const commitPage = (items: readonly StagedProcess[], signal: AbortSignal) => this.commitPage(items, signal);
		const pruneSourceKeysBefore = (timestamp: string) => this.store.pruneSourceKeysBefore(this.query.id, timestamp);
		const getQueueSize = () => this.queue.length();
		const threshold = query.process.maxQueuedProcesses ?? DEFAULTS.maxQueuedProcessesPerQuery;

		if (query.type === "Transactions") {
			const txQuery = query as Extract<RuntimeQueryConfig, { type: "Transactions" }>;
			this.readJob = new TransactionsReadJob(txQuery, rest, registry, commitPage, pruneSourceKeysBefore, getQueueSize, threshold, this.hooks, callbackOwner);
		} else {
			const logsQuery = query as Extract<RuntimeQueryConfig, { type: "Multi-Contract-Logs" | "Single-Contract-Logs" }>;
			this.readJob = new LogsReadJob(logsQuery, rest, registry, commitPage, pruneSourceKeysBefore, getQueueSize, threshold, this.hooks, callbackOwner);
		}
	}

	private async commitPage(items: readonly StagedProcess[], signal: AbortSignal): Promise<Process[]> {
		const handlersBySourceKey = new Map(items.map((item) => [item.process.sourceKey, item.handler]));
		const fresh = this.store.filterUnseenProcesses(items.map((item) => item.process));
		if (fresh.length === 0) return [];
		await this.queue.waitForCapacity(fresh.length, signal);
		const created = this.store.createProcesses(fresh);
		await this.queue.enqueueBatch(
			created.map((process) => ({ process, handler: handlersBySourceKey.get(process.sourceKey)! }))
		);
		return created;
	}

	async start(): Promise<void> {
		if (this.halted || this.stopped) return;
		await this.readJob.initialize();
		if (this.halted || this.stopped) return;
		this.readLoop = this.superviseLoop("read", this.readJob.runForever());
		this.queueLoop = this.superviseLoop("processing", this.queue.runLoop());
	}

	setLifecycleHold(held: boolean, scope: GlobalPauseScope = "both"): void {
		if (scope !== "process") this.readJob.setLifecycleHold(held);
		if (scope !== "read") this.queue.setLifecycleHold(held);
	}

	async settleLifecycleHold(scope: GlobalPauseScope = "both"): Promise<void> {
		await Promise.all([
			...(scope !== "process" ? [this.readJob.settleLifecycleHold()] : []),
			...(scope !== "read" ? [this.queue.settleLifecycleHold()] : []),
		]);
	}

	getControlState() {
		return { read: this.readJob.getControlState(), process: this.queue.getControlState() };
	}

	restoreControlState(state: ReturnType<QueryRuntime["getControlState"]>): void {
		// A replacement has fresh loop controllers. Keep a terminal predecessor
		// held, but make explicit per-stage resume available after the operator fixes it.
		const recoverable = (status: typeof state.read.status) =>
			status.name === "FAILED" || status.name === "STOPPED"
				? { name: "PAUSED" as const, info: status.info ?? `Previous runtime ${status.name.toLowerCase()}` }
				: status;
		this.readJob.restoreControlState({ ...state.read, status: recoverable(state.read.status) });
		this.queue.restoreControlState({ ...state.process, status: recoverable(state.process.status) });
	}

	async stop(): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;
		this.readJob.abort();
		this.queue.stop();

		// Wait for long-running loops to exit
		const waits: Promise<unknown>[] = [];
		if (this.readLoop) waits.push(this.readLoop.catch(() => {}));
		if (this.queueLoop) waits.push(this.queueLoop.catch(() => {}));
		if (waits.length) await Promise.all(waits);
		await Promise.all([this.readJob.flushHooks(), this.queue.flushHooks()]);

		// Dispose timers/queues and drop stored processes
		// Capture before disposal: queued records and unfinished replay windows
		// still constrain the safe restart boundary even after loops have stopped.
		this.stoppedCheckpoint = this.queue.getProcessedCheckpoint();
		this.queue.dispose();
		this.store.clear();
	}

	/** Discard a staged runtime that was never announced or initialized. No lifecycle hooks escape. */
	discardUnstarted(): void {
		if (this.readLoop || this.queueLoop) throw new Error("Cannot discard a started runtime");
		this.stopped = true;
		this.stoppedCheckpoint = this.queue.getProcessedCheckpoint();
		this.queue.dispose();
		this.store.clear();
	}

	handleHookFailure(failure: HookFailure): void {
		if (this.stopped) return;
		if (failure.policy === "continue") return;
		if (failure.policy === "fail-fast") {
			this.halted = true;
			const reason = `Query runtime stopped because ${failure.hook} failed`;
			this.readJob.fail(reason);
			this.queue.fail(reason);
			return;
		}

		if (failure.scope === "query" || failure.scope === "read") {
			this.readJob.requestHookPause(failure);
		}
		if (failure.scope === "query" || failure.scope === "process") {
			this.queue.requestHookPause(failure);
		}
	}

	getInFlightProcess(opts?: { readonly includePayload?: false }): ProcessWithoutPayload | undefined;
	getInFlightProcess(opts: { readonly includePayload: true }): Process | undefined;
	getInFlightProcess(opts: { readonly includePayload: boolean }): Process | ProcessWithoutPayload | undefined;
	getInFlightProcess(opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined;
	getInFlightProcess(opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined {
		return this.queue.getInFlight(!!opts?.includePayload);
	}

	async pauseRead(preserveExistingPause = false): Promise<void> {
		await this.readJob.pause("admin", preserveExistingPause);
	}
	async resumeRead(): Promise<void> {
		await this.readJob.resume();
	}

	async pauseProcessing(preserveExistingPause = false): Promise<void> {
		await this.queue.pause(undefined, preserveExistingPause);
	}
	async resumeProcessing(): Promise<void> {
		await this.queue.resume();
	}

	getReadStatus() {
		return { status: this.readJob.getStatus(), cursor: this.readJob.getCursor() };
	}

	getQueueStatus() {
		return { status: this.queue.getStatus() };
	}

	getLastReadCycle(): ReadCycleAudit | undefined {
		return this.readJob.getLastCycle();
	}

	getProcessedCheckpoint(): ProcessedCheckpoint {
		return this.stoppedCheckpoint ? { ...this.stoppedCheckpoint } : this.queue.getProcessedCheckpoint();
	}

	listProcesses(page: number, pageSize: number, status?: Process["status"], order: "asc" | "desc" = "asc") {
		const result = this.store.listByQuery(this.query.id, { page, pageSize, status, order });
		return { ...result, items: result.items.map((process) => snapshotProcess(process)) };
	}

	countProcesses(status?: Process["status"]): number {
		return this.store.countByQuery(this.query.id, status);
	}

	getFailedProcess(opts?: { readonly includePayload?: false }): ProcessWithoutPayload | undefined;
	getFailedProcess(opts: { readonly includePayload: true }): Process | undefined;
	getFailedProcess(opts: { readonly includePayload: boolean }): Process | ProcessWithoutPayload | undefined;
	getFailedProcess(opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined;
	getFailedProcess(opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined {
		const failed = this.store.getFailed(this.query.id);
		if (!failed) return undefined;
		return snapshotProcess(failed, !!opts?.includePayload);
	}

	getStatistics() {
		return this.store.getStatistics(this.query.id);
	}

	getProcessingTimeline(size: number): Process[] {
		return this.store.getProcessingTimeline(this.query.id, size).map((process) => snapshotProcess(process));
	}

	getQuerySnapshot(): RuntimeQueryConfig {
		const query = snapshotQuery(this.query);
		query.read.status = this.readJob.getStatus();
		query.process.status = this.queue.getStatus();
		return query;
	}

	private async superviseLoop(kind: "read" | "processing", loop: Promise<void>): Promise<void> {
		try {
			await loop;
		} catch (error) {
			if (this.stopped || this.halted) return;
			this.halted = true;
			const details = errorDetails(error);
			const reason = `Unexpected ${kind} loop failure (${details.name}): ${details.message}`;
			this.readJob.fail(reason);
			this.queue.fail(reason);
		}
	}
}
