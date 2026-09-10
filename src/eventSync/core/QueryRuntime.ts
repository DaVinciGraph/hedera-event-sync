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
import { errorDetails } from "../utils/error";

export class QueryRuntime {
	readonly store = new ProcessStore();
	readonly queue: ProcessQueue;
	private readLoop?: Promise<void>;
	private queueLoop?: Promise<void>;

	private readJob: LogsReadJob | TransactionsReadJob;

	private stopped = false;
	private halted = false;

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

	async pauseRead(): Promise<void> {
		await this.readJob.pause("admin");
	}
	async resumeRead(): Promise<void> {
		await this.readJob.resume();
	}

	async pauseProcessing(): Promise<void> {
		await this.queue.pause();
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
		return this.queue.getProcessedCheckpoint();
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
		return snapshotQuery(this.query);
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
