import type { QueryQueueAndJobStatus, QueryStatus, RuntimeQueryConfig } from "../../types/config";
import type { Cursor, ReadCycleAudit } from "../../types/domain";
import type { SyncReadItem } from "../../types/mirror";
import { DEFAULTS } from "../../constants";
import { withJitter } from "../../utils/jitter";
import { sleep, sleepWithSignal } from "../../utils/timer";
import { SyncHooks } from "../../hooks/SyncHooks";
import { compareHederaTimestamps, currentHederaTimestamp, normalizeHederaTimestamp, subtractHederaTimestampSeconds } from "../../utils/timestamp";
import { compareCursor } from "../../utils/comparators";
import { snapshotQuery, snapshotReadCycle } from "../../utils/snapshot";
import { runInUserCallback } from "../../utils/userCallback";
import { serializeError } from "../../utils/error";

export abstract class ReadJob {
	protected consecutiveFailures = 0;
	private lastFailureSource?: "read" | "until";
	protected cursor: Cursor;
	protected readonly initialCursor: Cursor;
	protected status: QueryStatus;
	protected maxPagesPerCycle: number;
	protected fetchDelayMs: number;
	protected pauseReason?: string;
	protected lastNotifiedStatus: QueryQueueAndJobStatus;
	private lastCycle?: ReadCycleAudit;

	private aborted = false;
	private abortController?: AbortController;
	private pausedByBackpressure = false;
	private cycleRunning = false;
	private resetFailuresOnResume = false;
	private statusHookChain: Promise<void> = Promise.resolve();
	private initialization?: Promise<void>;

	constructor(
		protected readonly query: RuntimeQueryConfig,
		protected readonly getQueueSize: () => number = () => 0,
		protected readonly backpressureThreshold: number = DEFAULTS.maxQueuedProcessesPerQuery,
		protected readonly hooks: SyncHooks,
		protected readonly callbackOwner?: object
	) {
		const ts = query.params.timestamp ?? "0";
		const idx = "index" in query.params ? query.params.index ?? 0 : 0;
		this.cursor = { timestamp: normalizeHederaTimestamp(ts), index: Number(idx) };
		this.initialCursor = { ...this.cursor };
		this.status = { ...query.read.status };
		this.lastNotifiedStatus = this.status.name;
		this.maxPagesPerCycle = Math.max(1, query.read.fetch.maxPagesPerCycle ?? DEFAULTS.maxPagesPerCycle);
		this.fetchDelayMs = Math.max(0, query.read.fetch.nextDelayMs ?? 0);
	}

	abort(reason = "Query runtime stopped"): void {
		this.aborted = true;
		this.setStatus("STOPPED", reason);
		this.cancelInFlightCycle();
	}

	fail(reason: string): void {
		this.aborted = true;
		this.setStatus("FAILED", reason);
		this.cancelInFlightCycle();
	}

	requestHookPause(failure: { hook: string }): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		this.pausedByBackpressure = false;
		this.pauseReason = `Read job paused because ${failure.hook} failed`;
		if (this.status.name !== "PAUSED" && this.status.name !== "PAUSING") {
			this.setStatus("PAUSING", this.pauseReason, false);
		}
		this.cancelInFlightCycle();
	}

	async runForever(): Promise<void> {
		await this.initialize();
		if (this.aborted) return;
		await this.settleStatusTransitions();

		const initialDelayMs = withJitter(1000 * Math.random() * 5, 0.25);
		this.abortController = new AbortController();
		await this.waitWithAbort(initialDelayMs, this.abortController.signal);

		while (!this.aborted) {
			this.abortController = new AbortController();
			const signal = this.abortController.signal;
			await this.settleStatusTransitions();

			// Auto-resume if paused due to backpressure and backlog dropped
			if (this.pausedByBackpressure && this.getQueueSize() < this.backpressureThreshold * 0.8) {
				this.requestResume(false);
				this.pausedByBackpressure = false;
				await this.settleStatusTransitions();
			}

			if (this.status.name !== "RUNNING") {
				await this.waitWithAbort(100, signal);
				continue;
			}

			// Pre-cycle backpressure
			if (this.getQueueSize() >= this.backpressureThreshold) {
				this.pausedByBackpressure = true;
				this.requestPause("backpressure");
				await this.waitWithAbort(250, signal);
				continue;
			}

			// Cancel late ticks if a cycle is still running
			if (this.cycleRunning) {
				await this.waitWithAbort(50, signal);
			} else {
				const audit: ReadCycleAudit = {
					queryId: this.query.id,
					startedAt: Date.now(),
					endedAt: 0,
					success: false,
				};
				let completionEmitted = false;

				try {
					this.cycleRunning = true;

					await this.hooks.onReadCycleStarted?.(this.query, this.cursor, this.query.read.fetch.batchSize);

					const items = await this.cycle(signal);
					if (this.aborted || signal.aborted) throw this.abortError();
					audit.success = true;
					audit.itemCount = items.length;
					audit.items = items;
					const endedAt = Date.now();
					audit.endedAt = endedAt;

					// A completed read has one terminal audit event. Later predicate
					// failures are operational failures, not a retroactive read failure.
					completionEmitted = true;
					await this.hooks.onReadCycleCompleted?.(this.query, {
						items,
						cursor: this.cursor,
						startedAt: audit.startedAt,
						endedAt,
						success: audit.success,
						itemCount: audit.itemCount,
					});

					let predicateFailed = false;
					const predicateDeferred = this.query.read.until !== undefined && this.getReplayFloor() !== undefined;
					if (!this.aborted && !signal.aborted && !predicateDeferred && this.query.read.until) {
						try {
							const fulfilled = await runInUserCallback(
								{ kind: "until", name: "read.until", queryId: this.query.id },
								() => this.query.read.until!({ ...this.cursor }),
								this.callbackOwner
							);
							if (fulfilled) this.requestPause("fulfilled");
						} catch (err) {
							predicateFailed = true;
							const abortedDuringPredicate = this.aborted || signal.aborted;
							if (!abortedDuringPredicate) await this.recordReadFailure(err, "until");
						}
					}
					// A successful read recovers a read-originated failure streak.
					// Only predicate failures survive cycles that defer the predicate.
					if (!predicateFailed && (!predicateDeferred || this.lastFailureSource !== "until")) {
						this.resetConsecutiveFailures();
					}
				} catch (err) {
					const abortedDuringCycle = this.aborted || signal.aborted;
					if (!abortedDuringCycle && !completionEmitted) {
						await this.recordReadFailure(err, "read");
					}

					if (!completionEmitted) {
						audit.success = false;
						audit.error = serializeError(err);
					}
				} finally {
					audit.endedAt = Date.now();
					if (!completionEmitted) {
						completionEmitted = true;
						await this.hooks.onReadCycleCompleted?.(this.query, {
							items: audit.items ?? [],
							cursor: this.cursor,
							startedAt: audit.startedAt,
							endedAt: audit.endedAt,
							success: audit.success,
							error: audit.error,
							itemCount: audit.itemCount,
						});
					}
					this.lastCycle = { ...audit };
					this.cycleRunning = false;
				}

				if (this.aborted) break;
			}

			await this.settleStatusTransitions();
			const intervalSeconds = this.query.read.fetch.pollIntervalSeconds;
			const jitteredSeconds = withJitter(intervalSeconds);
			const delaySeconds = Number.isFinite(jitteredSeconds) ? jitteredSeconds : intervalSeconds;
			const delayMs = delaySeconds > Number.MAX_VALUE / 1000 ? Number.MAX_VALUE : delaySeconds * 1000;
			await this.waitWithAbort(delayMs, signal);
		}
	}

	async initialize(): Promise<void> {
		this.initialization ??= Promise.resolve(this.hooks.onReadInit?.(this.query, this.query.read.fetch.pollIntervalSeconds));
		await this.initialization;
	}

	getStatus(): QueryStatus {
		return { ...this.status };
	}
	getCursor(): Cursor {
		return { ...this.cursor };
	}

	/** Timestamp boundary that must remain replayable while pagination is unfinished. */
	getReplayFloor(): string | undefined {
		return undefined;
	}

	getLastCycle(): ReadCycleAudit | undefined {
		return this.lastCycle ? snapshotReadCycle(this.lastCycle) : undefined;
	}

	async pause(reason?: string): Promise<void> {
		this.assertControllable("pause");
		// An operator pause supersedes backpressure ownership; only a future
		// backpressure transition may opt this job into automatic resumption again.
		this.pausedByBackpressure = false;
		this.requestPause(reason ?? "admin");

		await this.settleStatusTransitions();
		await this.waitForStatus("PAUSED");
	}

	async resume(): Promise<void> {
		this.assertControllable("resume");
		this.requestResume(true);

		await this.settleStatusTransitions();
		await this.waitForStatus("RUNNING");
	}

	protected abstract cycle(signal: AbortSignal): Promise<SyncReadItem[]>;

	protected replayStartTimestamp(): string {
		const replay = subtractHederaTimestampSeconds(this.cursor.timestamp, this.query.read.consistency.overlapSeconds);
		return compareHederaTimestamps(replay, this.initialCursor.timestamp) < 0 ? this.initialCursor.timestamp : replay;
	}

	protected safeHeadTimestamp(): string {
		return currentHederaTimestamp(this.query.read.consistency.finalityLagSeconds);
	}

	protected isEligibleCursor(item: Cursor, lowerBound = this.replayStartTimestamp()): boolean {
		if (compareCursor(item, this.initialCursor) <= 0) return false;
		return compareHederaTimestamps(item.timestamp, lowerBound) >= 0;
	}

	private cancelInFlightCycle(): void {
		if (!this.abortController) return;
		this.abortController.abort();
		this.abortController = undefined;
	}

	/** Internal loop transition: request a pause without waiting on the active cycle. */
	private requestPause(reason: string): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		this.pauseReason = reason;
		if (this.status.name === "RUNNING") {
			this.setStatus("PAUSING", this.pauseReason);
		} else if (this.status.name === "PAUSED") {
			this.setStatus("PAUSED", this.pauseReason);
		} else if (this.status.name !== "PAUSING") {
			this.setStatus("PAUSED", this.pauseReason);
		}
		this.cancelInFlightCycle();
	}

	/** Internal loop transition: request a resume without creating a floating Promise. */
	private requestResume(resetConsecutiveFailures: boolean): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		if (this.status.name === "PAUSED" || this.status.name === "PAUSING") {
			this.resetFailuresOnResume = resetConsecutiveFailures;
			this.setStatus("RESUMING");
		} else if (this.status.name === "RESUMING" && resetConsecutiveFailures) {
			// An explicit resume takes precedence if it races an automatic resume.
			this.resetFailuresOnResume = true;
		}
	}

	protected async waitWithAbort(ms: number, signal: AbortSignal): Promise<void> {
		await sleepWithSignal(ms, signal);
	}

	private async settleStatusTransitions(): Promise<void> {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") return;
		if (this.status.name === "PAUSING" && !this.cycleRunning) {
			this.setStatus("PAUSED", this.pauseReason);
		} else if (this.status.name === "RESUMING") {
			this.setStatus("RUNNING");
			if (this.resetFailuresOnResume) this.resetConsecutiveFailures();
			this.resetFailuresOnResume = false;
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
		this.query.read.status = this.status;
		if (notify) {
			const query = snapshotQuery(this.query);
			const status = { ...this.status };
			this.statusHookChain = this.statusHookChain
				.then(async () => this.hooks.onQueryStatusChange?.(query, { kind: "read", status }))
				.catch(() => {});
		}
	}

	async flushHooks(): Promise<void> {
		await this.statusHookChain;
	}

	private assertControllable(operation: string): void {
		if (this.status.name === "FAILED" || this.status.name === "STOPPED") {
			throw new Error(`Cannot ${operation} a ${this.status.name.toLowerCase()} read job`);
		}
	}

	private async waitForStatus(target: QueryQueueAndJobStatus): Promise<void> {
		while (this.status.name !== target) {
			if (this.status.name === "FAILED" || this.status.name === "STOPPED") {
				throw new Error(
					`Read job could not reach ${target} because it entered ${this.status.name}` +
						(this.status.info ? `: ${this.status.info}` : "")
				);
			}
			await this.settleStatusTransitions();
			if (this.status.name === target) break;
			await sleep(50);
		}
	}

	private resetConsecutiveFailures(): void {
		this.consecutiveFailures = 0;
		this.lastFailureSource = undefined;
	}

	private async recordReadFailure(error: unknown, source: "read" | "until"): Promise<void> {
		this.lastFailureSource = source;
		const failureCount = ++this.consecutiveFailures;
		if (failureCount >= this.query.read.pauseAfterConsecutiveFailures) {
			this.requestPause(`Read Job is paused due to ${this.query.read.pauseAfterConsecutiveFailures} consecutive failures`);
			await this.hooks.onReadConsecutiveFailuresReached?.(this.query, failureCount);
		}

		// A concurrent resume can reset the live counter while the threshold hook
		// is awaited. Report the count that belongs to this failure, not that reset.
		await this.hooks.onReadFailure?.(this.query, error, failureCount);
	}

	private abortError(): Error {
		const error = new Error("Read cycle was aborted");
		error.name = "AbortError";
		return error;
	}
}
