import type { ContractLogTopicFilter, NetworkName, QueryConfig, RuntimeQueryConfig } from "../types/config";
import { HandlerRegistry } from "../handlers/HandlerRegistry";
import { QueryRuntime } from "./QueryRuntime";
import type { Process, ProcessedCheckpoint, ProcessWithoutPayload, ReadCycleAudit } from "../types/domain";
import type { HookFailure, HookFailurePolicy, SyncHooks } from "../hooks/SyncHooks";
import { HookBase } from "../hooks/HookBase";
import { CompositeHooks } from "../hooks/CompositeHooks";
import { RestClient } from "../rest/RestClient";
import { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { AsyncMutex } from "../utils/mutex";
import { DEFAULTS } from "../constants";
import { normalizeHederaTimestamp } from "../utils/timestamp";
import { snapshotValue } from "../utils/snapshot";
import { assertOutsideUserCallback } from "../utils/userCallback";
import { errorDetails } from "../utils/error";
import { LifecycleBusyError } from "../errors";
import { hasContractLogTopicFilters, topicLogOverlapCanAdvance } from "../utils/topicLogWindow";
import type { AbiEvent } from "abitype";
import { AbiEvent as OxAbiEvent } from "ox";
import { toEventSelector } from "viem";

/** One-based, clamped page returned by runtime inspection methods. */
export type Page<T> = {
	items: T[];
	page: number;
	pageSize: number;
	/** Number of records after optional filtering. */
	total: number;
	/** Number of available pages; empty result sets report one empty page. */
	totalPages: number;
	hasPrev: boolean;
	hasNext: boolean;
};

/** Filtering and ordering applied by paginated `listQueries` calls. */
export type ListQueriesOptions = {
	/** Optional filter by query type. */
	type?: QueryConfig["type"];
	/** Optional filter by configured network name. */
	network?: NetworkName;
	/** Order of results. Default: "insertion" (Map insertion order). */
	order?: "insertion" | "id-asc" | "id-desc";
};

/** Construction options for the synchronization engine. */
export type HederaEventSyncOptions = {
	/** Configured REST client used by every query runtime in this instance. */
	restClient: HederaRestClient;
	/**
	 * Keeps `create()` process-wide by default. While the guarded instance is
	 * active, later calls return it and do not apply their supplied options. A
	 * guarded call made during shutdown waits for the one replacement instance.
	 * Set to `false` only when intentionally managing isolated instances.
	 */
	singletonGuard?: boolean;
	/** Controls runtime behavior after a hook rejects. Defaults to "continue". */
	hookFailurePolicy?: HookFailurePolicy;
	/** Query defaults used when the corresponding query-level value is absent. */
	defaults?: {
		/** Base delay between completed read cycles; runtime jitter is applied. Defaults to 30 seconds. */
		pollIntervalSeconds?: number;
		/** Requested Mirror Node page size. Defaults to 100. */
		batchSize?: number;
		/** Maximum pages consumed in one read cycle. Defaults to 10. */
		maxPagesPerCycle?: number;
		/** Hard queued-and-active process capacity per query. Defaults to 10,000. */
		maxQueuedProcessesPerQuery?: number;
		/** Upper timestamp bound kept behind the local clock. Defaults to 15 seconds. */
		finalityLagSeconds?: number;
		/** History horizon re-read for late-indexed records. Defaults to 30 seconds. */
		overlapSeconds?: number;
		/** Emits concise REST-facade request diagnostics. Defaults to false. */
		debugRestClient?: boolean;
	};
};

type ResolvedDefaults = {
	pollIntervalSeconds: number;
	batchSize: number;
	maxPagesPerCycle: number;
	maxQueuedProcessesPerQuery: number;
	finalityLagSeconds: number;
	overlapSeconds: number;
};

type QueryLifecyclePhase = "addQuery" | "updateQuery" | "removeQuery" | "pauseRead" | "resumeRead" | "pauseProcessing" | "resumeProcessing";
type QueryLifecycleState = {
	phase: QueryLifecyclePhase;
	candidate?: RuntimeQueryConfig;
};

/**
 * Coordinates Mirror Node readers, ordered processing queues, handlers, hooks,
 * and resumable checkpoints for a set of synchronization queries.
 */
export class HederaEventSync {
	private static _instance?: HederaEventSync;
	private readonly rest: RestClient;
	private readonly defaults: ResolvedDefaults;
	private readonly mutationMutex = new AsyncMutex();
	private readonly callbackOwner: object;
	private readonly queryLifecycles = new Map<number, QueryLifecycleState>();
	private shutdownPromise?: Promise<void>;
	private closed = false;

	private readonly _registry = new HandlerRegistry();
	private readonly queries = new Map<number, QueryRuntime>();

	private readonly hooks: CompositeHooks;

	/** Replaces the hook observers used for subsequently dispatched events. */
	setHooks(h: SyncHooks | SyncHooks[]): void {
		this.hooks.setHooks(Array.isArray(h) ? h : [h]);
	}

	private constructor(opts: HederaEventSyncOptions) {
		this.hooks = new CompositeHooks([new HookBase()], {
			policy: opts.hookFailurePolicy,
			onFailure: (failure) => this.handleHookFailure(failure),
		});
		this.callbackOwner = this.hooks;
		this.defaults = {
			pollIntervalSeconds: opts.defaults?.pollIntervalSeconds ?? DEFAULTS.pollIntervalSeconds,
			batchSize: opts.defaults?.batchSize ?? DEFAULTS.batchSize,
			maxPagesPerCycle: opts.defaults?.maxPagesPerCycle ?? DEFAULTS.maxPagesPerCycle,
			maxQueuedProcessesPerQuery: opts.defaults?.maxQueuedProcessesPerQuery ?? DEFAULTS.maxQueuedProcessesPerQuery,
			finalityLagSeconds: opts.defaults?.finalityLagSeconds ?? DEFAULTS.finalityLagSeconds,
			overlapSeconds: opts.defaults?.overlapSeconds ?? DEFAULTS.overlapSeconds,
		};
		this.rest = new RestClient(opts.restClient, { debug: opts?.defaults?.debugRestClient || false });
	}

	private handleHookFailure(failure: HookFailure): void {
		this.queries.get(failure.query.id)?.handleHookFailure(failure);
	}

	/**
	 * Returns the active process-local singleton, or creates it when absent.
	 * Later guarded calls do not replace the active instance's options.
	 */
	static async create(opts: HederaEventSyncOptions): Promise<HederaEventSync> {
		const guarded = opts.singletonGuard !== false;
		if (guarded) {
			for (;;) {
				const current = HederaEventSync._instance;
				if (!current) break;
				if (!current.shutdownPromise) return current;
				assertOutsideUserCallback("create", current.callbackOwner);
				// Keep the singleton occupied until shutdown has fully settled. A
				// concurrent create then competes for the one replacement instance.
				await current.shutdownPromise.catch(() => {});
			}
		}
		if (opts.hookFailurePolicy !== undefined && !["continue", "pause", "fail-fast"].includes(opts.hookFailurePolicy)) {
			throw new Error("hookFailurePolicy must be one of: continue, pause, fail-fast");
		}
		const inst = new HederaEventSync(opts);
		inst.validateDefaults();
		if (guarded) HederaEventSync._instance = inst;
		return inst;
	}

	/**
	 * Validates, registers, and starts a query. By default, a query with an
	 * equivalent normalized runtime configuration is ignored.
	 */
	async addQuery(cfg: QueryConfig, skipOnDuplication: boolean = true): Promise<void> {
		assertOutsideUserCallback("addQuery", this.callbackOwner);
		this.assertActive();
		this.validateInputShape(cfg);
		const resolved = this.toRuntimeConfig(cfg);
		this.validate(resolved);

		let runtime: QueryRuntime | undefined;
		let lifecycle: QueryLifecycleState | undefined;
		let skipped = false;
		await this.withMutationLock(async () => {
			this.assertActive();
			this.assertQueryLifecycleIdle(resolved.id, "addQuery");
			if (this.queries.has(resolved.id)) throw new Error(`Query ${resolved.id} already exists`);
			const duplicate = this.findDuplicateQuery(resolved);
			if (duplicate?.busy) throw new LifecycleBusyError("addQuery", duplicate.queryId, duplicate.busy.phase);
			if (skipOnDuplication && duplicate) {
				skipped = true;
				return;
			}

			runtime = new QueryRuntime(resolved, this.rest, this._registry, this.hooks, this.callbackOwner);
			lifecycle = { phase: "addQuery", candidate: resolved };
			this.queryLifecycles.set(resolved.id, lifecycle);
			this.queries.set(resolved.id, runtime);
		});
		if (skipped || !runtime || !lifecycle) return;

		let announced = false;
		try {
			await this.hooks.onQueryAdded?.(resolved);
			announced = true;
			await runtime.start();
		} catch (error) {
			await runtime.stop().catch(() => {});
			if (announced) await this.hooks.onQueryRemoved?.(resolved);
			await this.withMutationLock(async () => {
				if (this.queries.get(resolved.id) === runtime) this.queries.delete(resolved.id);
			});
			throw error;
		} finally {
			await this.finishQueryLifecycle(resolved.id, lifecycle);
		}
	}

	/** Stops and replaces an existing query runtime with the same query ID. */
	async updateQuery(cfg: QueryConfig, skipOnDuplication: boolean = true): Promise<void> {
		assertOutsideUserCallback("updateQuery", this.callbackOwner);
		this.assertActive();
		this.validateInputShape(cfg);
		const resolved = this.toRuntimeConfig(cfg);
		this.validate(resolved);

		let existing: QueryRuntime | undefined;
		let lifecycle: QueryLifecycleState | undefined;
		let skipped = false;
		await this.withMutationLock(async () => {
			this.assertActive();
			this.assertQueryLifecycleIdle(resolved.id, "updateQuery");
			existing = this.queries.get(resolved.id);
			if (!existing) throw new Error(`Query ${resolved.id} not found`);
			const duplicate = this.findDuplicateQuery(resolved, resolved.id);
			if (duplicate?.busy) throw new LifecycleBusyError("updateQuery", duplicate.queryId, duplicate.busy.phase);
			if (skipOnDuplication && duplicate) {
				skipped = true;
				return;
			}
			lifecycle = { phase: "updateQuery", candidate: resolved };
			this.queryLifecycles.set(resolved.id, lifecycle);
		});
		if (skipped || !existing || !lifecycle) return;

		let replacement: QueryRuntime | undefined;
		let replacementAnnounced = false;
		try {
			await existing.stop();
			await this.hooks.onQueryRemoved?.(existing.query);

			replacement = new QueryRuntime(resolved, this.rest, this._registry, this.hooks, this.callbackOwner);
			await this.withMutationLock(async () => {
				this.assertActive();
				this.queries.set(resolved.id, replacement!);
			});
			await this.hooks.onQueryAdded?.(resolved);
			replacementAnnounced = true;
			await replacement.start();
		} catch (error) {
			await replacement?.stop().catch(() => {});
			if (replacementAnnounced) await this.hooks.onQueryRemoved?.(resolved);
			await this.withMutationLock(async () => {
				const current = this.queries.get(resolved.id);
				if (current === existing || current === replacement) this.queries.delete(resolved.id);
			});
			throw error;
		} finally {
			await this.finishQueryLifecycle(resolved.id, lifecycle);
		}
	}

	/** Stops and removes a query; missing query IDs are ignored. */
	async removeQuery(queryId: number): Promise<void> {
		assertOutsideUserCallback("removeQuery", this.callbackOwner);
		let runtime: QueryRuntime | undefined;
		let lifecycle: QueryLifecycleState | undefined;
		await this.withMutationLock(async () => {
			if (this.closed) return;
			this.assertQueryLifecycleIdle(queryId, "removeQuery");
			runtime = this.queries.get(queryId);
			if (!runtime) return;
			lifecycle = { phase: "removeQuery" };
			this.queryLifecycles.set(queryId, lifecycle);
		});
		if (!runtime || !lifecycle) return;

		try {
			await runtime.stop();
			await this.hooks.onQueryRemoved?.(runtime.query);
		} finally {
			await this.withMutationLock(async () => {
				if (this.queries.get(queryId) === runtime) this.queries.delete(queryId);
			});
			await this.finishQueryLifecycle(queryId, lifecycle);
		}
	}

	/** Stops all query runtimes, flushes ordered hooks, and releases in-memory state. */
	async shutdown(): Promise<void> {
		assertOutsideUserCallback("shutdown", this.callbackOwner);
		if (!this.shutdownPromise) this.shutdownPromise = this.shutdownInternal();
		try {
			await this.shutdownPromise;
		} catch (error) {
			this.shutdownPromise = undefined;
			throw error;
		}
	}

	/** Returns every resolved query in insertion order. */
	listQueries(): RuntimeQueryConfig[];
	/** Returns a filtered, ordered, one-based page of resolved queries. */
	listQueries(page: number, pageSize: number, opts?: ListQueriesOptions): Page<RuntimeQueryConfig>;
	listQueries(page?: number, pageSize?: number, opts?: ListQueriesOptions): RuntimeQueryConfig[] | Page<RuntimeQueryConfig> {
		if (page === undefined && pageSize === undefined) {
			const out: RuntimeQueryConfig[] = [];
			for (const rt of this.queries.values()) {
				out.push(rt.getQuerySnapshot());
			}
			return out;
		}

		const p = this.positiveInteger(page ?? 1, "page");
		const ps = this.positiveInteger(pageSize ?? 50, "pageSize");
		const start = (p - 1) * ps;
		const endExclusive = start + ps;

		const order = opts?.order ?? "insertion";
		const filterType = opts?.type;
		const filterNetwork = opts?.network;

		// In insertion order, collect only the requested slice to avoid copying every query.
		let total = 0;
		const items: RuntimeQueryConfig[] = [];

		if (order === "insertion") {
			let seen = 0;
			for (const rt of this.queries.values()) {
				if (filterType && rt.query.type !== filterType) continue;
				if (filterNetwork && rt.query.read.fetch.network !== filterNetwork) continue;
				if (seen >= start && seen < endExclusive) {
					items.push(rt.getQuerySnapshot());
				}
				seen++;
			}
			total = this.countFiltered(filterType, filterNetwork);
		} else {
			// Sorting IDs avoids materializing full query snapshots outside the requested page.
			const ids: number[] = [];
			for (const id of this.queries.keys()) {
				const q = this.queries.get(id)!;
				if ((!filterType || q.query.type === filterType) && (!filterNetwork || q.query.read.fetch.network === filterNetwork)) {
					ids.push(id);
				}
			}
			ids.sort((a, b) => (order === "id-asc" ? a - b : b - a));
			total = ids.length;
			const slice = ids.slice(start, endExclusive);
			for (const id of slice) {
				const rt = this.queries.get(id)!;
				items.push(rt.getQuerySnapshot());
			}
		}

		const totalPages = Math.max(1, Math.ceil(total / ps));
		const pageClamped = Math.min(p, totalPages);
		if (p !== pageClamped && total > 0) {
			// Requests beyond the end return the last available page.
			const newStart = (pageClamped - 1) * ps;
			const newEndExclusive = newStart + ps;
			items.length = 0;
			if (order === "insertion") {
				let seen = 0;
				for (const rt of this.queries.values()) {
					if (filterType && rt.query.type !== filterType) continue;
					if (filterNetwork && rt.query.read.fetch.network !== filterNetwork) continue;
					if (seen >= newStart && seen < newEndExclusive) {
						items.push(rt.getQuerySnapshot());
					}
					seen++;
				}
			} else {
				const ids: number[] = [];
				for (const id of this.queries.keys()) {
					const q = this.queries.get(id)!;
					if ((!filterType || q.query.type === filterType) && (!filterNetwork || q.query.read.fetch.network === filterNetwork)) {
						ids.push(id);
					}
				}
				ids.sort((a, b) => (order === "id-asc" ? a - b : b - a));
				const slice = ids.slice(newStart, newEndExclusive);
				for (const id of slice) {
					const rt = this.queries.get(id)!;
					items.push(rt.getQuerySnapshot());
				}
			}
		}

		return {
			items,
			page: pageClamped,
			pageSize: ps,
			total,
			totalPages,
			hasPrev: pageClamped > 1,
			hasNext: pageClamped < totalPages,
		};
	}

	private countFiltered(type?: QueryConfig["type"], network?: NetworkName): number {
		if (!type && !network) return this.queries.size;
		let n = 0;
		for (const rt of this.queries.values()) {
			if (type && rt.query.type !== type) continue;
			if (network && rt.query.read.fetch.network !== network) continue;
			n++;
		}
		return n;
	}

	/** Returns a resolved query snapshot without exposing mutable runtime state. */
	getQuery(queryId: number): RuntimeQueryConfig | undefined {
		return this.queries.get(queryId)?.getQuerySnapshot();
	}

	/** Returns the active process metadata; payload fields require `{ includePayload: true }`. */
	getInFlightProcess(queryId: number, opts?: { readonly includePayload?: false }): ProcessWithoutPayload | undefined;
	getInFlightProcess(queryId: number, opts: { readonly includePayload: true }): Process | undefined;
	getInFlightProcess(queryId: number, opts: { readonly includePayload: boolean }): Process | ProcessWithoutPayload | undefined;
	getInFlightProcess(queryId: number, opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined;
	getInFlightProcess(queryId: number, opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined {
		return this.queries.get(queryId)?.getInFlightProcess(opts);
	}

	/** Pauses ingestion after cancelling and settling the active read cycle. */
	async pauseRead(queryId: number): Promise<void> {
		assertOutsideUserCallback("pauseRead", this.callbackOwner);
		await this.withQueryControl(queryId, "pauseRead", (runtime) => runtime.pauseRead());
	}

	/** Resumes ingestion for a paused query. */
	async resumeRead(queryId: number): Promise<void> {
		assertOutsideUserCallback("resumeRead", this.callbackOwner);
		await this.withQueryControl(queryId, "resumeRead", (runtime) => runtime.resumeRead());
	}

	/** Pauses the processing queue after any active handler settles. */
	async pauseProcessing(queryId: number): Promise<void> {
		assertOutsideUserCallback("pauseProcessing", this.callbackOwner);
		await this.withQueryControl(queryId, "pauseProcessing", (runtime) => runtime.pauseProcessing());
	}

	/** Resumes the processing queue for a paused query. */
	async resumeProcessing(queryId: number): Promise<void> {
		assertOutsideUserCallback("resumeProcessing", this.callbackOwner);
		await this.withQueryControl(queryId, "resumeProcessing", (runtime) => runtime.resumeProcessing());
	}

	/** Returns read status and its ingestion cursor, which may represent a fully drained window boundary. */
	getReadStatus(queryId: number) {
		return this.queries.get(queryId)?.getReadStatus();
	}

	/** Returns the processing queue's current lifecycle status. */
	getQueueStatus(queryId: number) {
		return this.queries.get(queryId)?.getQueueStatus();
	}

	/** Returns the latest failed process metadata; payload fields require `{ includePayload: true }`. */
	getFailedProcess(queryId: number, opts?: { readonly includePayload?: false }): ProcessWithoutPayload | undefined;
	getFailedProcess(queryId: number, opts: { readonly includePayload: true }): Process | undefined;
	getFailedProcess(queryId: number, opts: { readonly includePayload: boolean }): Process | ProcessWithoutPayload | undefined;
	getFailedProcess(queryId: number, opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined;
	getFailedProcess(queryId: number, opts?: { readonly includePayload?: boolean }): Process | ProcessWithoutPayload | undefined {
		return this.queries.get(queryId)?.getFailedProcess(opts);
	}

	/** Returns the most recent terminal read-cycle audit. */
	getLastReadCycle(queryId: number): ReadCycleAudit | undefined {
		return this.queries.get(queryId)?.getLastReadCycle();
	}

	/** Returns the monotonic, overlap-aware cursor safe to persist for restart. */
	getProcessedCheckpoint(queryId: number): ProcessedCheckpoint | undefined {
		return this.queries.get(queryId)?.getProcessedCheckpoint();
	}

	/** Returns a clamped page of retained processes for one query. */
	listProcesses(queryId: number, page: number, pageSize: number, status?: Process["status"], order: "asc" | "desc" = "asc") {
		const validPage = this.positiveInteger(page, "page");
		const validPageSize = this.positiveInteger(pageSize, "pageSize");
		const runtime = this.queries.get(queryId);
		let res = runtime?.listProcesses(validPage, validPageSize, status, order);
		const total = res ? res.total : 0;
		const totalPages = Math.max(1, Math.ceil(total / validPageSize));
		const pageClamped = Math.min(validPage, totalPages);
		if (runtime && total > 0 && pageClamped !== validPage) {
			res = runtime.listProcesses(pageClamped, validPageSize, status, order);
		}
		return {
			items: res ? res.items : [],
			page: pageClamped,
			pageSize: validPageSize,
			total,
			totalPages,
			hasPrev: pageClamped > 1,
			hasNext: pageClamped < totalPages,
		};
	}

	/** Counts retained processes, optionally restricted to one status. */
	countProcesses(queryId: number, status?: Process["status"]): number {
		return this.queries.get(queryId)?.countProcesses(status) ?? 0;
	}

	/** Returns aggregate process statistics for one query. */
	getStatistics(queryId: number) {
		return this.queries.get(queryId)?.getStatistics();
	}

	/** Returns processed and queued windows around an active or failed center. */
	getProcessingTimeline(queryId: number, size: number) {
		return this.queries.get(queryId)?.getProcessingTimeline(this.nonNegativeInteger(size, "size")) ?? [];
	}

	/** Summarizes query lifecycle and retained-process counts across the instance. */
	getSummary() {
		let totalQueries = 0;
		let runningQueries = 0;
		let pausedQueries = 0;
		let totalProcesses = 0;
		let queued = 0;
		let processing = 0;
		let processed = 0;
		let failed = 0;
		const read = { running: 0, paused: 0, failed: 0, stopped: 0, transitioning: 0 };
		const process = { running: 0, paused: 0, failed: 0, stopped: 0, transitioning: 0 };

		for (const rt of this.queries.values()) {
			totalQueries++;
			const readStatus = rt.query.read.status.name;
			const processStatus = rt.query.process.status.name;
			this.countStatus(read, readStatus);
			this.countStatus(process, processStatus);
			if (readStatus === "RUNNING" && processStatus === "RUNNING") runningQueries++;
			else if ([readStatus, processStatus].some((status) => status === "PAUSED" || status === "PAUSING")) pausedQueries++;

			totalProcesses += rt.countProcesses();
			queued += rt.countProcesses("Queued");
			processing += rt.countProcesses("Processing");
			processed += rt.countProcesses("Processed");
			failed += rt.countProcesses("Failed");
		}

		return { totalQueries, runningQueries, pausedQueries, totalProcesses, queued, processing, processed, failed, read, process };
	}

	/** Shared registry used to resolve log and transaction handlers. */
	get registry(): HandlerRegistry {
		return this._registry;
	}

	private findDuplicateQuery(
		cfg: RuntimeQueryConfig,
		excludeQueryId?: number
	): { queryId: number; busy?: QueryLifecycleState } | undefined {
		for (const rt of this.queries.values()) {
			const existing = rt.query;
			if (excludeQueryId !== undefined && existing.id === excludeQueryId) continue;
			const lifecycle = this.queryLifecycles.get(existing.id);
			if (lifecycle?.phase === "removeQuery") {
				if (this.valuesEqual(this.comparableQuery(existing), this.comparableQuery(cfg))) {
					return { queryId: existing.id, busy: lifecycle };
				}
				continue;
			}
			if (lifecycle?.phase === "updateQuery" && lifecycle.candidate) {
				if (this.valuesEqual(this.comparableQuery(existing), this.comparableQuery(cfg))) {
					return { queryId: existing.id, busy: lifecycle };
				}
				if (this.valuesEqual(this.comparableQuery(lifecycle.candidate), this.comparableQuery(cfg))) {
					return { queryId: existing.id };
				}
				continue;
			}
			if (this.valuesEqual(this.comparableQuery(existing), this.comparableQuery(cfg))) return { queryId: existing.id };
		}
		return undefined;
	}

	private comparableQuery(query: RuntimeQueryConfig): unknown {
		const { id: _id, title: _title, icon: _icon, read, process, ...rest } = query;
		const { status: _readStatus, ...readConfig } = read;
		const { status: _processStatus, ...processConfig } = process;
		return { ...rest, read: readConfig, process: processConfig };
	}

	private valuesEqual(a: unknown, b: unknown): boolean {
		if (Object.is(a, b)) return true;
		if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
		if (Array.isArray(a) || Array.isArray(b)) {
			if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
			// Empty optional positions mean undefined; array.every() would skip them.
			for (let index = 0; index < a.length; index++) {
				if (!this.valuesEqual(a[index], b[index])) return false;
			}
			return true;
		}
		const aRecord = a as Record<string, unknown>;
		const bRecord = b as Record<string, unknown>;
		const aKeys = Object.keys(aRecord).filter((key) => aRecord[key] !== undefined).sort();
		const bKeys = Object.keys(bRecord).filter((key) => bRecord[key] !== undefined).sort();
		if (aKeys.length !== bKeys.length || aKeys.some((key, index) => key !== bKeys[index])) return false;
		return aKeys.every((key) => this.valuesEqual(aRecord[key], bRecord[key]));
	}

	private toRuntimeConfig(cfg: QueryConfig): RuntimeQueryConfig {
		const cloned = snapshotValue(cfg);
		const common = {
			...cloned,
			read: {
				...cloned.read,
				fetch: {
					...cloned.read.fetch,
					pollIntervalSeconds: cloned.read.fetch.pollIntervalSeconds ?? this.defaults.pollIntervalSeconds,
					batchSize: cloned.read.fetch.batchSize ?? this.defaults.batchSize,
					maxPagesPerCycle: cloned.read.fetch.maxPagesPerCycle ?? this.defaults.maxPagesPerCycle,
					nextDelayMs: cloned.read.fetch.nextDelayMs ?? DEFAULTS.nextPageDelayMs,
				},
				consistency: {
					finalityLagSeconds: cloned.read.consistency?.finalityLagSeconds ?? this.defaults.finalityLagSeconds,
					overlapSeconds: cloned.read.consistency?.overlapSeconds ?? this.defaults.overlapSeconds,
				},
				status: { name: "RUNNING" },
			},
			process: {
				...cloned.process,
				maxQueuedProcesses: cloned.process.maxQueuedProcesses ?? this.defaults.maxQueuedProcessesPerQuery,
				holdPeriod: cloned.process.holdPeriod ?? DEFAULTS.holdPeriod,
				nextDelayMs: cloned.process.nextDelayMs ?? DEFAULTS.nextProcessDelayMs,
				status: { name: "RUNNING" },
			},
		};

		if (cloned.type === "Single-Contract-Logs") {
			return {
				...common,
				contract: { ...(cloned.contract ?? {}), address: cloned.contract?.address ?? "" },
				params: { ...cloned.params, index: cloned.params.index ?? 0 },
				process: { ...common.process, skipEventNames: cloned.process.skipEventNames ?? [] },
			} as RuntimeQueryConfig;
		}
		if (cloned.type === "Transactions") {
			return {
				...common,
				contract: cloned.contract,
				params: {
					...cloned.params,
					index: cloned.params.index ?? 0,
					result: cloned.params.result === undefined ? "success" : cloned.params.result,
				},
				process: {
					...common.process,
					skipTransactionTypes: cloned.process.skipTransactionTypes ?? [],
					unhandledTransactionPolicy: cloned.process.unhandledTransactionPolicy ?? "error",
				},
			} as RuntimeQueryConfig;
		}
		return {
			...common,
			params: { ...cloned.params, index: cloned.params.index ?? 0 },
			process: { ...common.process, skipEventNames: cloned.process.skipEventNames ?? [] },
		} as RuntimeQueryConfig;
	}

	private validateDefaults(): void {
		this.assertFinite("defaults.pollIntervalSeconds", this.defaults.pollIntervalSeconds, 1);
		this.assertPositiveInteger("defaults.batchSize", this.defaults.batchSize);
		this.assertPositiveInteger("defaults.maxPagesPerCycle", this.defaults.maxPagesPerCycle);
		this.assertPositiveInteger("defaults.maxQueuedProcessesPerQuery", this.defaults.maxQueuedProcessesPerQuery);
		this.assertFinite("defaults.finalityLagSeconds", this.defaults.finalityLagSeconds, 0);
		this.assertFinite("defaults.overlapSeconds", this.defaults.overlapSeconds, 0);
	}

	private validate(cfg: RuntimeQueryConfig): void {
		if (!Number.isSafeInteger(cfg.id) || cfg.id < 0) throw new Error(`Query ${cfg.id} invalid: id must be a non-negative safe integer`);
		if (typeof cfg.title !== "string" || !cfg.title.trim()) throw new Error(`Query ${cfg.id} invalid: title is required`);
		if (typeof cfg.read.fetch.network !== "string" || !cfg.read.fetch.network.trim()) throw new Error(`Query ${cfg.id} invalid: network is required`);
		if (typeof cfg.read.fetch.restProvider !== "string" || !cfg.read.fetch.restProvider.trim()) throw new Error(`Query ${cfg.id} invalid: restProvider is required`);

		this.assertFinite(`Query ${cfg.id} pollIntervalSeconds`, cfg.read.fetch.pollIntervalSeconds, 1);
		this.assertPositiveInteger(`Query ${cfg.id} batchSize`, cfg.read.fetch.batchSize);
		this.assertPositiveInteger(`Query ${cfg.id} maxPagesPerCycle`, cfg.read.fetch.maxPagesPerCycle);
		this.assertFinite(`Query ${cfg.id} read.nextDelayMs`, cfg.read.fetch.nextDelayMs, 0);
		this.assertFinite(`Query ${cfg.id} finalityLagSeconds`, cfg.read.consistency.finalityLagSeconds, 0);
		this.assertFinite(`Query ${cfg.id} overlapSeconds`, cfg.read.consistency.overlapSeconds, 0);
		this.assertPositiveInteger(`Query ${cfg.id} read.pauseAfterConsecutiveFailures`, cfg.read.pauseAfterConsecutiveFailures);
		this.assertPositiveInteger(`Query ${cfg.id} process.pauseAfterConsecutiveFailures`, cfg.process.pauseAfterConsecutiveFailures);
		this.assertPositiveInteger(`Query ${cfg.id} process.maxQueuedProcesses`, cfg.process.maxQueuedProcesses);
		if (cfg.process.maxQueuedProcesses < cfg.read.fetch.batchSize) {
			throw new Error(`Query ${cfg.id} invalid: process.maxQueuedProcesses must be at least read.batchSize`);
		}
		this.assertFinite(`Query ${cfg.id} process.nextDelayMs`, cfg.process.nextDelayMs, 0);
		if (!["none", "daily", "weekly", "monthly"].includes(cfg.process.holdPeriod)) throw new Error(`Query ${cfg.id} invalid: unsupported holdPeriod`);
		if (cfg.process.context !== undefined && typeof cfg.process.context !== "string") throw new Error(`Query ${cfg.id} invalid: process.context must be a string`);
		if (cfg.read.until !== undefined && typeof cfg.read.until !== "function") throw new Error(`Query ${cfg.id} invalid: until must be a function`);

		const timestamp = cfg.params.timestamp ?? 0;
		try {
			cfg.params.timestamp = normalizeHederaTimestamp(timestamp);
		} catch (error) {
			throw new Error(`Query ${cfg.id} invalid: ${errorDetails(error).message}`);
		}

		if (cfg.type !== "Transactions") {
			const index = cfg.params.index ?? 0;
			if (!Number.isSafeInteger(index) || index < 0) throw new Error(`Query ${cfg.id} invalid: index must be a non-negative safe integer`);
			this.validateTopics(cfg.id, cfg.params.topics, cfg.type === "Multi-Contract-Logs");
			this.validateAbi(cfg.id, cfg.abi);
			const topicFiltered = cfg.type === "Multi-Contract-Logs" || hasContractLogTopicFilters(cfg.params.topics);
			if (topicFiltered && !topicLogOverlapCanAdvance(cfg.read.consistency.overlapSeconds)) {
				throw new Error(`Query ${cfg.id} invalid: overlapSeconds must be shorter than the usable seven-day topic-query window`);
			}
			this.validateStringArray(cfg.id, "skipEventNames", cfg.process.skipEventNames);
		}

		if (cfg.type === "Multi-Contract-Logs") {
			if (cfg.params.contractResolver !== undefined && typeof cfg.params.contractResolver !== "function") {
				throw new Error(`Query ${cfg.id} invalid: contractResolver must be a function`);
			}
		} else if (cfg.type === "Single-Contract-Logs") {
			const address = cfg.contract.address;
			if (typeof address !== "string" || !address.trim()) {
				throw new Error(`Query ${cfg.id} invalid: contract.address is required`);
			}
		} else {
			const accountId = cfg.params.accountId;
			if (typeof accountId !== "string" || !accountId.trim()) throw new Error(`Query ${cfg.id} invalid: accountId is required`);
			const index = cfg.params.index ?? 0;
			if (!Number.isSafeInteger(index) || index < 0) throw new Error(`Query ${cfg.id} invalid: index must be a non-negative safe integer`);
			cfg.params.index = index;
			if (cfg.params.result !== undefined && cfg.params.result !== null) {
				if (typeof cfg.params.result !== "string") throw new Error(`Query ${cfg.id} invalid: result must be success, fail, or null`);
				const result = cfg.params.result.toLowerCase();
				if (result !== "success" && result !== "fail") throw new Error(`Query ${cfg.id} invalid: result must be success, fail, or null`);
				cfg.params.result = result;
			}
			this.validateStringArray(cfg.id, "skipTransactionTypes", cfg.process.skipTransactionTypes);
			if (cfg.process.unhandledTransactionPolicy !== undefined && !["error", "skip"].includes(cfg.process.unhandledTransactionPolicy)) {
				throw new Error(`Query ${cfg.id} invalid: unsupported unhandledTransactionPolicy`);
			}
		}
	}

	private validateAbi(queryId: number, abi: unknown): void {
		if (!Array.isArray(abi)) {
			throw new Error(`Query ${queryId} invalid: abi must be an array containing at least one non-anonymous event`);
		}

		let nonAnonymousEvents = 0;
		for (let index = 0; index < abi.length; index++) {
			const item = abi[index];
			if (!item || typeof item !== "object" || (item as { type?: unknown }).type !== "event") continue;

			try {
				const candidate = item as Partial<AbiEvent>;
				if (typeof candidate.name !== "string" || candidate.name.length === 0 || !Array.isArray(candidate.inputs)) {
					throw new TypeError("event name and inputs are required");
				}
				if (candidate.anonymous !== undefined && typeof candidate.anonymous !== "boolean") {
					throw new TypeError("event anonymous must be a boolean when provided");
				}
				if (candidate.inputs.some((input) => (
					input !== null
					&& typeof input === "object"
					&& "indexed" in input
					&& input.indexed !== undefined
					&& typeof input.indexed !== "boolean"
				))) {
					throw new TypeError("event input indexed must be a boolean when provided");
				}

				// An Ox format/parse round trip validates Solidity types, tuples,
				// arrays, identifiers, and parameters. Parsing the JSON object directly
				// would only prepare it and would not perform this grammar validation.
				// Constructing the selector through viem also verifies the exact
				// representation later consumed by the log decoder.
				const parsed = OxAbiEvent.from(OxAbiEvent.format(item as AbiEvent), { prepare: false }) as AbiEvent;
				if (candidate.anonymous !== true) {
					toEventSelector(parsed);
					nonAnonymousEvents++;
				}
			} catch (error) {
				const detail = `: ${errorDetails(error).message}`;
				throw new Error(`Query ${queryId} invalid: malformed event ABI item at index ${index}${detail}`);
			}
		}

		if (nonAnonymousEvents === 0) {
			throw new Error(`Query ${queryId} invalid: abi must contain at least one non-anonymous event`);
		}
	}

	private validateTopics(queryId: number, topics: readonly (ContractLogTopicFilter | undefined)[] | undefined, required: boolean): void {
		if (topics !== undefined && !Array.isArray(topics)) throw new Error(`Query ${queryId} invalid: topics must be an array`);
		if (required && (!topics || !topics.some((topic) => typeof topic === "string" ? topic.length > 0 : Array.isArray(topic) && topic.length > 0))) {
			throw new Error(`Query ${queryId} invalid: topics must contain at least one filter`);
		}
		if (!topics) return;
		if (topics.length > 4) throw new Error(`Query ${queryId} invalid: at most four topics are supported`);
		for (let index = 0; index < topics.length; index++) {
			const topic = topics[index];
			if (topic === undefined) continue;
			const values = typeof topic === "string" ? [topic] : topic;
			if (!Array.isArray(values) || values.length === 0 || values.some((value) => typeof value !== "string" || !/^(?:0x)?[0-9a-fA-F]{1,64}$/.test(value))) {
				throw new Error(`Query ${queryId} invalid: topic${index} must contain one or more valid log topics`);
			}
		}
	}

	private validateStringArray(queryId: number, name: string, value: readonly string[] | undefined): void {
		if (value === undefined) return;
		if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || !item)) {
			throw new Error(`Query ${queryId} invalid: ${name} must contain only non-empty strings`);
		}
	}

	private validateInputShape(value: unknown): asserts value is QueryConfig {
		if (!value || typeof value !== "object") throw new Error("Query config must be an object");
		const candidate = value as Record<string, unknown>;
		if (!["Multi-Contract-Logs", "Single-Contract-Logs", "Transactions"].includes(String(candidate.type))) {
			throw new Error("Query config has an unsupported type");
		}
		if (!candidate.read || typeof candidate.read !== "object" || Array.isArray(candidate.read)) throw new Error("Query config requires read settings");
		if (!candidate.process || typeof candidate.process !== "object" || Array.isArray(candidate.process)) throw new Error("Query config requires process settings");
		if (!candidate.params || typeof candidate.params !== "object" || Array.isArray(candidate.params)) throw new Error("Query config requires params");
		if (candidate.contract !== undefined && (!candidate.contract || typeof candidate.contract !== "object" || Array.isArray(candidate.contract))) {
			throw new Error("Query contract metadata must be an object");
		}
		const read = candidate.read as Record<string, unknown>;
		if (!read.fetch || typeof read.fetch !== "object" || Array.isArray(read.fetch)) throw new Error("Query config requires read.fetch settings");
	}

	private assertActive(): void {
		if (this.closed) throw new Error("HederaEventSync has been shut down");
	}

	private assertPositiveInteger(name: string, value: number): void {
		if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive safe integer`);
	}

	private assertFinite(name: string, value: number, minimum: number): void {
		if (!Number.isFinite(value) || value < minimum) throw new Error(`${name} must be a finite number >= ${minimum}`);
	}

	private positiveInteger(value: number, name: string): number {
		if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive safe integer`);
		return value;
	}

	private nonNegativeInteger(value: number, name: string): number {
		if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a non-negative safe integer`);
		return value;
	}

	private countStatus(
		counts: { running: number; paused: number; failed: number; stopped: number; transitioning: number },
		status: RuntimeQueryConfig["read"]["status"]["name"]
	): void {
		if (status === "RUNNING") counts.running++;
		else if (status === "PAUSED") counts.paused++;
		else if (status === "FAILED") counts.failed++;
		else if (status === "STOPPED") counts.stopped++;
		else counts.transitioning++;
	}

	private async shutdownInternal(): Promise<void> {
		let runtimes: QueryRuntime[] = [];
		await this.withMutationLock(async () => {
			if (this.closed) return;
			const activeLifecycle = this.queryLifecycles.entries().next().value as [number, QueryLifecycleState] | undefined;
			if (activeLifecycle) {
				throw new LifecycleBusyError("shutdown", activeLifecycle[0], activeLifecycle[1].phase);
			}
			this.closed = true;
			runtimes = [...this.queries.values()];
		});

		try {
			// QueryRuntime.stop() signals both loops before its first await. Starting
			// every stop first ensures one slow handler cannot delay cancellation of
			// the other queries.
			const stopResults = await Promise.allSettled(runtimes.map((runtime) => runtime.stop()));
			for (const runtime of runtimes) await this.hooks.onQueryRemoved?.(runtime.query);
			const failedStop = stopResults.find((result): result is PromiseRejectedResult => result.status === "rejected");
			if (failedStop) throw failedStop.reason;
		} finally {
			await this.withMutationLock(async () => {
				this.queries.clear();
				this.queryLifecycles.clear();
			});
			if (HederaEventSync._instance === this) HederaEventSync._instance = undefined;
		}
	}

	private async withQueryControl(
		queryId: number,
		phase: Extract<QueryLifecyclePhase, "pauseRead" | "resumeRead" | "pauseProcessing" | "resumeProcessing">,
		operation: (runtime: QueryRuntime) => Promise<void>
	): Promise<void> {
		let runtime: QueryRuntime | undefined;
		let lifecycle: QueryLifecycleState | undefined;
		await this.withMutationLock(async () => {
			this.assertActive();
			this.assertQueryLifecycleIdle(queryId, phase);
			runtime = this.queries.get(queryId);
			if (!runtime) return;
			lifecycle = { phase };
			this.queryLifecycles.set(queryId, lifecycle);
		});
		if (!runtime || !lifecycle) return;

		try {
			await operation(runtime);
		} finally {
			await this.finishQueryLifecycle(queryId, lifecycle);
		}
	}

	private assertQueryLifecycleIdle(queryId: number, operation: string): void {
		const active = this.queryLifecycles.get(queryId);
		if (active) throw new LifecycleBusyError(operation, queryId, active.phase);
	}

	private async finishQueryLifecycle(queryId: number, lifecycle: QueryLifecycleState): Promise<void> {
		await this.withMutationLock(async () => {
			if (this.queryLifecycles.get(queryId) === lifecycle) this.queryLifecycles.delete(queryId);
		});
	}

	private async withMutationLock<T>(operation: () => Promise<T>): Promise<T> {
		const release = await this.mutationMutex.lock();
		try {
			return await operation();
		} finally {
			release();
		}
	}
}
