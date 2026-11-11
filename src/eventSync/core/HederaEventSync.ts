import type { ContractLogTopicFilter, NetworkName, QueryConfig, QueryStatus, QueryStartState, RuntimeQueryConfig } from "../types/config";
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
import { normalizeTransactionType } from "../utils/transactionType";
import { snapshotValue } from "../utils/snapshot";
import { assertOutsideUserCallback, runInUserCallback } from "../utils/userCallback";
import { errorDetails } from "../utils/error";
import { LifecycleBusyError, LifecycleTimeoutError } from "../errors";
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
	/** Exact contract metadata type; null matches absent or blank types, undefined matches all. */
	contractType?: string | null | undefined;
	/** Effective reader status, including global holds. Arrays match any listed status; an empty array matches none. */
	readStatus?: QueryStatus["name"] | readonly QueryStatus["name"][] | undefined;
	/** Effective processor status, combined with the other filters using AND. Arrays match any listed status. */
	processStatus?: QueryStatus["name"] | readonly QueryStatus["name"][] | undefined;
	/** Trimmed, case-insensitive literal substring of title, query ID, configured contract ID/address, or transaction account ID. Blank searches match all. */
	search?: string | undefined;
	/** Order of results. Default: "insertion" (Map insertion order). */
	order?: "insertion" | "id-asc" | "id-desc";
};

type QueryListFilters = {
	type: QueryConfig["type"] | undefined;
	network: NetworkName | undefined;
	contractType: string | null | undefined;
	readStatuses: ReadonlySet<QueryStatus["name"]> | undefined;
	processStatuses: ReadonlySet<QueryStatus["name"]> | undefined;
	search: string | undefined;
};

export type { QueryStartState } from "../types/config";

export type LifecycleOperationOptions = { timeoutMs?: number };
export type GlobalPauseScope = "read" | "process" | "both";
export type GlobalPauseOptions = LifecycleOperationOptions & { scope?: GlobalPauseScope };
export type LifecycleOperation = "start" | "pauseAll" | "resumeAll" | "restartQueries" | "reloadQueries" | "shutdown";
export type LifecycleState = Readonly<{
	started: boolean;
	closed: boolean;
	generation: number;
	/** True only when both stages are globally held, including maintenance. */
	globalPaused: boolean;
	/** Effective global holds, independent of each query's own pause reasons. */
	globalReadPaused: boolean;
	globalProcessPaused: boolean;
	/** Internal replacement/shutdown hold; scoped resume cannot release it. */
	maintenanceHeld: boolean;
	/** A retired/partially initialized generation needs a successful restart or reload before release. */
	recoveryRequired: boolean;
	operation?: Readonly<{ name: LifecycleOperation; startedAt: number; timedOut: boolean; scope?: GlobalPauseScope }>;
	lastError?: string;
}>;
export type QuerySourceContext = Readonly<{ signal: AbortSignal; reason: "start" | "reload" }>;
export type QuerySource = (context: QuerySourceContext) => readonly QueryConfig[] | Promise<readonly QueryConfig[]>;
export type BeforeReplaceContext = Readonly<{
	signal: AbortSignal;
	reason: "restart" | "reload" | "shutdown";
	checkpoints: readonly Readonly<{ query: RuntimeQueryConfig; checkpoint: Readonly<ProcessedCheckpoint> }>[];
}>;

/** Opt-in retry of failure-limit pauses; intentional and terminal pauses are excluded. */
export type FailureRecoveryOptions = {
	enabled: boolean;
	/** Positive whole seconds, up to 2147483647. Defaults to one hour. */
	delaySeconds?: number;
};

/** Construction options for the synchronization engine. */
export type HederaEventSyncOptions = {
	/** Configured REST client used by every query runtime in this instance. */
	restClient: HederaRestClient;
	/** Complete authoritative query catalog. Invoked only by start() and reloadQueries(). */
	querySource: QuerySource;
	/** Await durable checkpoints before retiring a generation. Rejection leaves the old runtimes held. */
	beforeReplace?: (context: BeforeReplaceContext) => void | Promise<void>;
	/**
	 * Keeps `create()` process-wide by default. While the guarded instance is
	 * active, later calls return it and do not apply their supplied options. A
	 * guarded call made during shutdown waits for the one replacement instance.
	 * Set to `false` only when intentionally managing isolated instances.
	 */
	singletonGuard?: boolean;
	/** Controls runtime behavior after a hook rejects. Defaults to "continue". */
	hookFailurePolicy?: HookFailurePolicy;
	/** Disabled unless explicitly enabled. Reading and processing retry independently. */
	failureRecovery?: FailureRecoveryOptions;
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

type QueryLifecyclePhase = "pauseRead" | "resumeRead" | "pauseProcessing" | "resumeProcessing";
type QueryLifecycleState = { phase: QueryLifecyclePhase };

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
	/** Underlying shutdown work, which can outlive the caller-visible deadline. */
	private shutdownCompletion?: Promise<void>;
	private closed = false;
	private started = false;
	private generation = 0;
	private globalReadPaused = false;
	private globalProcessPaused = false;
	private maintenanceHeld = false;
	private recoveryRequired = false;
	private instanceOperation?: { name: LifecycleOperation; startedAt: number; timedOut: boolean; scope?: GlobalPauseScope };
	private lastLifecycleError?: string;
	private readonly catalog = new Map<number, RuntimeQueryConfig>();
	private readonly querySource: QuerySource;
	private readonly beforeReplace?: HederaEventSyncOptions["beforeReplace"];
	private readonly failureRecoveryDelayMs?: number;
	private failureRecoveryTimer?: ReturnType<typeof setTimeout>;
	private failureRecoveryHolds = 0;


	private readonly _registry = new HandlerRegistry();
	private readonly queries = new Map<number, QueryRuntime>();

	private readonly hooks: CompositeHooks;

	/** Replaces the hook observers used for subsequently dispatched events. */
	setHooks(h: SyncHooks | SyncHooks[]): void {
		if (this.instanceOperation) throw new LifecycleBusyError("setHooks", -1, this.instanceOperation.name);
		this.hooks.setHooks(Array.isArray(h) ? h : [h]);
	}

	private constructor(opts: HederaEventSyncOptions) {
		this.querySource = opts.querySource;
		this.beforeReplace = opts.beforeReplace;
		const recovery = opts.failureRecovery;
		if (recovery !== undefined) {
			if (!recovery || typeof recovery !== "object" || Array.isArray(recovery) || typeof recovery.enabled !== "boolean") {
				throw new Error("failureRecovery must have a boolean enabled property");
			}
			const delay = recovery.delaySeconds === undefined ? 3600 : recovery.delaySeconds;
			if (!Number.isSafeInteger(delay) || delay < 1 || delay > 2_147_483_647) {
				throw new Error("failureRecovery.delaySeconds must be an integer between 1 and 2147483647");
			}
			if (recovery.enabled) this.failureRecoveryDelayMs = delay * 1000;
		}
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
		if (typeof opts.querySource !== "function") throw new Error("querySource must be a function");
		const guarded = opts.singletonGuard !== false;
		if (guarded) {
			for (;;) {
				const current = HederaEventSync._instance;
				if (!current) break;
				const shutdown = current.shutdownCompletion ?? current.shutdownPromise;
				if (!shutdown) return current;
				assertOutsideUserCallback("create", current.callbackOwner);
				// Keep the singleton occupied until shutdown has fully settled. A
				// concurrent create then competes for the one replacement instance.
				await shutdown.catch(() => {});
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

	/** Loads the initial catalog; construction alone never starts callbacks or network work. */
	async start(options: LifecycleOperationOptions = {}): Promise<void> {
		return this.runInstanceOperation("start", options, async (signal) => {
			if (this.started) return;
			const candidate = await this.loadCatalog("start", signal);
			await this.replaceCatalog(candidate, "start", signal);
		});
	}

	/** Holds the selected stages across all queries; omitted scope holds both. */
	async pauseAll(options: GlobalPauseOptions = {}): Promise<void> {
		const scope = this.globalControlScope(options);
		return this.runInstanceOperation("pauseAll", options, async () => {
			if (scope !== "process") this.globalReadPaused = true;
			if (scope !== "read") this.globalProcessPaused = true;
			await this.holdRuntimes([...this.queries.values()], scope);
		}, scope);
	}

	/** Releases only the selected global holds. Independent query pauses remain. */
	async resumeAll(options: GlobalPauseOptions = {}): Promise<void> {
		const scope = this.globalControlScope(options);
		return this.runInstanceOperation("resumeAll", options, async (signal) => {
			signal.throwIfAborted();
			if (this.recoveryRequired || this.maintenanceHeld) throw new Error("The previous replacement did not finish; restart or reload queries before resuming");
			if (scope !== "process") this.globalReadPaused = false;
			if (scope !== "read") this.globalProcessPaused = false;
			this.applyGlobalHolds(this.queries.values());
		}, scope);
	}

	/** Rebuilds the accepted catalog from final processed checkpoints, without invoking the source. */
	async restartQueries(options: LifecycleOperationOptions = {}): Promise<void> {
		return this.runInstanceOperation("restartQueries", options, async (signal) => {
			this.assertStarted();
			await this.replaceCatalog([...this.catalog.values()].map((query) => snapshotValue(query)), "restart", signal);
		});
	}

	/** Validates a fresh complete source snapshot before holding any current work. */
	async reloadQueries(options: LifecycleOperationOptions = {}): Promise<void> {
		return this.runInstanceOperation("reloadQueries", options, async (signal) => {
			this.assertStarted();
			const candidate = await this.loadCatalog("reload", signal);
			await this.replaceCatalog(candidate, "reload", signal);
		});
	}

	getLifecycleState(): LifecycleState {
		const globalReadPaused = this.globalReadPaused || this.maintenanceHeld;
		const globalProcessPaused = this.globalProcessPaused || this.maintenanceHeld;
		return Object.freeze({ started: this.started, closed: this.closed, generation: this.generation, recoveryRequired: this.recoveryRequired,
			globalPaused: globalReadPaused && globalProcessPaused, globalReadPaused, globalProcessPaused, maintenanceHeld: this.maintenanceHeld,
			...(this.instanceOperation ? { operation: Object.freeze({ ...this.instanceOperation }) } : {}),
			...(this.lastLifecycleError ? { lastError: this.lastLifecycleError } : {}) });
	}

	/**
	 * Defer automatic retries while application-owned management work settles.
	 * This does not pause active work or block explicit lifecycle controls. Call
	 * the idempotent release function in finally; overlapping holds compose.
	 */
	holdFailureRecovery(): () => void {
		this.assertActive();
		this.failureRecoveryHolds++;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.failureRecoveryHolds--;
		};
	}

	/** Validates and normalizes without registering a runtime, calling hooks, or making network requests. */
	validateQuery(cfg: QueryConfig, initialState: QueryStartState = cfg?.initialState ?? {}): RuntimeQueryConfig {
		this.assertActive();
		this.validateInputShape(cfg);
		const resolved = this.toRuntimeConfig(cfg);
		this.validate(resolved);
		if (!initialState || typeof initialState !== "object" || Array.isArray(initialState)) throw new Error("initialState must be an object");
		for (const kind of ["read", "process"] as const) {
			const state = initialState[kind];
			if (state === undefined) continue;
			if (!state || typeof state !== "object" || !["RUNNING", "PAUSED"].includes(state.name) || (state.info !== undefined && typeof state.info !== "string")) {
				throw new Error(`initialState.${kind} must have a RUNNING or PAUSED name and optional string info`);
			}
			resolved[kind].status = { name: state.name, ...(state.name === "PAUSED" && state.info !== undefined ? { info: state.info } : {}) } as QueryStatus;
		}
		return resolved;
	}

	/** Quiesces work and confirms durable checkpoints before disposing runtime state. */
	async shutdown(options: LifecycleOperationOptions = {}): Promise<void> {
		assertOutsideUserCallback("shutdown", this.callbackOwner);
		if (this.closed) return;
		if (!this.shutdownPromise) {
			this.shutdownPromise = this.runInstanceOperation("shutdown", options, async (signal) => {
				this.globalReadPaused = true;
				this.globalProcessPaused = true;
				this.maintenanceHeld = true;
				try {
					const runtimes = [...this.queries.values()];
					await this.holdRuntimes(runtimes);
					signal.throwIfAborted();
					await this.persistReplacementCheckpoints(runtimes, "shutdown", signal);
					signal.throwIfAborted();
					this.recoveryRequired = true;
					await this.stopRuntimes(runtimes);
					this.queries.clear();
					this.catalog.clear();
					this.closed = true;
					this.recoveryRequired = false;
					if (HederaEventSync._instance === this) HederaEventSync._instance = undefined;
				} finally { this.maintenanceHeld = this.recoveryRequired; }
			});
		}
		try { await this.shutdownPromise; }
		catch (error) { this.shutdownPromise = undefined; throw error; }
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
		const filters = this.normalizeQueryListFilters(opts);

		// In insertion order, collect only the requested slice to avoid copying every query.
		let total = 0;
		const items: RuntimeQueryConfig[] = [];

		if (order === "insertion") {
			let seen = 0;
			for (const rt of this.queries.values()) {
				if (!this.matchesQueryFilters(rt, filters)) continue;
				if (seen >= start && seen < endExclusive) {
					items.push(rt.getQuerySnapshot());
				}
				seen++;
			}
			total = seen;
		} else {
			// Sorting IDs avoids materializing full query snapshots outside the requested page.
			const ids: number[] = [];
			for (const id of this.queries.keys()) {
				const q = this.queries.get(id)!;
				if (this.matchesQueryFilters(q, filters)) {
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
					if (!this.matchesQueryFilters(rt, filters)) continue;
					if (seen >= newStart && seen < newEndExclusive) {
						items.push(rt.getQuerySnapshot());
					}
					seen++;
				}
			} else {
				const ids: number[] = [];
				for (const id of this.queries.keys()) {
					const q = this.queries.get(id)!;
					if (this.matchesQueryFilters(q, filters)) {
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

	private normalizeQueryListFilters(opts?: ListQueriesOptions): QueryListFilters {
		if (opts?.search !== undefined && typeof opts.search !== "string") {
			throw new TypeError("listQueries search must be a string");
		}
		return {
			type: opts?.type,
			network: opts?.network,
			contractType: opts?.contractType,
			readStatuses: this.normalizeQueryStatusFilter(opts?.readStatus, "readStatus"),
			processStatuses: this.normalizeQueryStatusFilter(opts?.processStatus, "processStatus"),
			search: opts?.search?.trim().toLowerCase() || undefined,
		};
	}

	private normalizeQueryStatusFilter(value: unknown, field: "readStatus" | "processStatus"): ReadonlySet<QueryStatus["name"]> | undefined {
		if (value === undefined) return undefined;
		const statuses: readonly string[] = ["PAUSED", "RUNNING", "PAUSING", "RESUMING", "FAILED", "STOPPED"];
		const values = typeof value === "string" ? [value] : value;
		const message = `listQueries ${field} must be a status or an array of statuses: ${statuses.join(", ")}`;
		if (!Array.isArray(values)) throw new TypeError(message);
		const normalized = new Set<QueryStatus["name"]>();
		for (const status of values) {
			if (typeof status !== "string" || !statuses.includes(status)) throw new TypeError(message);
			normalized.add(status as QueryStatus["name"]);
		}
		return normalized;
	}

	private matchesQueryFilters(runtime: QueryRuntime, filters: QueryListFilters): boolean {
		const query = runtime.query;
		if (filters.type && query.type !== filters.type) return false;
		if (filters.network && query.read.fetch.network !== filters.network) return false;
		if (filters.contractType !== undefined) {
			const actualType = query.contract?.type;
			const matches = filters.contractType === null
				? actualType == null || actualType.trim().length === 0
				: actualType === filters.contractType;
			if (!matches) return false;
		}
		// Use the same effective state as returned snapshots, rather than the
		// underlying per-query state which does not include instance-wide holds.
		if (filters.readStatuses && !filters.readStatuses.has(runtime.getReadStatus().status.name)) return false;
		if (filters.processStatuses && !filters.processStatuses.has(runtime.getQueueStatus().status.name)) return false;
		if (filters.search) {
			const search = filters.search;
			const fields = [query.title, String(query.id), query.contract?.address, query.contract?.id,
				query.type === "Transactions" ? query.params.accountId : undefined];
			if (!fields.some(value => typeof value === "string" && value.toLowerCase().includes(search))) return false;
		}
		return true;
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
	async pauseRead(queryId: number, options: { preserveExistingPause?: boolean } = {}): Promise<void> {
		assertOutsideUserCallback("pauseRead", this.callbackOwner);
		await this.withQueryControl(queryId, "pauseRead", (runtime) => runtime.pauseRead(options.preserveExistingPause === true));
	}

	/** Resumes ingestion for a paused query. */
	async resumeRead(queryId: number): Promise<void> {
		assertOutsideUserCallback("resumeRead", this.callbackOwner);
		await this.withQueryControl(queryId, "resumeRead", (runtime) => runtime.resumeRead());
	}

	/** Pauses the processing queue after any active handler settles. */
	async pauseProcessing(queryId: number, options: { preserveExistingPause?: boolean } = {}): Promise<void> {
		assertOutsideUserCallback("pauseProcessing", this.callbackOwner);
		await this.withQueryControl(queryId, "pauseProcessing", (runtime) => runtime.pauseProcessing(options.preserveExistingPause === true));
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
			const readStatus = rt.getReadStatus().status.name;
			const processStatus = rt.getQueueStatus().status.name;
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
			if (cfg.params.logFilter !== undefined && typeof cfg.params.logFilter !== "function") {
				throw new Error(`Query ${cfg.id} invalid: logFilter must be a function`);
			}
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
			try {
				const transactionType = normalizeTransactionType(cfg.params.transactionType);
				if (transactionType !== undefined) cfg.params.transactionType = transactionType;
			} catch (error) {
				throw new Error(`Query ${cfg.id} invalid: ${errorDetails(error).message}`);
			}
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

	private assertStarted(): void {
		if (!this.started) throw new Error("Call start() before reloading or restarting queries");
	}

	private async loadCatalog(reason: "start" | "reload", signal: AbortSignal): Promise<RuntimeQueryConfig[]> {
		const source = await runInUserCallback({ kind: "querySource", name: "querySource", queryId: null },
			() => this.querySource(Object.freeze({ reason, signal })), this.callbackOwner);
		signal.throwIfAborted();
		if (!Array.isArray(source)) throw new Error("querySource must return an array of query configurations");
		const ids = new Set<number>();
		const candidate: RuntimeQueryConfig[] = [];
		// Validate every position: Array.map would skip holes and pass an invalid
		// catalog into replacement after the existing runtimes have been held.
		for (let index = 0; index < source.length; index++) {
			const query = this.validateQuery(source[index]);
			if (ids.has(query.id)) throw new Error(`Query ${query.id} occurs more than once in querySource`);
			ids.add(query.id);
			const existing = this.catalog.get(query.id);
			if (existing && !this.valuesEqual(this.sourceIdentity(existing), this.sourceIdentity(query))) {
				throw new Error(`Query ${query.id} changes its source identity; use a new query ID for a different source`);
			}
			candidate.push(query);
		}
		return candidate;
	}

	private sourceIdentity(query: RuntimeQueryConfig): unknown {
		return { type: query.type, network: query.read.fetch.network,
			address: query.type === "Single-Contract-Logs" ? query.contract.address.toLowerCase() : undefined,
			...(query.type === "Transactions" ? { accountId: query.params.accountId, result: query.params.result, transactionType: query.params.transactionType }
				: { topics: query.params.topics }) };
	}

	private globalControlScope(options: GlobalPauseOptions): GlobalPauseScope {
		if (!options || typeof options !== "object" || Array.isArray(options)) throw new Error("Global pause options must be an object");
		const scope = options.scope === undefined ? "both" : options.scope;
		if (scope !== "read" && scope !== "process" && scope !== "both") throw new Error("scope must be read, process, or both");
		return scope;
	}

	private applyGlobalHolds(runtimes: Iterable<QueryRuntime>): void {
		for (const runtime of runtimes) {
			runtime.setLifecycleHold(this.globalReadPaused || this.maintenanceHeld, "read");
			runtime.setLifecycleHold(this.globalProcessPaused || this.maintenanceHeld, "process");
		}
	}

	private async holdRuntimes(runtimes: readonly QueryRuntime[], scope: GlobalPauseScope = "both"): Promise<void> {
		// Set every gate before awaiting one callback, so a blocked query cannot leave others running.
		for (const runtime of runtimes) runtime.setLifecycleHold(true, scope);
		const settled = await Promise.allSettled(runtimes.map((runtime) => runtime.settleLifecycleHold(scope)));
		const failure = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
		if (failure) throw failure.reason;
	}

	private async persistReplacementCheckpoints(runtimes: readonly QueryRuntime[], reason: BeforeReplaceContext["reason"], signal: AbortSignal): Promise<void> {
		if (!this.beforeReplace) return;
		const checkpoints = runtimes.map((runtime) => Object.freeze({
			query: runtime.getQuerySnapshot(), checkpoint: Object.freeze(runtime.getProcessedCheckpoint()) }));
		await runInUserCallback({ kind: "checkpoint", name: "beforeReplace", queryId: null },
			() => this.beforeReplace!(Object.freeze({ reason, signal, checkpoints: Object.freeze(checkpoints) })), this.callbackOwner);
	}

	private async stopRuntimes(runtimes: readonly QueryRuntime[], announce = true): Promise<void> {
		const settled = await Promise.allSettled(runtimes.map((runtime) => runtime.stop()));
		const failure = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
		if (failure) throw failure.reason;
		if (announce) for (const runtime of runtimes) await this.hooks.onQueryRemoved?.(runtime.query);
	}

	private async replaceCatalog(candidate: RuntimeQueryConfig[], reason: "start" | "restart" | "reload", signal: AbortSignal): Promise<void> {
		signal.throwIfAborted();
		const oldRuntimes = [...this.queries.values()];
		const staged = new Map<number, QueryRuntime>();
		let installed = false;
		try {
			this.maintenanceHeld = true;
			await this.holdRuntimes(oldRuntimes);
			signal.throwIfAborted();
			for (const config of candidate) {
				const resolved = snapshotValue(config);
				const old = this.queries.get(config.id);
				if (old) Object.assign(resolved.params, old.getProcessedCheckpoint());
				const runtime = new QueryRuntime(resolved, this.rest, this._registry, this.hooks, this.callbackOwner, this.failureRecoveryDelayMs);
				runtime.setLifecycleHold(true);
				if (old) runtime.restoreControlState(old.getControlState());
				staged.set(config.id, runtime);
			}
			if (reason !== "start") await this.persistReplacementCheckpoints(oldRuntimes, reason, signal);
			signal.throwIfAborted();
			this.recoveryRequired = true;
			await this.stopRuntimes(oldRuntimes);
			// Even if the caller's deadline expired while stop hooks settled, install
			// the prepared, held runtimes. The disposed generation cannot be resumed.
			this.queries.clear();
			for (const [id, runtime] of staged) this.queries.set(id, runtime);
			installed = true;
			this.catalog.clear();
			for (const config of candidate) this.catalog.set(config.id, snapshotValue(config));
			this.started = true;
			this.generation++;
			signal.throwIfAborted();
			for (const runtime of staged.values()) {
				await this.hooks.onQueryAdded?.(runtime.query);
				signal.throwIfAborted();
				await runtime.start();
				signal.throwIfAborted();
				if ([runtime.getReadStatus().status.name, runtime.getQueueStatus().status.name].includes("FAILED")) {
					throw new Error(`Query ${runtime.query.id} failed during initialization; the instance remains paused`);
				}
			}
			this.recoveryRequired = false;
			this.maintenanceHeld = false;
			this.applyGlobalHolds(staged.values());
		} catch (error) {
			// Never reactivate work after timeout or partial replacement. Before commit,
			// the original catalog and queues remain inspectable; after it, restart can retry.
			// Preserve the existing failure contract: both stages remain globally
			// paused after a failed replacement, including after a successful retry.
			this.globalReadPaused = true;
			this.globalProcessPaused = true;
			this.maintenanceHeld = this.recoveryRequired;
			if (!installed) for (const runtime of staged.values()) runtime.discardUnstarted();
			throw error;
		}
	}

	private async runInstanceOperation(name: LifecycleOperation, options: LifecycleOperationOptions,
		work: (signal: AbortSignal) => Promise<void>, scope?: GlobalPauseScope): Promise<void> {
		assertOutsideUserCallback(name, this.callbackOwner);
		this.assertActive();
		if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1 || options.timeoutMs > 2_147_483_647)) {
			throw new Error("timeoutMs must be an integer between 1 and 2147483647");
		}
		if (this.instanceOperation) throw new LifecycleBusyError(name, -1, this.instanceOperation.name);
		const activeQuery = this.queryLifecycles.entries().next().value as [number, QueryLifecycleState] | undefined;
		if (activeQuery) throw new LifecycleBusyError(name, activeQuery[0], activeQuery[1].phase);
		const operation = { name, startedAt: Date.now(), timedOut: false, ...(scope === undefined ? {} : { scope }) };
		this.instanceOperation = operation;
		if (this.failureRecoveryTimer) clearTimeout(this.failureRecoveryTimer);
		this.failureRecoveryTimer = undefined;
		this.lastLifecycleError = undefined;
		const controller = new AbortController();
		let timer: ReturnType<typeof setTimeout> | undefined;
		const pending = Promise.resolve().then(() => work(controller.signal)).catch((error) => {
			this.lastLifecycleError = errorDetails(error).message;
			throw error;
		}).finally(() => {
			if (timer) clearTimeout(timer);
			if (this.instanceOperation === operation) this.instanceOperation = undefined;
			if (name === "shutdown") this.shutdownCompletion = undefined;
			this.scheduleFailureRecovery();
		});
		if (name === "shutdown") this.shutdownCompletion = pending;
		if (options.timeoutMs === undefined) return pending;
		const deadline = new Promise<never>((_, reject) => {
			timer = setTimeout(() => {
				operation.timedOut = true;
				const error = new LifecycleTimeoutError(name, options.timeoutMs!);
				this.lastLifecycleError = error.message;
				controller.abort(error);
				reject(error);
			}, options.timeoutMs);
		});
		return Promise.race([pending, deadline]);
	}

	/** One instance timer; runtime identity and eligibility are rechecked under the control gate. */
	private scheduleFailureRecovery(): void {
		if (this.failureRecoveryDelayMs === undefined || !this.started || this.closed || this.failureRecoveryTimer || this.instanceOperation) return;
		this.failureRecoveryTimer = setTimeout(() => {
			this.failureRecoveryTimer = undefined;
			void this.recoverFailurePauses().finally(() => this.scheduleFailureRecovery());
		}, 1000);
		this.failureRecoveryTimer.unref();
	}

	private async recoverFailurePauses(): Promise<void> {
		if (this.failureRecoveryHolds > 0) return;
		for (const [id, expected] of this.queries) {
			for (const kind of ["read", "process"] as const) {
				const eligible = (runtime: QueryRuntime) => {
					if (this.closed || this.instanceOperation || this.maintenanceHeld || this.recoveryRequired || this.failureRecoveryHolds > 0 || runtime !== expected) return false;
					if (kind === "read" ? this.globalReadPaused : this.globalProcessPaused) return false;
					const status = kind === "read" ? runtime.getReadStatus().status : runtime.getQueueStatus().status;
					return status.name === "PAUSED" && status.failureRecovery !== undefined && status.failureRecovery.retryAt <= Date.now();
				};
				if (!eligible(expected) || this.queryLifecycles.has(id)) continue;
				try {
					await this.withQueryControl(id, kind === "read" ? "resumeRead" : "resumeProcessing", async runtime => {
						if (!eligible(runtime)) return;
						if (kind === "read") await runtime.resumeRead(); else await runtime.resumeProcessing();
					});
				} catch (error) {
					if (!(error instanceof LifecycleBusyError)) this.lastLifecycleError = `Automatic failure recovery: ${errorDetails(error).message}`;
				}
			}
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
			if (this.instanceOperation) throw new LifecycleBusyError(phase, queryId, this.instanceOperation.name);
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
