import type { Process, ProcessTry, ProcessStep } from "../../types/domain";
import { compareCursor } from "../../utils/comparators";
import type { NewProcess } from "./types";

export class ProcessStore {
	private processes = new Map<number, Process>();
	private seenSourceKeys = new Map<string, { queryId: number; pointerTS: string }>();
	private seq = 1;
	private trySeq = 1;
	private stepSeq = 1;

	createProcess(p: NewProcess): Process {
		const [created] = this.createProcesses([p]);
		if (!created) throw new Error(`Duplicate process source key: ${p.sourceKey}`);
		return created;
	}

	/** Validates and commits a complete prepared page without partially inserting it. */
	createProcesses(items: readonly NewProcess[]): Process[] {
		const pendingKeys = new Set<string>();
		for (const item of items) {
			if (!item.sourceKey) throw new Error("Process sourceKey is required");
			if (pendingKeys.has(item.sourceKey)) continue;
			pendingKeys.add(item.sourceKey);
		}

		const freshKeys = new Set<string>();
		const fresh = items.filter((item) => {
			if (this.seenSourceKeys.has(item.sourceKey)) return false;
			if (freshKeys.has(item.sourceKey)) return false;
			freshKeys.add(item.sourceKey);
			return true;
		});
		const records = fresh.map((item, index) => ({ ...item, id: this.seq + index, tries: item.tries ?? [] } satisfies Process));

		this.seq += records.length;
		for (const record of records) {
			this.processes.set(record.id, record);
			this.seenSourceKeys.set(record.sourceKey, { queryId: record.queryId, pointerTS: record.pointerTS });
		}
		return records;
	}

	/** Returns the page records that are not already represented by a source key. */
	filterUnseenProcesses(items: readonly NewProcess[]): NewProcess[] {
		const freshKeys = new Set<string>();
		return items.filter((item) => {
			if (this.seenSourceKeys.has(item.sourceKey) || freshKeys.has(item.sourceKey)) return false;
			freshKeys.add(item.sourceKey);
			return true;
		});
	}

	pruneSourceKeysBefore(queryId: number, timestamp: string): void {
		for (const [key, source] of this.seenSourceKeys) {
			if (source.queryId === queryId && compareCursor({ timestamp: source.pointerTS, index: 0 }, { timestamp, index: 0 }) < 0) {
				this.seenSourceKeys.delete(key);
			}
		}
	}

	get(id: number): Process | undefined {
		return this.processes.get(id);
	}

	delete(id: number): boolean {
		return this.processes.delete(id);
	}

	set(p: Process): void {
		this.processes.set(p.id, p);
	}

	createTry(procId: number, t: Omit<ProcessTry, "id" | "processId">): ProcessTry {
		return { ...t, id: this.trySeq++, processId: procId };
	}

	createStep(tryId: number, s: Omit<ProcessStep, "id" | "tryId">): ProcessStep {
		return { ...s, id: this.stepSeq++, tryId };
	}

	listByQuery(
		queryId: number,
		{ page, pageSize, status, order = "asc" }: { page: number; pageSize: number; status?: Process["status"]; order?: "asc" | "desc" }
	): { items: Process[]; total: number } {
		const filtered = [...this.processes.values()].filter((p) => p.queryId === queryId && (!status || p.status === status));

		const cmp = (a: Process, b: Process) => {
			return compareCursor(
				{ timestamp: a.pointerTS, index: a.pointerIDX },
				{ timestamp: b.pointerTS, index: b.pointerIDX }
			);
		};

		filtered.sort((a, b) => (order === "asc" ? cmp(a, b) : cmp(b, a)));

		const total = filtered.length;
		const start = (page - 1) * pageSize;
		const items = filtered.slice(start, start + pageSize);

		return { items, total };
	}

	countByQuery(queryId: number, status?: Process["status"]): number {
		let n = 0;
		for (const p of this.processes.values()) {
			if (p.queryId !== queryId) continue;
			if (status && p.status !== status) continue;
			n++;
		}
		return n;
	}

	getFailed(queryId: number): Process | undefined {
		// At any point we expect at most one Failed process per query; return the latest if present.
		let candidate: Process | undefined;
		for (const p of this.processes.values()) {
			if (p.queryId !== queryId) continue;
			if (p.status !== "Failed") continue;
			if (!candidate || p.updatedAt > candidate.updatedAt) candidate = p;
		}
		return candidate;
	}

	getStatistics(queryId: number): { total: number; queued: number; processing: number; processed: number; failed: number } {
		let total = 0;
		let queued = 0;
		let processing = 0;
		let processed = 0;
		let failed = 0;
		for (const p of this.processes.values()) {
			if (p.queryId !== queryId) continue;
			total++;
			if (p.status === "Queued") queued++;
			else if (p.status === "Processing") processing++;
			else if (p.status === "Processed") processed++;
			else if (p.status === "Failed") failed++;
		}
		return { total, queued, processing, processed, failed };
	}

	getProcessingTimeline(queryId: number, size: number): Process[] {
		if (size <= 0) return [];
		const all = [...this.processes.values()].filter((p) => p.queryId === queryId);
		const center = all.find((p) => p.status === "Processing") ?? all.find((p) => p.status === "Failed");

		const cmp = (a: Process, b: Process) => {
			return compareCursor(
				{ timestamp: a.pointerTS, index: a.pointerIDX },
				{ timestamp: b.pointerTS, index: b.pointerIDX }
			);
		};

		const queued = all.filter((p) => p.status === "Queued" && (!center || p.id !== center.id)).sort(cmp);
		const processed = all.filter((p) => p.status === "Processed" && (!center || p.id !== center.id)).sort(cmp);

		// For queued items we want the next ones that will be processed (earliest first),
		// not the farthest-out entries. Grab from the head of the sorted array.
		const queuedSlice = queued.slice(0, size);
		const processedSlice = processed.slice(-size);

		const combined = [...queuedSlice, ...(center ? [center] : []), ...processedSlice];
		combined.sort(cmp);
		return combined;
	}

	clear(): void {
		this.processes.clear();
		this.seenSourceKeys.clear();
		this.seq = 1;
		this.trySeq = 1;
		this.stepSeq = 1;
	}
}
