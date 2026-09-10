import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { ProcessStore } from "../src/eventSync/core/queue/ProcessStore";
import type { NewProcess } from "../src/eventSync/core/queue/types";

function process(sourceKey: string, timestamp: string): NewProcess {
	return {
		queryId: 1,
		sourceKey,
		eventName: "Ping",
		data: {},
		rawData: {},
		pointerTS: timestamp,
		pointerIDX: 0,
		status: "Queued",
		timeOfEmit: 1,
		createdAt: 1,
		updatedAt: 1,
	};
}

describe("ProcessStore source idempotency", () => {
	it("commits complete batches and suppresses duplicate source records", () => {
		const store = new ProcessStore();
		const first = store.createProcesses([
			process("log:1:a", "10.000000000"),
			process("log:1:b", "11.000000000"),
			process("log:1:a", "10.000000000"),
		]);
		const replay = store.createProcesses([
			process("log:1:a", "10.000000000"),
			process("log:1:c", "12.000000000"),
		]);

		assert.deepEqual(first.map((item) => item.sourceKey), ["log:1:a", "log:1:b"]);
		assert.deepEqual(replay.map((item) => item.sourceKey), ["log:1:c"]);
		assert.equal(store.countByQuery(1), 3);
	});

	it("retains dedupe keys after process deletion until the overlap floor passes", () => {
		const store = new ProcessStore();
		const [created] = store.createProcesses([process("log:1:a", "10.000000000")]);
		store.delete(created.id);
		assert.equal(store.createProcesses([process("log:1:a", "10.000000000")]).length, 0);

		store.pruneSourceKeysBefore(1, "11.000000000");
		assert.equal(store.createProcesses([process("log:1:a", "10.000000000")]).length, 1);
	});

	it("validates a page before committing any item", () => {
		const store = new ProcessStore();
		assert.throws(
			() => store.createProcesses([process("log:1:a", "10.000000000"), process("", "11.000000000")]),
			/sourceKey is required/
		);
		assert.equal(store.countByQuery(1), 0);
	});

	it("reports every process status in statistics", () => {
		const store = new ProcessStore();
		const statuses = ["Queued", "Processing", "Processed", "Failed"] as const;
		for (const [index, status] of statuses.entries()) {
			store.createProcess({ ...process(`statistics-${status}`, `${index + 1}.000000000`), status });
		}
		assert.deepEqual(store.getStatistics(1), { total: 4, queued: 1, processing: 1, processed: 1, failed: 1 });
	});
});
