import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { RuntimeQueryConfig } from "../src/eventSync/types/config";
import type { Cursor } from "../src/eventSync/types/domain";
import type { SyncReadItem } from "../src/eventSync/types/mirror";
import { ReadJob } from "../src/eventSync/core/jobs/ReadJob";
import { CompositeHooks } from "../src/eventSync/hooks/CompositeHooks";

function query(until: (cursor: Readonly<Cursor>) => boolean): Extract<RuntimeQueryConfig, { type: "Multi-Contract-Logs" }> {
	return {
		type: "Multi-Contract-Logs",
		id: 51,
		title: "read transition test",
		read: {
			fetch: { network: "testnet", restProvider: "public", pollIntervalSeconds: 1, batchSize: 1, maxPagesPerCycle: 1, nextDelayMs: 0 },
			consistency: { finalityLagSeconds: 0, overlapSeconds: 0 },
			pauseAfterConsecutiveFailures: 1,
			until,
			status: { name: "RUNNING" },
		},
		params: { topics: ["0x1"], timestamp: "1.000000000", index: 0 },
		abi: [{ type: "event", name: "Ping", anonymous: false, inputs: [] }],
		process: { pauseAfterConsecutiveFailures: 1, maxQueuedProcesses: 1, holdPeriod: "none", nextDelayMs: 0, skipEventNames: [], status: { name: "RUNNING" } },
	};
}

class ImmediateReadJob extends ReadJob {
	protected async cycle(): Promise<SyncReadItem[]> {
		return [];
	}

	protected async waitWithAbort(): Promise<void> {}
}

class YieldingReadJob extends ReadJob {
	protected async cycle(): Promise<SyncReadItem[]> {
		return [];
	}

	protected async waitWithAbort(): Promise<void> {
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
}

type ReadStep = { readFailure?: boolean; replayPending?: boolean; untilFailure?: boolean };

class ScriptedReplayReadJob extends YieldingReadJob {
	steps: ReadStep[] = [];
	currentStep?: ReadStep;
	private nextStep = 0;

	protected async cycle(): Promise<SyncReadItem[]> {
		this.currentStep = this.steps[this.nextStep++];
		if (!this.currentStep) {
			this.abort("test steps complete");
		} else if (this.currentStep.readFailure) {
			throw new Error("provider failed");
		}
		return [];
	}

	getReplayFloor(): string | undefined {
		return this.currentStep?.replayPending ? "1.000000000" : undefined;
	}
}

async function waitUntil(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	throw new Error("Condition was not reached");
}

function abortNamedError(message: string): Error {
	const error = new Error(message);
	error.name = "AbortError";
	return error;
}

async function runFailureSequence(steps: ReadStep[], threshold = 2) {
	const failureCounts: number[] = [];
	const thresholdCounts: number[] = [];
	let predicateCalls = 0;
	let job!: ScriptedReplayReadJob;
	const config = query(() => {
		predicateCalls++;
		if (job.currentStep?.untilFailure) throw new Error("predicate failed");
		return false;
	});
	config.read.pauseAfterConsecutiveFailures = threshold;
	job = new ScriptedReplayReadJob(config, () => 0, 10, {
		onReadFailure(_query, _error, count) { failureCounts.push(count); },
		onReadConsecutiveFailuresReached(_query, count) { thresholdCounts.push(count); },
	});
	job.steps = steps;
	const loop = job.runForever();
	try {
		await waitUntil(() => job.getStatus().name === "PAUSED" || job.getStatus().name === "STOPPED");
		return { failureCounts, thresholdCounts, predicateCalls, status: job.getStatus().name };
	} finally {
		job.abort("test cleanup");
		await loop;
	}
}

describe("ReadJob lifecycle transitions", () => {
	it("does not auto-resume an operator pause that supersedes backpressure", async () => {
		let queueSize = 1;
		const job = new YieldingReadJob(query(() => false), () => queueSize, 1, {});
		const loop = job.runForever();

		await waitUntil(() => job.getStatus().name === "PAUSED");
		await job.pause("admin");
		assert.deepEqual(job.getStatus(), { name: "PAUSED", info: "admin" });
		queueSize = 0;
		for (let attempt = 0; attempt < 10; attempt++) {
			await new Promise<void>((resolve) => setImmediate(resolve));
		}
		assert.equal(job.getStatus().name, "PAUSED");

		job.abort("test complete");
		await loop;
	});

	it("does not run until or leak a rejection after a fail-fast completion hook", async () => {
		let untilCalled = false;
		let job!: ImmediateReadJob;
		let unhandled: unknown;
		const onUnhandled = (error: unknown) => { unhandled = error; };
		process.on("unhandledRejection", onUnhandled);
		const hooks = new CompositeHooks(
			[{ onReadCycleCompleted() { throw new Error("completion persistence failed"); } }],
			{ policy: "fail-fast", onFailure: () => job.fail("completion persistence failed") }
		);
		job = new ImmediateReadJob(query(() => {
			untilCalled = true;
			return true;
		}), () => 0, 10, hooks);

		try {
			await job.runForever();
			await new Promise<void>((resolve) => setImmediate(resolve));
			assert.equal(untilCalled, false);
			assert.equal(unhandled, undefined);
			assert.equal(job.getStatus().name, "FAILED");
		} finally {
			process.removeListener("unhandledRejection", onUnhandled);
		}
	});

	it("isolates the cursor passed to until and reports predicate failures without changing a completed audit", async () => {
		const completions: boolean[] = [];
		let reportedError: unknown;
		let reportedFailures = 0;
		let job!: ImmediateReadJob;
		job = new ImmediateReadJob(
			query((cursor) => {
				(cursor as Cursor).timestamp = "999.000000000";
				throw new Error("predicate failed");
			}),
			() => 0,
			10,
			{
				onReadCycleCompleted(_query, result) {
					completions.push(result.success);
				},
				onReadFailure(_query, error, consecutiveFailures) {
					reportedError = error;
					reportedFailures = consecutiveFailures;
					job.abort("test complete");
				},
			}
		);

		await job.runForever();

		assert.deepEqual(completions, [true]);
		assert.match((reportedError as Error).message, /predicate failed/);
		assert.equal(reportedFailures, 1);
		assert.deepEqual(job.getCursor(), { timestamp: "1.000000000", index: 0 });
		assert.equal(job.getLastCycle()?.success, true);
		assert.equal(job.getLastCycle()?.error, undefined);
	});

	it("counts consecutive until failures while keeping each completed read successful", async () => {
		let predicateCalls = 0;
		const failureCounts: number[] = [];
		const completions: boolean[] = [];
		const config = query(() => {
			predicateCalls++;
			throw new Error("predicate failed");
		});
		config.read.pauseAfterConsecutiveFailures = 2;
		let job!: ImmediateReadJob;
		job = new ImmediateReadJob(config, () => 0, 10, {
			onReadCycleCompleted(_query, result) {
				completions.push(result.success);
			},
			onReadFailure(_query, _error, consecutiveFailures) {
				failureCounts.push(consecutiveFailures);
			},
			onReadConsecutiveFailuresReached(_query, consecutiveFailures) {
				assert.equal(consecutiveFailures, 2);
				job.abort("test complete");
			},
		});

		await job.runForever();

		assert.equal(predicateCalls, 2);
		assert.deepEqual(failureCounts, [1, 2]);
		assert.deepEqual(completions, [true, true]);
		assert.equal(job.getLastCycle()?.success, true);
	});

	it("clears a read-failure streak when a successful read defers until", async () => {
		const result = await runFailureSequence([
			{ readFailure: true },
			{ replayPending: true },
			{ readFailure: true },
		]);

		assert.deepEqual(result.failureCounts, [1, 1]);
		assert.deepEqual(result.thresholdCounts, []);
		assert.equal(result.predicateCalls, 0);
		assert.equal(result.status, "STOPPED");
	});

	it("starts a new predicate-failure streak after a successful deferred read clears a read failure", async () => {
		const result = await runFailureSequence([
			{ readFailure: true },
			{ replayPending: true },
			{ untilFailure: true },
		]);

		assert.deepEqual(result.failureCounts, [1, 1]);
		assert.deepEqual(result.thresholdCounts, []);
		assert.equal(result.predicateCalls, 1);
		assert.equal(result.status, "STOPPED");
	});

	it("preserves a predicate-failure streak across multiple successful deferred reads", async () => {
		const result = await runFailureSequence([
			{ untilFailure: true },
			{ replayPending: true },
			{ replayPending: true },
			{ untilFailure: true },
		]);

		assert.deepEqual(result.failureCounts, [1, 2]);
		assert.deepEqual(result.thresholdCounts, [2]);
		assert.equal(result.predicateCalls, 2);
		assert.equal(result.status, "PAUSED");
	});

	for (const firstFailure of ["readFailure", "untilFailure"] as const) {
		it(`clears a ${firstFailure} streak after a successful predicate evaluation`, async () => {
			const result = await runFailureSequence([
				{ [firstFailure]: true },
				{},
				{ replayPending: true },
				{ untilFailure: true },
			]);

			assert.deepEqual(result.failureCounts, [1, 1]);
			assert.deepEqual(result.thresholdCounts, []);
			assert.equal(result.status, "STOPPED");
		});
	}

	for (const steps of [
		[{ readFailure: true }, { untilFailure: true }, { readFailure: true }],
		[{ untilFailure: true }, { readFailure: true }, { untilFailure: true }],
	]) {
		it(`counts alternating failures starting with ${steps[0].readFailure ? "reading" : "until"} without a successful cycle`, async () => {
			const result = await runFailureSequence(steps, 3);

			assert.deepEqual(result.failureCounts, [1, 2, 3]);
			assert.deepEqual(result.thresholdCounts, [3]);
			assert.equal(result.status, "PAUSED");
		});
	}

	for (const firstFailure of ["readFailure", "untilFailure"] as const) {
		it(`clears a ${firstFailure} streak on explicit resume before replay defers until`, async () => {
			const failureCounts: number[] = [];
			let job!: ScriptedReplayReadJob;
			const config = query(() => {
				if (job.currentStep?.untilFailure) throw new Error("predicate failed");
				return false;
			});
			job = new ScriptedReplayReadJob(config, () => 0, 10, {
				onReadFailure(_query, _error, count) { failureCounts.push(count); },
			});
			job.steps = [{ [firstFailure]: true }, { replayPending: true }, { untilFailure: true }];
			const loop = job.runForever();
			try {
				await waitUntil(() => job.getStatus().name === "PAUSED");
				assert.deepEqual(failureCounts, [1]);
				await job.resume();
				await waitUntil(() => job.getStatus().name === "PAUSED");
				assert.deepEqual(failureCounts, [1, 1]);
			} finally {
				job.abort("test cleanup");
				await loop;
			}
		});
	}

	it("contains an opaque cycle rejection and reports it as a read failure", async () => {
		const thrown = Proxy.revocable({}, {});
		thrown.revoke();
		let reportedError: unknown;
		let reportedFailures = 0;
		const completions: boolean[] = [];

		class OpaqueFailureReadJob extends ReadJob {
			protected async cycle(): Promise<SyncReadItem[]> {
				throw thrown.proxy;
			}

			protected async waitWithAbort(): Promise<void> {}
		}

		let job!: OpaqueFailureReadJob;
		job = new OpaqueFailureReadJob(query(() => false), () => 0, 10, {
			onReadCycleCompleted(_query, result) {
				completions.push(result.success);
			},
			onReadFailure(_query, error, consecutiveFailures) {
				reportedError = error;
				reportedFailures = consecutiveFailures;
				job.abort("test complete");
			},
		});

		await job.runForever();

		assert.equal(reportedError, thrown.proxy);
		assert.equal(reportedFailures, 1);
		assert.deepEqual(completions, [false]);
		assert.equal(job.getLastCycle()?.success, false);
		assert.equal(job.getStatus().name, "STOPPED");
	});

	it("contains an opaque until rejection without invalidating the completed read", async () => {
		const thrown = Proxy.revocable({}, {});
		thrown.revoke();
		let reportedError: unknown;
		let reportedFailures = 0;
		const completions: boolean[] = [];
		let job!: ImmediateReadJob;
		job = new ImmediateReadJob(
			query(() => {
				throw thrown.proxy;
			}),
			() => 0,
			10,
			{
				onReadCycleCompleted(_query, result) {
					completions.push(result.success);
				},
				onReadFailure(_query, error, consecutiveFailures) {
					reportedError = error;
					reportedFailures = consecutiveFailures;
					job.abort("test complete");
				},
			}
		);

		await job.runForever();

		assert.equal(reportedError, thrown.proxy);
		assert.equal(reportedFailures, 1);
		assert.deepEqual(completions, [true]);
		assert.equal(job.getLastCycle()?.success, true);
		assert.equal(job.getStatus().name, "STOPPED");
	});

	it("counts an unowned AbortError from a read cycle as an operational failure", async () => {
		const failure = abortNamedError("provider aborted its request");
		const failureCounts: number[] = [];
		const thresholdCounts: number[] = [];
		const completions: boolean[] = [];
		const config = query(() => false);
		config.read.pauseAfterConsecutiveFailures = 2;

		class AbortNamedFailureReadJob extends ReadJob {
			protected async cycle(): Promise<SyncReadItem[]> {
				throw failure;
			}

			protected async waitWithAbort(): Promise<void> {
				await new Promise<void>((resolve) => setImmediate(resolve));
			}
		}

		const job = new AbortNamedFailureReadJob(config, () => 0, 10, {
			onReadCycleCompleted(_query, result) {
				completions.push(result.success);
			},
			onReadFailure(_query, error, consecutiveFailures) {
				assert.equal(error, failure);
				failureCounts.push(consecutiveFailures);
			},
			onReadConsecutiveFailuresReached(_query, consecutiveFailures) {
				thresholdCounts.push(consecutiveFailures);
			},
		});

		const loop = job.runForever();
		try {
			await waitUntil(() => thresholdCounts.length === 1);
		} finally {
			job.abort("test complete");
			await loop;
		}

		assert.deepEqual(failureCounts, [1, 2]);
		assert.deepEqual(thresholdCounts, [2]);
		assert.deepEqual(completions, [false, false]);
		assert.equal(job.getStatus().name, "STOPPED");
	});

	it("counts an unowned AbortError from until while preserving successful read audits", async () => {
		const failure = abortNamedError("predicate aborted itself");
		const failureCounts: number[] = [];
		const thresholdCounts: number[] = [];
		const completions: boolean[] = [];
		const config = query(() => {
			throw failure;
		});
		config.read.pauseAfterConsecutiveFailures = 2;

		const job = new YieldingReadJob(config, () => 0, 10, {
			onReadCycleCompleted(_query, result) {
				completions.push(result.success);
			},
			onReadFailure(_query, error, consecutiveFailures) {
				assert.equal(error, failure);
				failureCounts.push(consecutiveFailures);
			},
			onReadConsecutiveFailuresReached(_query, consecutiveFailures) {
				thresholdCounts.push(consecutiveFailures);
			},
		});

		const loop = job.runForever();
		try {
			await waitUntil(() => thresholdCounts.length === 1);
		} finally {
			job.abort("test complete");
			await loop;
		}

		assert.deepEqual(failureCounts, [1, 2]);
		assert.deepEqual(thresholdCounts, [2]);
		assert.deepEqual(completions, [true, true]);
		assert.equal(job.getLastCycle()?.success, true);
		assert.equal(job.getStatus().name, "STOPPED");
	});

	it("does not count an AbortError caused by the active lifecycle signal", async () => {
		let cycleStarted!: () => void;
		const cycleStart = new Promise<void>((resolve) => { cycleStarted = resolve; });
		let reportedFailures = 0;

		class AbortableReadJob extends ReadJob {
			protected async cycle(signal: AbortSignal): Promise<SyncReadItem[]> {
				cycleStarted();
				return new Promise<SyncReadItem[]>((_resolve, reject) => {
					signal.addEventListener("abort", () => reject(abortNamedError("owned cancellation")), { once: true });
				});
			}

			protected async waitWithAbort(): Promise<void> {
				await new Promise<void>((resolve) => setImmediate(resolve));
			}
		}

		const job = new AbortableReadJob(query(() => false), () => 0, 10, {
			onReadFailure() {
				reportedFailures++;
			},
		});
		const loop = job.runForever();

		try {
			await cycleStart;
			await job.pause("operator");
			assert.equal(reportedFailures, 0);
			assert.deepEqual(job.getStatus(), { name: "PAUSED", info: "operator" });
		} finally {
			job.abort("test complete");
			await loop;
		}
	});

	it("preserves consecutive failures across automatic backpressure recovery", async () => {
		let queueSize = 0;
		const failureCounts: number[] = [];
		const thresholdCounts: number[] = [];
		const config = query(() => false);
		config.read.pauseAfterConsecutiveFailures = 2;

		class YieldingFailureReadJob extends ReadJob {
			protected async cycle(): Promise<SyncReadItem[]> {
				throw new Error("provider failed");
			}

			protected async waitWithAbort(): Promise<void> {
				await new Promise<void>((resolve) => setImmediate(resolve));
			}
		}

		let job!: YieldingFailureReadJob;
		job = new YieldingFailureReadJob(config, () => queueSize, 1, {
			onReadFailure(_query, _error, consecutiveFailures) {
				failureCounts.push(consecutiveFailures);
				if (failureCounts.length === 1) queueSize = 1;
				if (failureCounts.length === 2) job.abort("test complete");
			},
			onReadConsecutiveFailuresReached(_query, consecutiveFailures) {
				thresholdCounts.push(consecutiveFailures);
			},
		});

		const loop = job.runForever();
		try {
			await waitUntil(() => job.getStatus().name === "PAUSED");
			assert.deepEqual(job.getStatus(), { name: "PAUSED", info: "backpressure" });
			queueSize = 0;
			await loop;

			assert.deepEqual(failureCounts, [1, 2]);
			assert.deepEqual(thresholdCounts, [2]);
		} finally {
			job.abort("test cleanup");
			await loop;
		}
	});

	it("resets consecutive failures after a fully successful cycle", async () => {
		let cycleNumber = 0;
		const failureCounts: number[] = [];
		const config = query(() => false);
		config.read.pauseAfterConsecutiveFailures = 10;

		class IntermittentReadJob extends ReadJob {
			protected async cycle(): Promise<SyncReadItem[]> {
				cycleNumber++;
				if (cycleNumber !== 2) throw new Error("intermittent provider failure");
				return [];
			}

			protected async waitWithAbort(): Promise<void> {}
		}

		let job!: IntermittentReadJob;
		job = new IntermittentReadJob(config, () => 0, 10, {
			onReadFailure(_query, _error, consecutiveFailures) {
				failureCounts.push(consecutiveFailures);
				if (failureCounts.length === 2) job.abort("test complete");
			},
		});

		await job.runForever();

		assert.equal(cycleNumber, 3);
		assert.deepEqual(failureCounts, [1, 1]);
	});

	it("resets consecutive failures when resumed while a threshold pause hook is settling", async () => {
		const failureCounts: number[] = [];
		const thresholdCounts: number[] = [];
		const config = query(() => {
			throw new Error("predicate failed");
		});
		config.read.pauseAfterConsecutiveFailures = 2;

		let thresholdStarted!: () => void;
		const thresholdStart = new Promise<void>((resolve) => { thresholdStarted = resolve; });
		let releaseThreshold!: () => void;
		const thresholdBlocked = new Promise<void>((resolve) => { releaseThreshold = resolve; });
		let thresholdReleased = false;
		let job!: ImmediateReadJob;
		job = new ImmediateReadJob(config, () => 0, 10, {
			async onReadConsecutiveFailuresReached(_query, consecutiveFailures) {
				thresholdCounts.push(consecutiveFailures);
				if (consecutiveFailures === 2) {
					thresholdStarted();
					await thresholdBlocked;
				}
			},
			onReadFailure(_query, _error, consecutiveFailures) {
				failureCounts.push(consecutiveFailures);
				if (failureCounts.length === 3) job.abort("test complete");
			},
		});

		const loop = job.runForever();
		try {
			await thresholdStart;
			assert.equal(job.getStatus().name, "PAUSING");

			await job.resume();
			assert.equal(job.getStatus().name, "RUNNING");

			releaseThreshold();
			thresholdReleased = true;
			await loop;

			assert.deepEqual(failureCounts, [1, 2, 1]);
			assert.deepEqual(thresholdCounts, [2]);
		} finally {
			if (!thresholdReleased) releaseThreshold();
			job.abort("test cleanup");
			await loop;
		}
	});

	it("rejects pause when a terminal failure wins the active-cycle race", async () => {
		let cycleStarted!: () => void;
		const cycleStart = new Promise<void>((resolve) => { cycleStarted = resolve; });
		let releaseCycle!: () => void;
		const cycleBlocked = new Promise<SyncReadItem[]>((resolve) => { releaseCycle = () => resolve([]); });
		class BlockingCycleReadJob extends ReadJob {
			protected async cycle(): Promise<SyncReadItem[]> {
				cycleStarted();
				return cycleBlocked;
			}

			protected async waitWithAbort(): Promise<void> {}
		}

		const job = new BlockingCycleReadJob(query(() => false), () => 0, 10, {});
		const loop = job.runForever();
		try {
			await cycleStart;
			const pausing = job.pause("operator");
			job.fail("terminal failure");

			await assert.rejects(pausing, {
				name: "Error",
				message: "Read job could not reach PAUSED because it entered FAILED: terminal failure",
			});
		} finally {
			releaseCycle();
			await loop;
		}
	});

	it("rejects resume when a status observer stops the job before it settles", async () => {
		const config = query(() => false);
		config.read.status = { name: "PAUSED", info: "operator" };
		let job!: ImmediateReadJob;
		job = new ImmediateReadJob(config, () => 0, 10, {
			onQueryStatusChange(_query, payload) {
				if (payload.status.name === "RESUMING") job.abort("terminal stop");
			},
		});

		await assert.rejects(job.resume(), {
			name: "Error",
			message: "Read job could not reach RUNNING because it entered STOPPED: terminal stop",
		});
		await job.flushHooks();
		assert.deepEqual(job.getStatus(), { name: "STOPPED", info: "terminal stop" });
	});
});
