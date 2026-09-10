import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { describe, it } from "node:test";
import { compareCursor } from "../src/eventSync/utils/comparators";
import { currentHederaTimestamp, formatHederaTimestampNs, normalizeHederaTimestamp, parseHederaTimestampNs, subtractHederaTimestampSeconds } from "../src/eventSync/utils/timestamp";
import { MAX_TIMER_DELAY_MS, nextRetentionDelay } from "../src/eventSync/utils/retention";
import { sleepWithSignal } from "../src/eventSync/utils/timer";

describe("Hedera timestamp handling", () => {
	it("compares timestamps without losing nanosecond precision", () => {
		assert.equal(compareCursor({ timestamp: "1700000000.000000001", index: 0 }, { timestamp: "1700000000.000000002", index: 0 }), -1);
		assert.equal(compareCursor({ timestamp: "9.9", index: 0 }, { timestamp: "10.0", index: 0 }), -1);
		assert.equal(compareCursor({ timestamp: "1", index: 2 }, { timestamp: "1.000000000", index: 1 }), 1);
	});

	it("accepts seconds-only timestamps and canonicalizes nanoseconds", () => {
		assert.equal(normalizeHederaTimestamp("123"), "123.000000000");
		assert.equal(normalizeHederaTimestamp(123), "123.000000000");
		assert.equal(normalizeHederaTimestamp("0000000001.1"), "1.100000000");
		assert.equal(parseHederaTimestampNs("123.4"), 123_400_000_000n);
		assert.equal(formatHederaTimestampNs(123_400_000_001n), "123.400000001");
	});

	it("enforces the Mirror Node signed-int64 nanosecond boundary", () => {
		assert.equal(normalizeHederaTimestamp("9223372036.854775807"), "9223372036.854775807");
		assert.equal(normalizeHederaTimestamp(9_223_372_036), "9223372036.000000000");

		for (const value of ["9223372036.854775808", "9223372037", "9999999999", "00000000001"]) {
			assert.throws(() => parseHederaTimestampNs(value));
		}
		assert.throws(() => parseHederaTimestampNs(9_223_372_037));
		assert.throws(() => formatHederaTimestampNs(9_223_372_036_854_775_808n));
	});

	it("requires strings for fractional timestamps so JavaScript cannot round cursor precision", () => {
		assert.throws(() => parseHederaTimestampNs(1.25), /safe-integer seconds; use a string for fractional seconds/);
		assert.throws(() => parseHederaTimestampNs(1e-9), /safe-integer seconds; use a string for fractional seconds/);
		assert.throws(() => parseHederaTimestampNs(Number.MAX_SAFE_INTEGER + 1), /safe-integer seconds/);
		assert.throws(() => parseHederaTimestampNs(1n as never), /expected a string or a number/);
		assert.equal(normalizeHederaTimestamp("1.25"), "1.250000000");
	});

	it("rejects malformed, negative, and over-precise timestamps", () => {
		for (const value of ["-1.0", "1.1234567890", "1.", "1.2.3", " 1.0", "1.0 ", Number.NaN, Number.POSITIVE_INFINITY]) {
			assert.throws(() => parseHederaTimestampNs(value));
		}
	});

	it("handles huge finite consistency horizons without overflowing Number arithmetic", () => {
		assert.equal(subtractHederaTimestampSeconds("10.000000000", Number.MAX_VALUE), "0.000000000");
		assert.equal(currentHederaTimestamp(Number.MAX_VALUE), "0.000000000");
	});

	it("converts decimal durations without binary floating-point nanosecond loss", () => {
		assert.equal(subtractHederaTimestampSeconds("3.000000000", 1.999999999), "1.000000001");
		assert.equal(subtractHederaTimestampSeconds("1.000000000", 0.000508624), "0.999491376");
		assert.equal(subtractHederaTimestampSeconds("8589934502.000000000", 8_589_934_501), "1.000000000");
		assert.equal(subtractHederaTimestampSeconds("8589934502.000000000", 8_589_934_501.5), "0.500000000");
	});
});

describe("retention scheduling", () => {
	it("caps long waits and supports monthly retention without timer overflow", () => {
		const now = 1_000;
		assert.equal(nextRetentionDelay(now + 30 * 24 * 60 * 60 * 1_000, now), MAX_TIMER_DELAY_MS);
		assert.equal(nextRetentionDelay(now + 25, now), 25);
	});
});

describe("long timer scheduling", () => {
	it("splits delays that exceed Node's native timer maximum", async () => {
		const originalSetTimeout = globalThis.setTimeout;
		const delays: number[] = [];
		globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
			delays.push(delay ?? 0);
			queueMicrotask(() => callback(...args));
			return 0 as unknown as ReturnType<typeof setTimeout>;
		}) as typeof setTimeout;

		try {
			await sleepWithSignal(MAX_TIMER_DELAY_MS + 25, new AbortController().signal);
		} finally {
			globalThis.setTimeout = originalSetTimeout;
		}

		assert.deepEqual(delays, [MAX_TIMER_DELAY_MS, 25]);
	});

	it("cancels a native-safe long timer immediately when aborted", async () => {
		const controller = new AbortController();
		const waiting = sleepWithSignal(MAX_TIMER_DELAY_MS, controller.signal);
		controller.abort();
		await waiting;
	});

	it("does not schedule nominal remainder after suspension already consumed it", async () => {
		const originalSetTimeout = globalThis.setTimeout;
		const originalPerformanceNow = Object.getOwnPropertyDescriptor(performance, "now");
		const delays: number[] = [];
		let now = 1_000;
		Object.defineProperty(performance, "now", { configurable: true, value: () => now });
		globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
			delays.push(delay ?? 0);
			now += MAX_TIMER_DELAY_MS + 100;
			queueMicrotask(() => callback(...args));
			return 0 as unknown as ReturnType<typeof setTimeout>;
		}) as typeof setTimeout;

		try {
			await sleepWithSignal(MAX_TIMER_DELAY_MS + 25, new AbortController().signal);
		} finally {
			globalThis.setTimeout = originalSetTimeout;
			if (originalPerformanceNow) Object.defineProperty(performance, "now", originalPerformanceNow);
			else Reflect.deleteProperty(performance, "now");
		}

		assert.deepEqual(delays, [MAX_TIMER_DELAY_MS]);
	});

	it("does not finish a long wait early after a forward wall-clock adjustment", async () => {
		const originalSetTimeout = globalThis.setTimeout;
		const originalDateNow = Date.now;
		const delays: number[] = [];
		let wallClock = 1_000;
		Date.now = () => wallClock;
		globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
			delays.push(delay ?? 0);
			wallClock += MAX_TIMER_DELAY_MS + 100;
			queueMicrotask(() => callback(...args));
			return 0 as unknown as ReturnType<typeof setTimeout>;
		}) as typeof setTimeout;

		try {
			await sleepWithSignal(MAX_TIMER_DELAY_MS + 25, new AbortController().signal);
		} finally {
			globalThis.setTimeout = originalSetTimeout;
			Date.now = originalDateNow;
		}

		assert.deepEqual(delays, [MAX_TIMER_DELAY_MS, 25]);
	});
});
