const NANOS_PER_SECOND = 1_000_000_000n;
const MAX_HEDERA_TIMESTAMP_NS = 9_223_372_036_854_775_807n;
const HEDERA_TIMESTAMP = /^(\d{1,10})(?:\.(\d{1,9}))?$/;

export function parseHederaTimestampNs(value: string | number): bigint {
	if (typeof value !== "string" && typeof value !== "number") {
		throw new Error("Invalid Hedera timestamp: expected a string or a number of whole seconds");
	}
	if (typeof value === "number") {
		if (!Number.isSafeInteger(value) || value < 0) {
			throw new Error(`Invalid Hedera timestamp: numeric timestamps must be non-negative safe-integer seconds; use a string for fractional seconds`);
		}
	}

	const raw = String(value);
	const match = HEDERA_TIMESTAMP.exec(raw);
	if (!match) throw new Error(`Invalid Hedera timestamp: ${raw}`);

	const seconds = BigInt(match[1]);
	const nanos = BigInt((match[2] ?? "").padEnd(9, "0") || "0");
	const timestamp = seconds * NANOS_PER_SECOND + nanos;
	if (timestamp > MAX_HEDERA_TIMESTAMP_NS) {
		throw new Error(`Invalid Hedera timestamp: ${raw} exceeds the signed 64-bit nanosecond maximum`);
	}
	return timestamp;
}

export function formatHederaTimestampNs(value: bigint): string {
	if (value < 0n) throw new Error("Hedera timestamps cannot be negative");
	if (value > MAX_HEDERA_TIMESTAMP_NS) throw new Error("Hedera timestamp exceeds the signed 64-bit nanosecond maximum");
	const seconds = value / NANOS_PER_SECOND;
	const nanos = value % NANOS_PER_SECOND;
	return `${seconds.toString()}.${nanos.toString().padStart(9, "0")}`;
}

export function normalizeHederaTimestamp(value: string | number): string {
	return formatHederaTimestampNs(parseHederaTimestampNs(value));
}

export function compareHederaTimestamps(a: string | number, b: string | number): number {
	const aNs = parseHederaTimestampNs(a);
	const bNs = parseHederaTimestampNs(b);
	return aNs < bNs ? -1 : aNs > bNs ? 1 : 0;
}

export function subtractHederaTimestampSeconds(value: string | number, seconds: number): string {
	if (!Number.isFinite(seconds) || seconds < 0) throw new Error("seconds must be a finite non-negative number");
	const delta = secondsToNanoseconds(seconds);
	const timestamp = parseHederaTimestampNs(value);
	return formatHederaTimestampNs(timestamp > delta ? timestamp - delta : 0n);
}

export function currentHederaTimestamp(finalityLagSeconds = 0): string {
	if (!Number.isFinite(finalityLagSeconds) || finalityLagSeconds < 0) {
		throw new Error("finalityLagSeconds must be a finite non-negative number");
	}
	const now = BigInt(Date.now()) * 1_000_000n;
	const lag = secondsToNanoseconds(finalityLagSeconds);
	return formatHederaTimestampNs(now > lag ? now - lag : 0n);
}

/**
 * Converts a finite, non-negative duration without binary floating-point
 * multiplication. Number#toString provides the shortest decimal form that
 * round-trips to the supplied value; parsing that form keeps exact nanosecond
 * boundaries exact and truncates only precision below one nanosecond.
 */
function secondsToNanoseconds(seconds: number): bigint {
	const [coefficient, rawExponent] = seconds.toString().toLowerCase().split("e");
	const [whole, fraction = ""] = coefficient.split(".");
	const digits = `${whole}${fraction}`.replace(/^0+/, "") || "0";
	const scale = Number(rawExponent ?? 0) + 9 - fraction.length;

	if (scale >= 0) return BigInt(digits) * 10n ** BigInt(scale);

	const retainedDigits = digits.length + scale;
	return retainedDigits > 0 ? BigInt(digits.slice(0, retainedDigits)) : 0n;
}
