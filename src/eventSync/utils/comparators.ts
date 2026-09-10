import type { Cursor } from "../types/domain";
import { compareHederaTimestamps } from "./timestamp";

export function compareCursor(a: Cursor, b: Cursor): number {
	const timestampComparison = compareHederaTimestamps(a.timestamp, b.timestamp);
	if (timestampComparison !== 0) return timestampComparison;
	if (a.index < b.index) return -1;
	if (a.index > b.index) return 1;
	return 0;
}

export function maxCursor(a: Cursor, b: Cursor): Cursor {
	return compareCursor(a, b) >= 0 ? a : b;
}
