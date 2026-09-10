import type { ContractLogTopicFilter } from "../types/config";
import { formatHederaTimestampNs, parseHederaTimestampNs } from "./timestamp";

const NANOS_PER_SECOND = 1_000_000_000n;
const NANOS_PER_MILLISECOND = 1_000_000n;

/** Mirror Node topic-filtered log searches are limited to seven days. */
export const TOPIC_LOG_WINDOW_SECONDS = 7 * 24 * 60 * 60;
export const TOPIC_LOG_WINDOW_NANOS = BigInt(TOPIC_LOG_WINDOW_SECONDS) * NANOS_PER_SECOND;
/** Inclusive `gte`/`lte` bounds leave one nanosecond less than the nominal window. */
export const TOPIC_LOG_WINDOW_SPAN_NANOS = TOPIC_LOG_WINDOW_NANOS - 1n;

/** Returns whether replaying this overlap still moves a bounded topic window forward. */
export function topicLogOverlapCanAdvance(overlapSeconds: number): boolean {
	if (!Number.isFinite(overlapSeconds) || overlapSeconds < 0) return false;
	if (overlapSeconds >= TOPIC_LOG_WINDOW_SECONDS) return false;
	const overlapNanos = BigInt(Math.floor(overlapSeconds * Number(NANOS_PER_SECOND)));
	return overlapNanos < TOPIC_LOG_WINDOW_SPAN_NANOS;
}

/** Applies the exact inclusive seven-day bound required by topic-filtered log endpoints. */
export function boundedTopicLogWindowEnd(startTimestamp: string, requestedEnd?: string): string {
	const startNs = parseHederaTimestampNs(startTimestamp);
	const maxEndNs = startNs + TOPIC_LOG_WINDOW_SPAN_NANOS;
	const hasExplicitEnd = requestedEnd !== undefined;
	const requestedEndNs = hasExplicitEnd ? parseHederaTimestampNs(requestedEnd) : BigInt(Date.now()) * NANOS_PER_MILLISECOND;
	if (hasExplicitEnd && requestedEndNs < startNs) throw new Error("Topic-log timestamp end must not be earlier than its start");
	const effectiveEndNs = requestedEndNs < startNs ? startNs : requestedEndNs;
	const endNs = maxEndNs < effectiveEndNs ? maxEndNs : effectiveEndNs;
	return formatHederaTimestampNs(endNs);
}

/** Reports whether at least one Mirror Node topic position has a value. */
export function hasContractLogTopicFilters(topics: readonly (ContractLogTopicFilter | undefined)[] | undefined): boolean {
	return topics?.some((topic) => typeof topic === "string" ? topic.length > 0 : Array.isArray(topic) && topic.length > 0) ?? false;
}
