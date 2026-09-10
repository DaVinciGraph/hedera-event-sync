export const DEFAULTS = {
	pollIntervalSeconds: 30,
	batchSize: 100,
	maxPagesPerCycle: 10,
	maxQueuedProcessesPerQuery: 10_000,
	nextPageDelayMs: 0,
	nextProcessDelayMs: 0,
	holdPeriod: "daily",
	finalityLagSeconds: 15,
	overlapSeconds: 30,
} as const;
