import { performance } from "node:perf_hooks";

/** Largest delay Node accepts without coercing the timer to approximately 1ms. */
export const MAX_TIMER_DELAY_MS = 2_147_483_647;

/**
 * Wait for the complete requested duration without overflowing Node's timer.
 * Long waits are split into native-safe chunks.
 */
export async function sleep(ms: number): Promise<void> {
	await waitInChunks(ms);
}

/** Wait for a possibly long duration while allowing immediate cancellation. */
export async function sleepWithSignal(ms: number, signal: AbortSignal): Promise<void> {
	await waitInChunks(ms, signal);
}

async function waitInChunks(ms: number, signal?: AbortSignal): Promise<void> {
	if (!Number.isFinite(ms) || ms <= 0 || signal?.aborted) return;

	let remaining = ms;
	while (remaining > 0 && !signal?.aborted) {
		const delay = Math.min(MAX_TIMER_DELAY_MS, remaining);
		const startedAt = performance.now();
		await sleepChunk(delay, signal);
		// A suspended process can wake long after the native timer's requested
		// delay. Count monotonic elapsed time without trusting an adjustable clock.
		const elapsed = Math.max(0, performance.now() - startedAt);
		remaining -= Math.max(delay, elapsed);
	}
}

async function sleepChunk(ms: number, signal?: AbortSignal): Promise<void> {
	await new Promise<void>((resolve) => {
		let settled = false;
		const finish = () => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			signal?.removeEventListener("abort", finish);
			resolve();
		};
		const timer = setTimeout(finish, ms);
		signal?.addEventListener("abort", finish, { once: true });
	});
}
