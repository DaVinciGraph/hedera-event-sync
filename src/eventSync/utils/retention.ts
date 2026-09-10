import { MAX_TIMER_DELAY_MS } from "./timer";

export { MAX_TIMER_DELAY_MS } from "./timer";

export function nextRetentionDelay(deadline: number, now = Date.now()): number {
	return Math.min(MAX_TIMER_DELAY_MS, Math.max(1, deadline - now));
}
