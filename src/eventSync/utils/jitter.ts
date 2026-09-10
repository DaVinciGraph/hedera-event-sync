export function withJitter(baseMs: number, ratio = 0.1): number {
	const delta = baseMs * ratio;
	return Math.max(0, baseMs + (Math.random() * 2 - 1) * delta);
}
