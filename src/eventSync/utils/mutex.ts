export class AsyncMutex {
	private locked = false;
	private waiters: Array<() => void> = [];

	async lock(): Promise<() => void> {
		if (!this.locked) {
			this.locked = true;
			return () => this.unlock();
		}
		await new Promise<void>((resolve) => this.waiters.push(resolve));
		this.locked = true;
		return () => this.unlock();
	}

	private unlock() {
		const next = this.waiters.shift();
		if (next) next();
		else this.locked = false;
	}
}
