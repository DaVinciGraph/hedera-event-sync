import type { NextPageFn } from "@davincigraph/hedera-rest-client";
import { compareHederaTimestamps } from "./timestamp";

type Page<P> = { next: NextPageFn<P>; syncWindowEnd?: string };
type Window<P> = {
	start: string;
	end: string;
	continuation?: P;
	completed: boolean;
	completedAt?: string;
};

/**
 * Keeps forward pagination and fresh replay queries independent. A bounded
 * forward segment is followed by a frozen replay sweep before the next segment.
 * Dense sweeps retain their own continuation, so a page limit cannot repeatedly
 * restart the same overlap or silently move its lower bound past unread records.
 */
export class ReplayPaginator<P extends Page<P>> {
	private forward?: Window<P>;
	private replay?: Window<P>;
	private segmentFloor?: string;

	/** Oldest timestamp still awaiting a fresh replay, including failed segments. */
	getReplayFloor(): string | undefined {
		return this.segmentFloor;
	}

	async run(options: {
		signal: AbortSignal;
		maxPages: number;
		replayEnabled: boolean;
		replayStart: string;
		safeHead: string;
		fetch: (start: string, end: string) => Promise<P | null>;
		consume: (page: P, lowerBound: string) => Promise<boolean>;
		cursorTimestamp: () => string;
		advanceTo: (timestamp: string) => void;
	}): Promise<void> {
		const { signal, replayStart, safeHead } = options;
		if (signal.aborted) return;
		if (!this.forward) {
			if (compareHederaTimestamps(replayStart, safeHead) > 0) return;
			this.forward = { start: replayStart, end: safeHead, completed: false };
		}

		const replaying = this.replay !== undefined;
		const window = this.replay ?? this.forward;
		// Install the checkpoint barrier before fetching/committing forward work.
		// Keep it through cancellation, failures, and the resulting replay sweep.
		this.segmentFloor ??= replayStart;
		const lowerBound = replaying ? window.start : this.segmentFloor;
		const continued = window.continuation !== undefined;
		let pagesRead = 0;
		while (!window.completed && pagesRead < options.maxPages && !signal.aborted) {
			const page = window.continuation
				? await window.continuation.next({ signal })
				: await options.fetch(window.start, window.end);
			pagesRead++;
			if (signal.aborted) return;
			if (!page) {
				window.completed = true;
				break;
			}
			if (!window.continuation) {
				window.completedAt = page.syncWindowEnd;
				if (page.syncWindowEnd) window.end = page.syncWindowEnd;
			}
			if (!await options.consume(page, lowerBound) || signal.aborted) return;
			// Only committed pages become continuation anchors. A failed or aborted
			// page is fetched again with its frozen eligibility bound on the next run.
			window.continuation = page;
			window.completed = page.next.url() === null;
		}
		if (signal.aborted) return;

		if (replaying) {
			if (!window.completed) return;
			this.replay = undefined;
		} else {
			if (window.completed && window.completedAt) options.advanceTo(window.completedAt);
			if (options.replayEnabled && (continued || !window.completed || pagesRead > 1)) {
				this.replay = {
					start: this.segmentFloor,
					end: options.cursorTimestamp(),
					completed: false,
				};
				return;
			}
		}

		this.segmentFloor = undefined;
		if (this.forward.completed) this.forward = undefined;
	}
}
