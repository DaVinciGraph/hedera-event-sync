import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { discoverTestFiles } from "./run-tests.mjs";

test("test discovery recursively selects only TypeScript test files", async () => {
	const root = await mkdtemp(join(tmpdir(), "hedera-event-sync-tests-"));
	const nestedDirectory = join(root, "nested", "deeper");

	try {
		await mkdir(nestedDirectory, { recursive: true });
		await Promise.all([
			writeFile(join(root, "root.test.ts"), ""),
			writeFile(join(root, "ignored.ts"), ""),
			writeFile(join(nestedDirectory, "nested.test.ts"), ""),
			writeFile(join(nestedDirectory, "ignored.test.mts"), ""),
		]);

		assert.deepEqual(await discoverTestFiles(root), [
			join(root, "root.test.ts"),
			join(nestedDirectory, "nested.test.ts"),
		].sort());
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});
