import { readdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

export async function discoverTestFiles(directory) {
	const testFiles = [];
	const entries = await readdir(directory, { withFileTypes: true });

	for (const entry of entries) {
		const entryPath = join(directory, entry.name);

		if (entry.isDirectory()) {
			testFiles.push(...(await discoverTestFiles(entryPath)));
		} else if (entry.isFile() && entry.name.endsWith(".test.ts")) {
			testFiles.push(entryPath);
		}
	}

	return testFiles.sort();
}

async function run() {
	const testDirectory = dirname(fileURLToPath(import.meta.url));
	const testFiles = await discoverTestFiles(testDirectory);

	if (testFiles.length === 0) {
		throw new Error(`No unit tests were found in ${testDirectory}`);
	}

	const require = createRequire(import.meta.url);
	const tsxCli = require.resolve("tsx/cli");
	const result = spawnSync(process.execPath, [tsxCli, "--test", ...testFiles], {
		stdio: "inherit",
	});

	if (result.error) {
		throw result.error;
	}

	if (result.signal) {
		throw new Error(`The test runner was terminated by ${result.signal}`);
	}

	process.exitCode = result.status ?? 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	await run();
}
