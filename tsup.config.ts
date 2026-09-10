import { defineConfig } from "tsup";

export default defineConfig({
	entry: {
		index: "src/index.ts",
		"types/config": "src/eventSync/types/config.ts",
		"types/domain": "src/eventSync/types/domain.ts",
		"core/logs/LogEventProcessor": "src/eventSync/core/logs/LogEventProcessor.ts",
	},
	format: ["esm"],
	target: "es2022",
	platform: "node",
	dts: true,
	sourcemap: true,
	clean: true,
	treeshake: true,
});
