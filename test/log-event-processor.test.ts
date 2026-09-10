import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Abi } from "abitype";
import type { HederaRestClient } from "@davincigraph/hedera-rest-client";
import { encodeAbiParameters, keccak256, toBytes } from "viem";
import { DecodeError, NormalizationError } from "../src/eventSync/errors";
import { HandlerRegistry } from "../src/eventSync/handlers/HandlerRegistry";
import { LogEventProcessor } from "../src/eventSync/core/logs/LogEventProcessor";
import type { SyncContractLog } from "../src/eventSync/types/mirror";

const pingAbi = [{ type: "event", name: "Ping", anonymous: false, inputs: [] }] as const satisfies Abi;
const valueAbi = [{
	type: "event",
	name: "Value",
	anonymous: false,
	inputs: [
		{ name: "indexedValue", type: "uint256", indexed: true },
		{ name: "value", type: "uint256", indexed: false },
	],
}] as const satisfies Abi;

function log(eventSignature: string, data = "0x"): SyncContractLog {
	return {
		address: `0x${"1".repeat(40)}`,
		data,
		index: 0,
		timestamp: "1.000000000",
		topics: [keccak256(toBytes(eventSignature))],
		transaction_hash: `0x${"2".repeat(64)}`,
	};
}

function processor(registry = new HandlerRegistry()): LogEventProcessor {
	return new LogEventProcessor({
		registry,
		restClient: {} as HederaRestClient,
		network: "testnet",
		provider: "public",
	});
}

describe("LogEventProcessor decoding", () => {
	for (const anonymousFirst of [true, false]) {
		it(`decodes the supported event when a same-signature anonymous declaration appears ${anonymousFirst ? "first" : "last"}`, async () => {
			const supported = valueAbi[0];
			const anonymous = {
				...supported,
				anonymous: true,
				inputs: [
					{ name: "indexedValue", type: "uint256", indexed: false },
					{ name: "value", type: "uint256", indexed: true },
				],
			} as const;
			const abi = anonymousFirst ? [anonymous, supported] : [supported, anonymous];
			const registry = new HandlerRegistry();
			const handled: unknown[] = [];
			registry.registerLog("Value", {
				normalize: (args) => args,
				handle: (data) => { handled.push(data); },
			});
			const record = log("Value(uint256,uint256)", encodeAbiParameters([{ type: "uint256" }], [29n]));
			record.topics.push(encodeAbiParameters([{ type: "uint256" }], [17n]));

			const prepared = await processor(registry).prepareAndExecute(record, { abi });

			assert.deepEqual(prepared?.data, { indexedValue: 17n, value: 29n });
			assert.deepEqual(handled, [{ indexedValue: 17n, value: 29n }]);
		});
	}

	it("uses the supported declaration for both topic cardinality and decoding", async () => {
		const anonymous = {
			...valueAbi[0],
			anonymous: true,
			inputs: valueAbi[0].inputs.map((input) => ({ ...input, indexed: false })),
		} as const;
		const abi = [anonymous, valueAbi[0]];
		const record = log("Value(uint256,uint256)", encodeAbiParameters([{ type: "uint256" }], [29n]));
		record.topics.push(encodeAbiParameters([{ type: "uint256" }], [17n]));

		assert.equal(await processor().prepare(record, { abi, skipEventNames: ["Value"] }), null);
		record.topics.pop();
		await assert.rejects(
			processor().prepare(record, { abi, skipEventNames: ["Value"] }),
			(error: unknown) => {
				assert.ok(error instanceof DecodeError);
				assert.match(error.message, /exactly 2 topics; received 1/);
				return true;
			}
		);
	});

	it("rejects unknown signatures and anonymous declarations even when their names are excluded", async () => {
		const abi = [{ ...pingAbi[0], anonymous: true }, valueAbi[0]] as const;
		for (const signature of ["Ping()", "Unknown()"]) {
			await assert.rejects(
				processor().prepare(log(signature), { abi, skipEventNames: ["Ping", "Unknown"] }),
				DecodeError
			);
		}
		const anonymousRecord = log("Ping()");
		anonymousRecord.topics = [];
		await assert.rejects(
			processor().prepare(anonymousRecord, { abi, skipEventNames: ["Ping"] }),
			DecodeError
		);
	});

	it("selects distinct event overloads by signature", async () => {
		const abi = [
			{ type: "event", name: "Changed", inputs: [{ name: "value", type: "uint256", indexed: true }] },
			{ type: "event", name: "Changed", inputs: [{ name: "value", type: "string", indexed: false }] },
		] as const satisfies Abi;
		const registry = new HandlerRegistry();
		const handled: unknown[] = [];
		registry.registerLog("Changed", {
			normalize: (args) => args,
			handle: (data) => { handled.push(data); },
		});
		const numeric = log("Changed(uint256)");
		numeric.topics.push(encodeAbiParameters([{ type: "uint256" }], [17n]));
		const textual = log("Changed(string)", encodeAbiParameters([{ type: "string" }], ["value"]));

		await processor(registry).prepareAndExecute(numeric, { abi });
		await processor(registry).prepareAndExecute(textual, { abi });

		assert.deepEqual(handled, [{ value: 17n }, { value: "value" }]);
	});

	it("decodes uppercase hexadecimal topics and awaits synchronous handlers", async () => {
		const registry = new HandlerRegistry();
		let handled = false;
		registry.registerLog("Ping", {
			normalize: () => undefined,
			handle: () => { handled = true; },
		});
		const uppercaseTopicLog = log("Ping()");
		uppercaseTopicLog.topics = uppercaseTopicLog.topics.map((topic) => topic.replace(/[a-f]/g, (character) => character.toUpperCase())) as SyncContractLog["topics"];

		const prepared = await processor(registry).prepareAndExecute(uppercaseTopicLog, { abi: pingAbi });

		assert.equal(prepared?.eventName, "Ping");
		assert.equal(handled, true);
	});

	it("rejects a known event whose indexed topics do not satisfy its ABI", async () => {
		const registry = new HandlerRegistry();
		let normalized = false;
		registry.registerLog("Value", {
			normalize: () => {
				normalized = true;
				return undefined;
			},
			handle: async () => {},
		});
		const malformedData = encodeAbiParameters(
			[{ type: "uint256" }, { type: "uint256" }],
			[11n, 22n]
		);

		await assert.rejects(
			processor(registry).prepare(log("Value(uint256,uint256)", malformedData), { abi: valueAbi }),
			DecodeError
		);
		assert.equal(normalized, false);
	});

	it("still skips an explicitly named event after successful decoding", async () => {
		const prepared = await processor().prepare(log("Ping()"), {
			abi: pingAbi,
			skipEventNames: ["Ping"],
		});

		assert.equal(prepared, null);
	});

	it("does not let an exclusion bypass strict decoding", async () => {
		const malformedData = encodeAbiParameters(
			[{ type: "uint256" }, { type: "uint256" }],
			[11n, 22n]
		);

		await assert.rejects(
			processor().prepare(log("Value(uint256,uint256)", malformedData), {
				abi: valueAbi,
				skipEventNames: ["Value"],
			}),
			DecodeError
		);
	});

	it("rejects extra topics that viem strict decoding otherwise ignores", async () => {
		const malformed = log("Ping()");
		malformed.topics = [malformed.topics[0], `0x${"3".repeat(64)}`];

		await assert.rejects(
			processor().prepare(malformed, { abi: pingAbi, skipEventNames: ["Ping"] }),
			(error: unknown) => {
				assert.ok(error instanceof DecodeError);
				assert.match(error.message, /exactly 1 topic; received 2/);
				return true;
			}
		);
	});

	it("preserves useful details when a normalizer throws a non-Error value", async () => {
		const registry = new HandlerRegistry();
		registry.registerLog("Ping", {
			normalize: () => {
				throw { code: "E_NORMALIZE", detail: "invalid payload" };
			},
			handle: async () => {},
		});

		await assert.rejects(
			processor(registry).prepare(log("Ping()"), { abi: pingAbi }),
			(error: unknown) => {
				assert.ok(error instanceof NormalizationError);
				assert.equal(error.message, '{"code":"E_NORMALIZE","detail":"invalid payload"}');
				return true;
			}
		);
	});

	it("contains opaque decoder failures inside DecodeError", async () => {
		const thrown = Proxy.revocable({}, {});
		thrown.revoke();
		const hostileLog = log("Ping()");
		Object.defineProperty(hostileLog, "data", {
			get() {
				throw thrown.proxy;
			},
		});

		await assert.rejects(
			processor().prepare(hostileLog, { abi: pingAbi }),
			(error: unknown) => {
				assert.ok(error instanceof DecodeError);
				assert.equal(error.message, "[unserializable thrown value]");
				return true;
			}
		);
	});
});
