import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { runInNewContext } from "node:vm";
import { errorDetails, serializeError } from "../src/eventSync/utils/error";

describe("error diagnostics", () => {
	it("preserves real errors from another JavaScript realm", () => {
		for (const kind of ["Error", "TypeError", "RangeError"]) {
			const error: unknown = runInNewContext(`new ${kind}("foreign failure")`);
			assert.equal(error instanceof Error, false);
			assert.deepEqual(errorDetails(error), { name: kind, message: "foreign failure" });
			assert.deepEqual(JSON.parse(serializeError(error)), { name: kind, message: "foreign failure" });
		}
	});

	it("preserves ordinary and proxied Error diagnostics and empty-message fallbacks", () => {
		assert.deepEqual(errorDetails(new Error("failure")), { name: "Error", message: "failure" });
		assert.deepEqual(errorDetails(new Proxy(new TypeError("proxied failure"), {})), {
			name: "TypeError", message: "proxied failure",
		});
		assert.deepEqual(errorDetails(new RangeError()), { name: "RangeError", message: "RangeError" });
	});

	it("does not mistake an ordinary object with error-like properties for an Error", () => {
		const thrown = { name: "CustomFailure", message: "details", code: 42 };
		assert.deepEqual(errorDetails(thrown), { name: "NonErrorThrown", message: JSON.stringify(thrown) });
	});

	it("retains non-Error primitives and circular objects safely", () => {
		const values: readonly [unknown, string][] = [
			["failure", "failure"], [null, "null"], [undefined, "undefined"],
			[42n, "42"], [Symbol("failure"), "failure"], [Symbol(), "Symbol()"],
			[false, "false"], [42, "42"],
		];
		for (const [thrown, message] of values) {
			assert.deepEqual(errorDetails(thrown), { name: "NonErrorThrown", message });
		}
		const circular: { self?: unknown } = {};
		circular.self = circular;
		assert.deepEqual(errorDetails(circular), { name: "NonErrorThrown", message: "[object Object]" });
	});

	it("contains revoked proxies and errors with hostile property access", () => {
		const revoked = Proxy.revocable({}, {});
		revoked.revoke();
		const hostile = new Error("hidden");
		Object.defineProperty(hostile, "message", { get() { throw new Error("access denied"); } });
		for (const thrown of [revoked.proxy, hostile]) {
			assert.deepEqual(errorDetails(thrown), {
				name: "NonErrorThrown", message: "[unserializable thrown value]",
			});
			assert.doesNotThrow(() => serializeError(thrown));
		}
	});
});
