import { types } from "node:util";

export type ErrorDetails = {
	name: string;
	message: string;
};

/** Normalizes every JavaScript throw value without assuming it is an Error. */
export function errorDetails(error: unknown): ErrorDetails {
	try {
		if (types.isNativeError(error) || error instanceof Error) {
			const errorName = error.name;
			const errorMessage = error.message;
			const name = typeof errorName === "string" && errorName ? errorName : "Error";
			const message =
				typeof errorMessage === "string" && errorMessage
					? errorMessage
					: typeof errorName === "string" && errorName
						? errorName
						: "Unknown error";
			return { name, message };
		}
	} catch {
		// Revoked or hostile proxies can throw from instanceof and property reads.
		// Treat them like any other opaque, non-Error throw value.
		return { name: "NonErrorThrown", message: safeString(error) };
	}

	if (typeof error === "string") {
		return { name: "NonErrorThrown", message: error };
	}

	if (error === null) return { name: "NonErrorThrown", message: "null" };
	if (error === undefined) return { name: "NonErrorThrown", message: "undefined" };
	if (typeof error === "bigint") return { name: "NonErrorThrown", message: error.toString() };
	if (typeof error === "symbol") return { name: "NonErrorThrown", message: error.description ?? error.toString() };

	try {
		const serialized = JSON.stringify(error);
		return {
			name: "NonErrorThrown",
			message: serialized === undefined ? safeString(error) : serialized,
		};
	} catch {
		return { name: "NonErrorThrown", message: safeString(error) };
	}
}

export function serializeError(error: unknown): string {
	return JSON.stringify(errorDetails(error));
}

function safeString(value: unknown): string {
	try {
		return String(value);
	} catch {
		return "[unserializable thrown value]";
	}
}
