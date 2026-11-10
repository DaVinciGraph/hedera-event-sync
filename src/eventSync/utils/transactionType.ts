import type { TransactionTypes } from "@davincigraph/hedera-rest-client";

/** Preserve provider-specific names while rejecting accidental broad or multi-type filters. */
export function normalizeTransactionType(value: unknown): TransactionTypes | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !value.trim()) {
		throw new Error("transactionType must be a non-empty string when provided");
	}
	return value.trim();
}
