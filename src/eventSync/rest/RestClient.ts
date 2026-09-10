import type { ContractLogTopicFilter, ContractLogTopicsFilter, NetworkName } from "../types/config";
import type {
	ContractLog,
	ContractLogsPage,
	Int64,
	RequestOptions,
	Transaction,
	TransactionResultFilter,
	TransactionsPage,
	TransactionTypes,
} from "@davincigraph/hedera-rest-client";
import { HederaRestClient } from "@davincigraph/hedera-rest-client";
import type { SyncContractLog, SyncContractLogsPage, SyncTransaction, SyncTransactionsPage } from "../types/mirror";
import { normalizeHederaTimestamp, parseHederaTimestampNs } from "../utils/timestamp";
import { boundedTopicLogWindowEnd, hasContractLogTopicFilters } from "../utils/topicLogWindow";

/** Parameters used by the synchronization engine to read global contract logs. */
export type GlobalLogsParams = {
	provider: string;
	network: NetworkName;
	topics?: ContractLogTopicsFilter;
	cursor: { timestamp: string; index: number };
	timestampGte?: string;
	timestampLte?: string;
	batchSize: number;
	signal?: AbortSignal;
};

/** Global log page annotated with the end of its bounded topic window. */
export type GlobalContractLogsPage = SyncContractLogsPage & {
	syncWindowEnd?: string;
};

/** Parameters used by the synchronization engine to read one contract's logs. */
export type ContractLogsParams = {
	provider: string;
	network: NetworkName;
	address: string;
	topics?: ContractLogTopicsFilter;
	cursor: { timestamp: string; index: number };
	timestampGte?: string;
	timestampLte?: string;
	batchSize: number;
	signal?: AbortSignal;
};

/** Parameters used by the synchronization engine to read account transactions. */
export type RestClientTransactionsParams = {
	accountId: string;
	timestampGte: string;
	timestampLte?: string;
	pageSize: number;
	provider: string;
	network: NetworkName;
	result?: TransactionResultFilter | null;
	signal?: AbortSignal;
};

/** Diagnostic options for the package-owned Mirror Node REST facade. */
export type RestClientOptions = {
	/** Emits concise request diagnostics to the console. Defaults to false. */
	debug?: boolean;
};

/**
 * Package-owned, low-level facade over `@davincigraph/hedera-rest-client`.
 * It selects the configured provider and network, applies sync-safe ordering and
 * time bounds, and validates response fields required by the synchronization
 * pipeline. Most consumers should use `HederaEventSync` instead.
 */
export class RestClient {
	/** Wraps a configured Hedera REST client without taking ownership of it. */
	constructor(public readonly client: HederaRestClient, private readonly opts: RestClientOptions = {}) {}

	/** Fetches the first normalized page of globally filtered contract logs. */
	async fetchGlobalLogs(p: GlobalLogsParams): Promise<GlobalContractLogsPage> {
		const { provider, network, topics, cursor, batchSize, signal } = p;
		assertSupportedTopicCount(topics);
		const timestampGte = p.timestampGte ?? cursor.timestamp;
		if (!hasContractLogTopicFilters(topics)) assertTimestampRange(timestampGte, p.timestampLte);
		const windowEnd = hasContractLogTopicFilters(topics) ? boundedTopicLogWindowEnd(timestampGte, p.timestampLte) : p.timestampLte;
		const c = this.client.useProvider(provider).useNetwork(network).contracts();

		const operation = c
			.globalLogs((q) => {
				let qb = q;
				if (hasTopicValue(topics?.[0])) qb = qb.topic0(...topicValues(topics[0]));
				if (hasTopicValue(topics?.[1])) qb = qb.topic1(...topicValues(topics[1]));
				if (hasTopicValue(topics?.[2])) qb = qb.topic2(...topicValues(topics[2]));
				if (hasTopicValue(topics?.[3])) qb = qb.topic3(...topicValues(topics[3]));

				qb = qb.timestamp().greaterThanOrEqualTo(timestampGte);
				if (windowEnd) qb = qb.timestamp().lessThanOrEqualTo(windowEnd);
				qb = qb.limit(batchSize);
				qb = qb.order("asc");
				return qb;
			});

		const page = normalizeContractLogsPage(await operation.get(signal ? { signal } : undefined)) as GlobalContractLogsPage;
		if (windowEnd) page.syncWindowEnd = windowEnd;

		if (this.opts.debug) {
			// eslint-disable-next-line no-console
			console.log(`[REST] globalLogs ${provider}/${network} ts>=${timestampGte}${windowEnd ? ` ts<=${windowEnd}` : ""} -> ${page.logs.length} logs`);
		}
		return page;
	}

	/** Fetches the first normalized page of logs for one contract identifier. */
	async fetchContractLogs(p: ContractLogsParams): Promise<SyncContractLogsPage> {
		const { provider, network, address, topics, cursor, batchSize, signal } = p;
		assertSupportedTopicCount(topics);
		const timestampGte = p.timestampGte ?? cursor.timestamp;
		if (!hasContractLogTopicFilters(topics)) assertTimestampRange(timestampGte, p.timestampLte);
		const windowEnd = hasContractLogTopicFilters(topics) ? boundedTopicLogWindowEnd(timestampGte, p.timestampLte) : p.timestampLte;
		const c = this.client.useProvider(provider).useNetwork(network).contracts();

		const operation = c
			.logs((q) => {
				let qb = q.idOrAddress(address).timestamp().greaterThanOrEqualTo(timestampGte);
				if (windowEnd) qb = qb.timestamp().lessThanOrEqualTo(windowEnd);
				if (hasTopicValue(topics?.[0])) qb = qb.topic0(...topicValues(topics[0]));
				if (hasTopicValue(topics?.[1])) qb = qb.topic1(...topicValues(topics[1]));
				if (hasTopicValue(topics?.[2])) qb = qb.topic2(...topicValues(topics[2]));
				if (hasTopicValue(topics?.[3])) qb = qb.topic3(...topicValues(topics[3]));
				qb = qb.limit(batchSize);
				qb = qb.order("asc");
				return qb;
			});

		const page = normalizeContractLogsPage(await operation.get(signal ? { signal } : undefined));
		if (windowEnd) page.syncWindowEnd = windowEnd;

		if (this.opts.debug) {
			// eslint-disable-next-line no-console
			console.log(`[REST] logs addr=${address} ${provider}/${network} ts>=${timestampGte}${windowEnd ? ` ts<=${windowEnd}` : ""} -> ${page.logs.length}`);
		}
		return page;
	}

	/** Fetches the first normalized page of transactions for one account identifier. */
	async fetchTransactions(p: RestClientTransactionsParams): Promise<SyncTransactionsPage> {
		const { provider, network, accountId, timestampGte, timestampLte, pageSize, result = "success", signal } = p;
		assertTimestampRange(timestampGte, timestampLte);
		const t = this.client.useProvider(provider).useNetwork(network).transactions();

		const operation = t.list((q) => {
				let qb = q.accountId(accountId).timestamp().greaterThanOrEqualTo(timestampGte);
				if (timestampLte) qb = qb.timestamp().lessThanOrEqualTo(timestampLte);
				if (result !== null) qb = qb.result(result);
				qb = qb.limit(pageSize);
				qb = qb.order("asc");
				return qb;
			});

		const page = normalizeTransactionsPage(await operation.get(signal ? { signal } : undefined));
		if (timestampLte) page.syncWindowEnd = timestampLte;

		if (this.opts.debug) {
			// eslint-disable-next-line no-console
			console.log(`[REST] txs id=${accountId} ${provider}/${network} ts>=${timestampGte} -> ${page.transactions.length}`);
		}

		return page;
	}
}

function normalizeContractLogsPage(page: ContractLogsPage): SyncContractLogsPage {
	if (!page || typeof page !== "object" || !Array.isArray(page.logs)) {
		throw new Error("Invalid mirror logs page: logs must be an array");
	}
	const next = Object.assign(
		async (options?: RequestOptions) => {
			const nextPage = await page.next(options);
			return nextPage ? normalizeContractLogsPage(nextPage) : null;
		},
		{ url: () => page.next.url() }
	);
	return {
		...page,
		logs: (page.logs ?? []).map(normalizeContractLog),
		next,
	};
}

function normalizeContractLog(log: ContractLog, position: number): SyncContractLog {
	const address = requiredMatchingString(log.address, "address", position, /^0x[0-9a-fA-F]{40}$/).toLowerCase();
	const timestamp = normalizeHederaTimestamp(requiredString(log.timestamp, "timestamp", position));
	const rawTransactionHash = optionalMatchingString(log.transaction_hash, "transaction_hash", position, /^0x(?:[0-9a-fA-F]{2})*$/);
	const transactionHash = rawTransactionHash === undefined || rawTransactionHash === "0x"
		? undefined
		: rawTransactionHash.toLowerCase();
	if (!Number.isSafeInteger(log.index) || (log.index as number) < 0) {
		throw invalidLogField("index", position);
	}
	if (log.block_number !== undefined && log.block_number !== null && !isNonNegativeInt64(log.block_number)) {
		throw invalidLogField("block_number", position);
	}
	const normalizedData = optionalMatchingStringOrNull(log.data, "data", position, /^0x(?:[0-9a-fA-F]{2})*$/);
	const data = normalizedData?.toLowerCase() ?? null;
	const topics = matchingStringArray(log.topics, "topics", position, /^0x[0-9a-fA-F]{64}$/, 4).map((topic) => topic.toLowerCase());

	return {
		...log,
		address,
		block_number: log.block_number ?? undefined,
		contract_id: typeof log.contract_id === "string" ? log.contract_id : undefined,
		data,
		index: log.index as number,
		timestamp,
		topics: topics as SyncContractLog["topics"],
		transaction_hash: transactionHash,
	};
}

function requiredString(value: unknown, field: string, position: number): string {
	if (typeof value !== "string" || value.length === 0) throw invalidLogField(field, position);
	return value;
}

function requiredMatchingString(value: unknown, field: string, position: number, pattern: RegExp): string {
	const stringValue = requiredString(value, field, position);
	if (!pattern.test(stringValue)) throw invalidLogField(field, position);
	return stringValue;
}

function invalidLogField(field: string, position: number): Error {
	return new Error(`Invalid mirror log at position ${position}: missing or invalid ${field}`);
}

function normalizeTransactionsPage(page: TransactionsPage): SyncTransactionsPage {
	if (!page || typeof page !== "object" || !Array.isArray(page.transactions)) {
		throw new Error("Invalid mirror transactions page: transactions must be an array");
	}
	const next = Object.assign(
		async (options?: RequestOptions) => {
			const nextPage = await page.next(options);
			return nextPage ? normalizeTransactionsPage(nextPage) : null;
		},
		{ url: () => page.next.url() }
	);
	return {
		...page,
		transactions: (page.transactions ?? []).map(normalizeTransaction),
		next,
	};
}

function normalizeTransaction(transaction: Transaction, position: number): SyncTransaction {
	const consensusTimestamp = normalizeHederaTimestamp(requiredTransactionString(transaction.consensus_timestamp, "consensus_timestamp", position));
	const name = requiredTransactionString(transaction.name, "name", position) as TransactionTypes;
	const transactionHash = optionalTransactionString(transaction.transaction_hash, "transaction_hash", position);
	const transactionId = optionalTransactionString(transaction.transaction_id, "transaction_id", position);
	const nonce = transaction.nonce ?? 0;
	if (!Number.isSafeInteger(nonce) || nonce < 0) throw invalidTransactionField("nonce", position);
	const nftTransfers = recordArrayOrEmpty<NonNullable<Transaction["nft_transfers"]>>(transaction.nft_transfers, "nft_transfers", position, isNftTransfer);
	const tokenTransfers = recordArrayOrEmpty<NonNullable<Transaction["token_transfers"]>>(transaction.token_transfers, "token_transfers", position, isTokenTransfer);
	const transfers = recordArrayOrEmpty<NonNullable<Transaction["transfers"]>>(transaction.transfers, "transfers", position, isTransfer);

	return {
		...transaction,
		consensus_timestamp: consensusTimestamp,
		name,
		nft_transfers: nftTransfers,
		nonce,
		token_transfers: tokenTransfers,
		transaction_hash: transactionHash,
		transaction_id: transactionId,
		transfers,
	};
}

function requiredTransactionString(value: unknown, field: string, position: number): string {
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`Invalid mirror transaction at position ${position}: missing or invalid ${field}`);
	}
	return value;
}

function optionalTransactionString(value: unknown, field: string, position: number): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.length === 0) throw invalidTransactionField(field, position);
	return value;
}

function optionalMatchingString(value: unknown, field: string, position: number, pattern: RegExp): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !pattern.test(value)) throw invalidLogField(field, position);
	return value;
}

function optionalStringOrNull(value: unknown, field: string, position: number): string | null {
	if (value === undefined || value === null) return null;
	if (typeof value !== "string") throw invalidLogField(field, position);
	return value;
}

function optionalMatchingStringOrNull(value: unknown, field: string, position: number, pattern: RegExp): string | null {
	const stringValue = optionalStringOrNull(value, field, position);
	if (stringValue !== null && !pattern.test(stringValue)) throw invalidLogField(field, position);
	return stringValue;
}

function stringArray(value: unknown, field: string, position: number, maximumLength?: number): string[] {
	if (value === undefined || value === null) return [];
	if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) throw invalidLogField(field, position);
	if (maximumLength !== undefined && value.length > maximumLength) throw invalidLogField(field, position);
	return [...value];
}

function matchingStringArray(value: unknown, field: string, position: number, pattern: RegExp, maximumLength?: number): string[] {
	const values = stringArray(value, field, position, maximumLength);
	if (values.some((item) => !pattern.test(item))) throw invalidLogField(field, position);
	return values;
}

function recordArrayOrEmpty<T extends readonly unknown[]>(
	value: unknown,
	field: string,
	position: number,
	isItem: (item: unknown) => boolean
): T {
	if (value === undefined || value === null) return [] as unknown as T;
	if (!Array.isArray(value) || value.some((item) => !isItem(item))) throw invalidTransactionField(field, position);
	return [...value] as unknown as T;
}

function isNftTransfer(value: unknown): boolean {
	if (!isRecord(value)) return false;
	return typeof value.is_approval === "boolean"
		&& isEntityId(value.receiver_account_id)
		&& isEntityId(value.sender_account_id)
		&& isPositiveInt64(value.serial_number)
		&& isEntityId(value.token_id);
}

function isTokenTransfer(value: unknown): boolean {
	if (!isRecord(value)) return false;
	return isEntityId(value.token_id)
		&& isEntityId(value.account)
		&& isInt64(value.amount)
		&& (value.is_approval === undefined || typeof value.is_approval === "boolean");
}

function isTransfer(value: unknown): boolean {
	if (!isRecord(value)) return false;
	return isEntityId(value.account)
		&& isInt64(value.amount)
		&& (value.is_approval === undefined || typeof value.is_approval === "boolean");
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return !!value && typeof value === "object" && !Array.isArray(value);
}

function isEntityId(value: unknown): boolean {
	return value === null || typeof value === "string";
}

function isInt64(value: unknown): boolean {
	if (typeof value === "number") return Number.isSafeInteger(value);
	return typeof value === "string" && /^-?(?:0|[1-9]\d*)$/.test(value);
}

function isNonNegativeInt64(value: unknown): value is Int64 {
	if (typeof value === "number") return Number.isSafeInteger(value) && value >= 0;
	return typeof value === "string" && /^(?:0|[1-9]\d*)$/.test(value);
}

function isPositiveInt64(value: unknown): value is Int64 {
	if (typeof value === "number") return Number.isSafeInteger(value) && value > 0;
	return typeof value === "string" && /^[1-9]\d*$/.test(value);
}

function invalidTransactionField(field: string, position: number): Error {
	return new Error(`Invalid mirror transaction at position ${position}: missing or invalid ${field}`);
}

function assertTimestampRange(startTimestamp: string, requestedEnd?: string): void {
	if (requestedEnd === undefined) return;
	if (parseHederaTimestampNs(requestedEnd) < parseHederaTimestampNs(startTimestamp)) {
		throw new RangeError(`Invalid timestamp range: end ${requestedEnd} is before start ${startTimestamp}`);
	}
}

function hasTopicValue(value: ContractLogTopicFilter | undefined): value is ContractLogTopicFilter {
	return typeof value === "string" ? value.length > 0 : Array.isArray(value) && value.length > 0;
}

function topicValues(value: ContractLogTopicFilter): string[] {
	return typeof value === "string" ? [value] : [...value];
}

function assertSupportedTopicCount(topics: readonly unknown[] | undefined): void {
	if (topics && topics.length > 4) {
		throw new RangeError("Invalid topics: Mirror Node log queries support at most four topic positions");
	}
}
