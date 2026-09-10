import type {
	ContractLog,
	ContractLogsPage,
	ContractLogTopics,
	Int64,
	NextPageFn,
	Transaction,
	TransactionsPage,
	TransactionTypes,
} from "@davincigraph/hedera-rest-client";

/**
 * Mirror log fields the sync pipeline requires for ordering, decoding, and
 * idempotent persistence. Raw REST response types remain deliberately loose;
 * RestClient validates and narrows them at the ingestion boundary.
 */
export type SyncContractLog = Omit<
	ContractLog,
	"address" | "block_number" | "contract_id" | "data" | "index" | "timestamp" | "topics" | "transaction_hash"
> & {
	address: string;
	/** Preserved when supplied by the Mirror Node; not required for sync identity or ordering. */
	block_number?: Int64;
	contract_id?: string;
	data: string | null;
	index: number;
	timestamp: string;
	topics: ContractLogTopics;
	/** Preserved and canonicalized when supplied by the Mirror Node; not required for sync identity. */
	transaction_hash?: string;
};

/** Normalized contract-log page with a recursively normalized continuation. */
export type SyncContractLogsPage = Omit<ContractLogsPage, "logs" | "next"> & {
	logs: SyncContractLog[];
	next: NextPageFn<SyncContractLogsPage>;
	syncWindowEnd?: string;
};

/** Mirror transaction fields validated and normalized at the ingestion boundary. */
export type SyncTransaction = Omit<
	Transaction,
	"consensus_timestamp" | "name" | "nft_transfers" | "nonce" | "token_transfers" | "transaction_hash" | "transaction_id" | "transfers"
> & {
	consensus_timestamp: string;
	name: TransactionTypes;
	nft_transfers: NonNullable<Transaction["nft_transfers"]>;
	nonce: number;
	token_transfers: NonNullable<Transaction["token_transfers"]>;
	/** Preserved when supplied by the Mirror Node; synchronization does not require it for identity or checkpoints. */
	transaction_hash?: string;
	/** Preserved when supplied by the Mirror Node; synchronization does not require it for identity or checkpoints. */
	transaction_id?: string;
	transfers: NonNullable<Transaction["transfers"]>;
};

/** Normalized Mirror Node record returned by a successful read cycle. */
export type SyncReadItem = SyncContractLog | SyncTransaction;

/** Normalized transaction page with a recursively normalized continuation. */
export type SyncTransactionsPage = Omit<TransactionsPage, "next" | "transactions"> & {
	transactions: SyncTransaction[];
	next: NextPageFn<SyncTransactionsPage>;
	syncWindowEnd?: string;
};
