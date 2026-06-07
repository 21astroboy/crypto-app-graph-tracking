CREATE TABLE IF NOT EXISTS raw.wallet_watchlist
(
    wallet_address String,
    roi_30d Float64,
    realized_pnl_usd Decimal(18, 4),
    rank UInt32,
    source LowCardinality(String),
    version UInt64,
    is_deleted UInt8 DEFAULT 0,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY wallet_address;

CREATE TABLE IF NOT EXISTS raw.dex_transactions
(
    chain LowCardinality(String),
    block_time DateTime,
    block_date Date MATERIALIZED toDate(block_time),
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    wallet_address String,
    token_address String,
    pool_address String,
    event_type LowCardinality(String),
    side LowCardinality(String),
    amount_token Decimal(76, 18),
    amount_usd Decimal(18, 4),
    fee_usd Decimal(18, 4),
    source LowCardinality(String) DEFAULT 'demo',
    run_id String DEFAULT '',
    loaded_at DateTime DEFAULT now(),
    INDEX idx_token_address token_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_pool_address pool_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (wallet_address, token_address, block_time, tx_hash, log_index)
TTL block_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS raw.ingest_runs
(
    run_id String,
    source LowCardinality(String),
    started_at DateTime,
    finished_at DateTime,
    status LowCardinality(String),
    wallets_count UInt32,
    rows_inserted UInt64,
    tokens_priced UInt32,
    error String DEFAULT '',
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source, run_id);

CREATE TABLE IF NOT EXISTS raw.token_prices_hourly
(
    token_address String,
    price_hour DateTime,
    price_usd Float64,
    source LowCardinality(String),
    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(price_hour)
ORDER BY (token_address, price_hour);
