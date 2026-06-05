CREATE TABLE IF NOT EXISTS graph.wallet_token_edges
(
    wallet_address String,
    token_address String,
    first_seen AggregateFunction(min, DateTime),
    last_seen AggregateFunction(max, DateTime),
    volume_usd AggregateFunction(sum, Decimal(18, 4)),
    tx_count AggregateFunction(count),
    buy_count AggregateFunction(countIf, UInt8),
    sell_count AggregateFunction(countIf, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (wallet_address, token_address);

CREATE MATERIALIZED VIEW IF NOT EXISTS graph.mv_wallet_token_edges
TO graph.wallet_token_edges
AS
SELECT
    wallet_address,
    token_address,
    minState(block_time) AS first_seen,
    maxState(block_time) AS last_seen,
    sumState(amount_usd) AS volume_usd,
    countState() AS tx_count,
    countIfState(side = 'buy') AS buy_count,
    countIfState(side = 'sell') AS sell_count
FROM raw.dex_transactions
GROUP BY wallet_address, token_address;

CREATE TABLE IF NOT EXISTS graph.wallet_similarity_edges
(
    wallet_a String,
    wallet_b String,
    common_tokens UInt32,
    jaccard_tokens Float64,
    time_correlation Float64,
    similarity_score Float64,
    calculated_at DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (wallet_a, wallet_b);

CREATE TABLE IF NOT EXISTS graph.token_transition_edges
(
    from_token String,
    to_token String,
    support_wallets UInt32,
    transition_count UInt32,
    avg_return_proxy Float64,
    volume_usd Float64,
    confidence Float64,
    edge_weight Float64,
    calculated_at DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (from_token, to_token);

CREATE TABLE IF NOT EXISTS graph.token_spanning_tree_edges
(
    from_token String,
    to_token String,
    support_wallets UInt32,
    transition_count UInt32,
    edge_weight Float64,
    tree_rank UInt32,
    calculated_at DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (tree_rank, from_token, to_token);

CREATE TABLE IF NOT EXISTS graph.token_route_recommendations
(
    route_rank UInt32,
    path_tokens Array(String),
    path_symbols Array(String),
    hops UInt8,
    expected_return_proxy Float64,
    confidence Float64,
    route_weight Float64,
    support_wallets UInt32,
    calculated_at DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY route_rank;
