-- ClickHouse Smart Wallet Profiler demo queries.
-- Run this file in ClickHouse connections.

-- ============================================================================
-- 00. Project run summary: one-screen system passport.
-- ============================================================================
SELECT
    metric,
    value,
    note
FROM mart.v_project_run_summary
ORDER BY metric;

-- ============================================================================
-- 00b. Layer health: quick sanity check.
-- ============================================================================
SELECT
    'raw.dex_transactions' AS object_name,
    count() AS rows
FROM raw.dex_transactions
UNION ALL
SELECT
    'mart.wallet_token_balances',
    count()
FROM mart.wallet_token_balances
UNION ALL
SELECT
    'graph.wallet_token_edges',
    count()
FROM graph.wallet_token_edges
UNION ALL
SELECT
    'graph.wallet_similarity_edges',
    count()
FROM graph.wallet_similarity_edges;

-- ============================================================================
-- 01. Engines used in the project.
-- ============================================================================
SELECT
    database,
    name,
    engine,
    sorting_key,
    partition_key
FROM system.tables
WHERE database IN ('raw', 'mart', 'graph')
ORDER BY database, name;

-- ============================================================================
-- 02. Dictionaries: Postgres-backed metadata and ClickHouse-backed prices.
-- ============================================================================
SELECT
    database,
    name,
    status,
    element_count
FROM system.dictionaries
WHERE database = 'mart'
ORDER BY name;

-- ============================================================================
-- 03. Raw fact table: append-only events on MergeTree.
-- ============================================================================
SELECT
    source,
    count() AS tx_count,
    min(block_time) AS first_tx,
    max(block_time) AS last_tx,
    uniqExact(wallet_address) AS wallets,
    uniqExact(token_address) AS tokens,
    sum(amount_usd) AS volume_usd
FROM raw.dex_transactions
GROUP BY source
ORDER BY tx_count DESC;

-- ============================================================================
-- 04. Real ingest event mix: demo events, ERC-20 transfers, decoded swaps.
-- ============================================================================
SELECT
    source,
    event_type,
    side,
    count() AS events,
    sum(amount_usd) AS volume_usd
FROM raw.dex_transactions
GROUP BY source, event_type, side
ORDER BY events DESC;

-- ============================================================================
-- 05. Latest wallet watchlist state on ReplacingMergeTree.
-- ============================================================================
SELECT
    wallet_address,
    rank,
    round(roi_30d, 3) AS roi_30d,
    realized_pnl_usd,
    source,
    updated_at
FROM raw.wallet_watchlist FINAL
WHERE is_deleted = 0
ORDER BY source, rank
LIMIT 30;

-- ============================================================================
-- 06. Wallet balances from SummingMergeTree.
-- ============================================================================
SELECT
    wallet_address,
    token_address,
    balance_token,
    net_flow_usd,
    buy_usd,
    sell_usd,
    tx_count
FROM mart.wallet_token_balances
ORDER BY abs(net_flow_usd) DESC
LIMIT 20;

-- ============================================================================
-- 07. Daily activity from AggregatingMergeTree states.
-- ============================================================================
SELECT
    wallet_address,
    activity_date,
    tx_count,
    tokens_traded,
    pools_touched,
    volume_usd,
    buy_count,
    sell_count
FROM mart.v_wallet_daily_activity
ORDER BY activity_date DESC, volume_usd DESC
LIMIT 30;

-- ============================================================================
-- 08. Smart-money inflow by token.
-- ============================================================================
SELECT
    token_address,
    sum(smart_buy_usd) AS buy_usd,
    sum(smart_sell_usd) AS sell_usd,
    sum(net_flow_usd) AS net_flow_usd,
    sum(tx_count) AS tx_count
FROM mart.token_smart_money_flow_5m
GROUP BY token_address
ORDER BY net_flow_usd DESC
LIMIT 15;

-- ============================================================================
-- 09. First buys are calculated with minState/argMinState.
-- ============================================================================
SELECT
    token_address,
    wallet_address,
    first_buy_time,
    first_buy_amount_usd,
    first_buy_amount_token
FROM mart.v_first_wallet_buys
ORDER BY first_buy_time
LIMIT 30;

-- ============================================================================
-- 12. Dictionary lookup: token symbol and hourly price.
-- ============================================================================
WITH
    (SELECT token_address FROM raw.token_prices_hourly LIMIT 1) AS token,
    (SELECT price_hour FROM raw.token_prices_hourly WHERE token_address = token LIMIT 1) AS hour
SELECT
    token,
    dictGet('mart.tokens_dict', 'symbol', token) AS symbol,
    hour,
    dictGet('mart.prices_dict', 'price_usd', (token, hour)) AS price_usd;

-- ============================================================================
-- 13. Latest token prices loaded by demo or price job.
-- ============================================================================
SELECT
    token_address,
    argMax(price_usd, loaded_at) AS latest_price_usd,
    max(price_hour) AS latest_price_hour,
    argMax(source, loaded_at) AS latest_source,
    max(loaded_at) AS latest_loaded_at
FROM raw.token_prices_hourly
GROUP BY token_address
ORDER BY latest_loaded_at DESC
LIMIT 30;

-- ============================================================================
-- 14. Recently seen tokens without a fresh hourly price.
-- ============================================================================
WITH latest_prices AS
(
    SELECT
        token_address AS priced_token_address,
        max(loaded_at) AS last_price_loaded_at
    FROM raw.token_prices_hourly
    GROUP BY token_address
)
SELECT
    tx.token_address,
    count() AS raw_events,
    max(tx.block_time) AS last_event_time,
    lp.last_price_loaded_at
FROM raw.dex_transactions AS tx
LEFT JOIN latest_prices AS lp
    ON tx.token_address = lp.priced_token_address
WHERE tx.block_time >= now() - INTERVAL 30 DAY
  AND (
      lp.priced_token_address = ''
      OR lp.last_price_loaded_at < now() - INTERVAL 12 HOUR
  )
GROUP BY
    tx.token_address,
    lp.last_price_loaded_at
ORDER BY raw_events DESC
LIMIT 30;

-- ============================================================================
-- 15. Top wallet similarity edges from graph job.
-- ============================================================================
SELECT
    wallet_a,
    wallet_b,
    common_tokens,
    round(jaccard_tokens, 3) AS jaccard_tokens,
    round(time_correlation, 3) AS time_correlation,
    round(similarity_score, 2) AS similarity_score,
    calculated_at
FROM graph.wallet_similarity_edges FINAL
ORDER BY similarity_score DESC
LIMIT 30;

-- ============================================================================
-- 16. Ego graph for the strongest wallet.
-- ============================================================================
WITH
    (
        SELECT wallet_a
        FROM graph.wallet_similarity_edges FINAL
        ORDER BY similarity_score DESC
        LIMIT 1
    ) AS seed_wallet
SELECT
    seed_wallet,
    if(wallet_a = seed_wallet, wallet_b, wallet_a) AS neighbor_wallet,
    common_tokens,
    round(jaccard_tokens, 3) AS jaccard_tokens,
    round(time_correlation, 3) AS time_correlation,
    round(similarity_score, 2) AS similarity_score
FROM graph.wallet_similarity_edges FINAL
WHERE wallet_a = seed_wallet OR wallet_b = seed_wallet
ORDER BY similarity_score DESC
LIMIT 20;

-- ============================================================================
-- 17. Explain one similarity edge through common tokens.
-- ============================================================================
WITH
    (
        SELECT wallet_a
        FROM graph.wallet_similarity_edges FINAL
        ORDER BY similarity_score DESC
        LIMIT 1
    ) AS a,
    (
        SELECT wallet_b
        FROM graph.wallet_similarity_edges FINAL
        ORDER BY similarity_score DESC
        LIMIT 1
    ) AS b
SELECT
    e1.token_address,
    e1.volume_usd AS wallet_a_volume_usd,
    e2.volume_usd AS wallet_b_volume_usd,
    e1.last_seen AS wallet_a_last_seen,
    e2.last_seen AS wallet_b_last_seen
FROM graph.v_wallet_token_edges AS e1
JOIN graph.v_wallet_token_edges AS e2 USING (token_address)
WHERE e1.wallet_address = a
  AND e2.wallet_address = b
ORDER BY least(e1.volume_usd, e2.volume_usd) DESC
LIMIT 20;

-- ============================================================================
-- 18. Ingest run log.
-- ============================================================================
SELECT
    source,
    run_id,
    started_at,
    finished_at,
    status,
    wallets_count,
    rows_inserted,
    tokens_priced,
    error
FROM raw.ingest_runs FINAL
ORDER BY started_at DESC;

-- ============================================================================
-- 19. Token transition graph: historically strong token-to-token moves.
-- ============================================================================
SELECT
    from_token,
    to_token,
    support_wallets,
    transition_count,
    round(avg_return_proxy, 3) AS avg_return_proxy,
    round(confidence, 3) AS confidence,
    round(edge_weight, 2) AS edge_weight
FROM graph.token_transition_edges FINAL
ORDER BY edge_weight DESC
LIMIT 30;

-- ============================================================================
-- 20. Maximum spanning tree over token transition graph.
-- ============================================================================
SELECT
    tree_rank,
    from_token,
    to_token,
    support_wallets,
    transition_count,
    round(edge_weight, 2) AS edge_weight
FROM graph.token_spanning_tree_edges FINAL
ORDER BY tree_rank
LIMIT 30;

-- ============================================================================
-- 21. Top token route recommendations.
-- ============================================================================
SELECT
    route_rank,
    path_symbols,
    hops,
    round(expected_return_proxy, 3) AS expected_return_proxy,
    round(confidence, 3) AS confidence,
    round(route_weight, 2) AS route_weight,
    support_wallets
FROM graph.token_route_recommendations FINAL
ORDER BY route_rank
LIMIT 30;

-- ============================================================================
-- 22. Ingest health by source/run_id.
-- ============================================================================
SELECT
    source,
    run_id,
    first_loaded_at,
    last_loaded_at,
    raw_rows,
    wallets,
    tokens,
    tx_hashes,
    decoded_swap_rows,
    rows_without_usd
FROM mart.v_ingest_health
ORDER BY last_loaded_at DESC;

-- ============================================================================
-- 23. Data skipping indexes on the raw fact table.
-- ============================================================================
SELECT
    name,
    type,
    expr,
    granularity
FROM system.data_skipping_indices
WHERE database = 'raw'
  AND table = 'dex_transactions'
ORDER BY name;

-- ============================================================================
-- 24. Real decoded swaps by wallet and pool.
-- ============================================================================
SELECT
    wallet_address,
    pool_address,
    tx_hash,
    groupArray((side, token_address, amount_token, amount_usd)) AS swap_legs
FROM raw.dex_transactions
WHERE event_type IN ('uniswap_v2_swap', 'uniswap_v3_swap')
GROUP BY wallet_address, pool_address, tx_hash
ORDER BY max(block_time) DESC
LIMIT 20;
