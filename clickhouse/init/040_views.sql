CREATE VIEW IF NOT EXISTS mart.v_wallet_daily_activity AS
SELECT
    wallet_address,
    activity_date,
    countMerge(tx_count) AS tx_count,
    uniqMerge(tokens_traded) AS tokens_traded,
    uniqMerge(pools_touched) AS pools_touched,
    sumMerge(volume_usd) AS volume_usd,
    countIfMerge(buy_count) AS buy_count,
    countIfMerge(sell_count) AS sell_count,
    minMerge(first_tx_time) AS first_tx_time,
    maxMerge(last_tx_time) AS last_tx_time
FROM mart.wallet_daily_activity
GROUP BY wallet_address, activity_date;

CREATE VIEW IF NOT EXISTS mart.v_first_wallet_buys AS
SELECT
    wallet_address,
    token_address,
    minMerge(first_buy_time) AS first_buy_time,
    argMinMerge(first_buy_amount_usd) AS first_buy_amount_usd,
    argMinMerge(first_buy_amount_token) AS first_buy_amount_token
FROM mart.first_wallet_buys
GROUP BY wallet_address, token_address;

CREATE VIEW IF NOT EXISTS graph.v_wallet_token_edges AS
SELECT
    wallet_address,
    token_address,
    minMerge(first_seen) AS first_seen,
    maxMerge(last_seen) AS last_seen,
    sumMerge(volume_usd) AS volume_usd,
    countMerge(tx_count) AS tx_count,
    countIfMerge(buy_count) AS buy_count,
    countIfMerge(sell_count) AS sell_count
FROM graph.wallet_token_edges
GROUP BY wallet_address, token_address;

CREATE VIEW IF NOT EXISTS mart.v_ingest_health AS
SELECT
    source,
    run_id,
    min(loaded_at) AS first_loaded_at,
    max(loaded_at) AS last_loaded_at,
    count() AS raw_rows,
    uniqExact(wallet_address) AS wallets,
    uniqExact(token_address) AS tokens,
    uniqExact(tx_hash) AS tx_hashes,
    countIf(event_type IN ('uniswap_v2_swap', 'uniswap_v3_swap')) AS decoded_swap_rows,
    countIf(amount_usd = 0) AS rows_without_usd
FROM raw.dex_transactions
GROUP BY source, run_id;
