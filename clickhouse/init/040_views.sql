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

CREATE OR REPLACE VIEW mart.v_project_run_summary AS
WITH
    (
        SELECT run_id
        FROM raw.ingest_runs FINAL
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS latest_job_run_id,
    (
        SELECT source
        FROM raw.ingest_runs FINAL
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS latest_job_source,
    (
        SELECT status
        FROM raw.ingest_runs FINAL
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS latest_job_status,
    (
        SELECT finished_at
        FROM raw.ingest_runs FINAL
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS latest_job_finished_at,
    (
        SELECT run_id
        FROM raw.ingest_runs FINAL
        WHERE source = 'etherscan_real'
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS real_run_id,
    (
        SELECT rows_inserted
        FROM raw.ingest_runs FINAL
        WHERE source = 'etherscan_real'
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS real_rows_inserted,
    (
        SELECT wallets_count
        FROM raw.ingest_runs FINAL
        WHERE source = 'etherscan_real'
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS real_wallets_count,
    (
        SELECT status
        FROM raw.ingest_runs FINAL
        WHERE source = 'etherscan_real'
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS real_status,
    (
        SELECT finished_at
        FROM raw.ingest_runs FINAL
        WHERE source = 'etherscan_real'
        ORDER BY finished_at DESC
        LIMIT 1
    ) AS real_finished_at,
    (
        SELECT count()
        FROM raw.dex_transactions
        WHERE source != 'demo'
    ) AS raw_rows_kept
SELECT
    'latest_job_source' AS metric,
    toString(latest_job_source) AS value,
    'Самая последняя job в raw.ingest_runs: real ingest, price job и т.д.' AS note
UNION ALL
SELECT
    'latest_job_run_id',
    toString(latest_job_run_id),
    'Run id самой последней job.'
UNION ALL
SELECT
    'latest_job_status',
    toString(latest_job_status),
    'success/failed для самой последней job.'
UNION ALL
SELECT
    'latest_job_finished_at',
    toString(latest_job_finished_at),
    'Когда закончилась самая последняя job.'
UNION ALL
SELECT
    'real_ingest_run_id',
    toString(real_run_id),
    'Последний Etherscan real ingestion run.'
UNION ALL
SELECT
    'real_ingest_status',
    toString(real_status),
    'success/failed для последнего Etherscan real ingestion.'
UNION ALL
SELECT
    'real_ingest_finished_at',
    toString(real_finished_at),
    'Когда закончился последний Etherscan real ingestion.'
UNION ALL
SELECT
    'real_ingest_wallets_requested',
    toString(real_wallets_count),
    'Сколько кошельков real-ingest пытался загрузить.'
UNION ALL
SELECT
    'real_ingest_rows_reported',
    toString(real_rows_inserted),
    'Сколько raw-событий real-ingest отправил в ClickHouse.'
UNION ALL
SELECT
    'raw_rows_kept_after_ttl',
    toString(raw_rows_kept),
    'Сколько строк осталось в raw.dex_transactions после TTL-окна.'
UNION ALL
SELECT
    'raw_rows_trimmed_by_ttl_estimate',
    toString(greatest(toInt64(real_rows_inserted) - toInt64(raw_rows_kept), 0)),
    'Оценка: сколько real-ingest строк загрузили, но не удержали из-за TTL/окна хранения.'
UNION ALL
SELECT
    'raw_wallets',
    toString(uniqExact(wallet_address)),
    'Уникальные кошельки в текущем raw-окне.'
FROM raw.dex_transactions
UNION ALL
SELECT
    'raw_tokens',
    toString(uniqExact(token_address)),
    'Уникальные токены в текущем raw-окне.'
FROM raw.dex_transactions
UNION ALL
SELECT
    'raw_time_window',
    concat(toString(min(block_time)), ' .. ', toString(max(block_time))),
    'Временной диапазон текущего raw-окна.'
FROM raw.dex_transactions
UNION ALL
SELECT
    'raw_rows_without_usd',
    toString(countIf(amount_usd = 0)),
    'Строки без USD-оценки; обычно значит, что price API не дал цену.'
FROM raw.dex_transactions
UNION ALL
SELECT
    'priced_tokens',
    toString(uniqExact(token_address)),
    'Токены, для которых есть цена в raw.token_prices_hourly.'
FROM raw.token_prices_hourly
UNION ALL
SELECT
    'mart_wallet_token_balances_rows',
    toString(count()),
    'Строки в балансной витрине mart.wallet_token_balances.'
FROM mart.wallet_token_balances
UNION ALL
SELECT
    'mart_wallet_daily_activity_rows',
    toString(count()),
    'Строки в дневной activity-витрине mart.wallet_daily_activity.'
FROM mart.wallet_daily_activity
UNION ALL
SELECT
    'mart_wallet_ratings_rows',
    toString(count()),
    'Строки в рейтингах кошельков.'
FROM mart.wallet_ratings_latest
UNION ALL
SELECT
    'graph_wallet_similarity_edges',
    toString(count()),
    'Ребра wallet-wallet similarity graph.'
FROM graph.wallet_similarity_edges FINAL
UNION ALL
SELECT
    'graph_token_transition_edges',
    toString(count()),
    'Ребра token-token transition graph.'
FROM graph.token_transition_edges FINAL
UNION ALL
SELECT
    'graph_token_spanning_tree_edges',
    toString(count()),
    'Ребра maximum spanning forest по token graph.'
FROM graph.token_spanning_tree_edges FINAL
UNION ALL
SELECT
    'graph_token_routes',
    toString(count()),
    'Количество рассчитанных token routes.'
FROM graph.token_route_recommendations FINAL
UNION ALL
SELECT
    'graph_top_token_route',
    if(count() = 0, '', any(arrayStringConcat(path_symbols, ' -> '))),
    'Лучший route по graph.token_route_recommendations.'
FROM
(
    SELECT path_symbols
    FROM graph.token_route_recommendations FINAL
    ORDER BY route_rank
    LIMIT 1
);
