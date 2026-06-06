-- ClickHouse Smart Wallet Profiler PostgreSQL demo queries.
-- Run this file in DataGrip connection: PG - metadata.

-- ============================================================================
-- 01. Discovery candidates stored in PostgreSQL metadata.
-- ============================================================================
SELECT
    wallet_address,
    source,
    score,
    roi_proxy_30d,
    realized_pnl_proxy_usd,
    volume_usd,
    tx_count,
    tokens_touched,
    active_days,
    metrics,
    updated_at
FROM wallet_candidates
ORDER BY score DESC
LIMIT 30;

-- ============================================================================
-- 02. Auto discovery watchlist in PostgreSQL.
-- ============================================================================
SELECT
    wl.name AS watchlist_name,
    ww.wallet_address,
    ww.note,
    ww.added_at
FROM watchlists wl
JOIN watchlist_wallets ww USING (watchlist_id)
WHERE wl.name = 'Auto discovery'
ORDER BY ww.added_at DESC
LIMIT 30;

-- ============================================================================
-- 03. Ingest checkpoints for real API jobs.
-- ============================================================================
SELECT
    source_name,
    checkpoint_value,
    updated_at
FROM ingest_checkpoints
ORDER BY updated_at DESC
LIMIT 30;

-- ============================================================================
-- 04. OLTP dictionaries used by ClickHouse dictionaries.
-- ============================================================================
SELECT
    'token_dictionary' AS table_name,
    count(*) AS rows
FROM token_dictionary
UNION ALL
SELECT
    'wallet_labels',
    count(*)
FROM wallet_labels
UNION ALL
SELECT
    'watchlist_wallets',
    count(*)
FROM watchlist_wallets;
