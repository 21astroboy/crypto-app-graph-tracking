CREATE TABLE IF NOT EXISTS mart.wallet_token_balances
(
    wallet_address String,
    token_address String,
    balance_token Decimal(38, 18),
    net_flow_usd Decimal(18, 4),
    buy_usd Decimal(18, 4),
    sell_usd Decimal(18, 4),
    tx_count UInt64
)
ENGINE = SummingMergeTree
ORDER BY (wallet_address, token_address);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_wallet_token_balances
TO mart.wallet_token_balances
AS
SELECT
    wallet_address,
    token_address,
    sum(
        multiIf(
            side = 'buy', amount_token,
            side = 'sell', -amount_token,
            event_type = 'add_liquidity', amount_token,
            event_type = 'remove_liquidity', -amount_token,
            toDecimal128(0, 18)
        )
    ) AS balance_token,
    sum(if(side = 'buy', amount_usd, -amount_usd)) AS net_flow_usd,
    sumIf(amount_usd, side = 'buy') AS buy_usd,
    sumIf(amount_usd, side = 'sell') AS sell_usd,
    count() AS tx_count
FROM raw.dex_transactions
GROUP BY wallet_address, token_address;

CREATE TABLE IF NOT EXISTS mart.wallet_daily_activity
(
    wallet_address String,
    activity_date Date,
    tx_count AggregateFunction(count),
    tokens_traded AggregateFunction(uniq, String),
    pools_touched AggregateFunction(uniq, String),
    volume_usd AggregateFunction(sum, Decimal(18, 4)),
    buy_count AggregateFunction(countIf, UInt8),
    sell_count AggregateFunction(countIf, UInt8),
    first_tx_time AggregateFunction(min, DateTime),
    last_tx_time AggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (wallet_address, activity_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_wallet_daily_activity
TO mart.wallet_daily_activity
AS
SELECT
    wallet_address,
    toDate(block_time) AS activity_date,
    countState() AS tx_count,
    uniqState(token_address) AS tokens_traded,
    uniqState(pool_address) AS pools_touched,
    sumState(amount_usd) AS volume_usd,
    countIfState(side = 'buy') AS buy_count,
    countIfState(side = 'sell') AS sell_count,
    minState(block_time) AS first_tx_time,
    maxState(block_time) AS last_tx_time
FROM raw.dex_transactions
GROUP BY wallet_address, activity_date;

CREATE TABLE IF NOT EXISTS mart.token_smart_money_flow_5m
(
    token_address String,
    bucket_time DateTime,
    smart_buy_usd Decimal(18, 4),
    smart_sell_usd Decimal(18, 4),
    net_flow_usd Decimal(18, 4),
    tx_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket_time)
ORDER BY (token_address, bucket_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_token_smart_money_flow_5m
TO mart.token_smart_money_flow_5m
AS
SELECT
    token_address,
    toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS bucket_time,
    sumIf(amount_usd, side = 'buy') AS smart_buy_usd,
    sumIf(amount_usd, side = 'sell') AS smart_sell_usd,
    sum(if(side = 'buy', amount_usd, -amount_usd)) AS net_flow_usd,
    count() AS tx_count
FROM raw.dex_transactions
GROUP BY token_address, bucket_time;

CREATE TABLE IF NOT EXISTS mart.first_wallet_buys
(
    wallet_address String,
    token_address String,
    first_buy_time AggregateFunction(min, DateTime),
    first_buy_amount_usd AggregateFunction(argMin, Decimal(18, 4), DateTime),
    first_buy_amount_token AggregateFunction(argMin, Decimal(38, 18), DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY (token_address, wallet_address);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_first_wallet_buys
TO mart.first_wallet_buys
AS
SELECT
    wallet_address,
    token_address,
    minState(block_time) AS first_buy_time,
    argMinState(amount_usd, block_time) AS first_buy_amount_usd,
    argMinState(amount_token, block_time) AS first_buy_amount_token
FROM raw.dex_transactions
WHERE side = 'buy'
GROUP BY wallet_address, token_address;

CREATE TABLE IF NOT EXISTS mart.wallet_ratings_latest
(
    wallet_address String,
    roi_7d Float64,
    roi_30d Float64,
    winrate Float64,
    avg_hold_time_hours Float64,
    realized_pnl_usd Decimal(18, 4),
    smart_score Float64,
    graph_score Float64,
    version UInt64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY wallet_address;
