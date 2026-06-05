CREATE DICTIONARY IF NOT EXISTS mart.tokens_dict
(
    token_address String,
    symbol String DEFAULT '',
    name String DEFAULT '',
    chain String DEFAULT '',
    decimals UInt8 DEFAULT 18,
    risk_level String DEFAULT 'normal'
)
PRIMARY KEY token_address
SOURCE(POSTGRESQL(
    host 'postgres'
    port 5432
    user 'student'
    password 'student'
    db 'wallet_meta'
    table 'token_dictionary'
    invalidate_query 'SELECT max(updated_at) FROM token_dictionary'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 900);

CREATE DICTIONARY IF NOT EXISTS mart.wallet_labels_dict
(
    wallet_address String,
    label String DEFAULT '',
    source String DEFAULT ''
)
PRIMARY KEY wallet_address
SOURCE(POSTGRESQL(
    host 'postgres'
    port 5432
    user 'student'
    password 'student'
    db 'wallet_meta'
    table 'wallet_labels'
    invalidate_query 'SELECT max(updated_at) FROM wallet_labels'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 900);

CREATE DICTIONARY IF NOT EXISTS mart.prices_dict
(
    token_address String,
    price_hour DateTime,
    price_usd Float64 DEFAULT 0
)
PRIMARY KEY token_address, price_hour
SOURCE(CLICKHOUSE(
    host 'localhost'
    port 9000
    user 'student'
    password 'student'
    db 'raw'
    query 'SELECT token_address, price_hour, price_usd FROM raw.token_prices_hourly FINAL'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 300 MAX 900);

CREATE DICTIONARY IF NOT EXISTS mart.smart_wallets_dict
(
    wallet_address String,
    smart_score Float64 DEFAULT 0,
    roi_30d Float64 DEFAULT 0,
    graph_score Float64 DEFAULT 0
)
PRIMARY KEY wallet_address
SOURCE(CLICKHOUSE(
    host 'localhost'
    port 9000
    user 'student'
    password 'student'
    db 'mart'
    query '
        SELECT
            wallet_address,
            smart_score,
            roi_30d,
            graph_score
        FROM mart.wallet_ratings_latest FINAL
        WHERE smart_score >= 50
        ORDER BY smart_score DESC
        LIMIT 500
    '
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 900);
