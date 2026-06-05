CREATE TABLE IF NOT EXISTS token_dictionary
(
    token_address text PRIMARY KEY,
    symbol text NOT NULL,
    name text NOT NULL,
    chain text NOT NULL,
    decimals integer NOT NULL,
    risk_level text NOT NULL DEFAULT 'normal',
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wallet_labels
(
    wallet_address text PRIMARY KEY,
    label text NOT NULL,
    source text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dex_pools
(
    pool_address text PRIMARY KEY,
    chain text NOT NULL,
    dex_name text NOT NULL,
    token0_address text NOT NULL,
    token1_address text NOT NULL,
    fee_tier integer,
    created_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dex_pools_token0 ON dex_pools (token0_address);
CREATE INDEX IF NOT EXISTS idx_dex_pools_token1 ON dex_pools (token1_address);

CREATE TABLE IF NOT EXISTS users
(
    user_id bigserial PRIMARY KEY,
    username text UNIQUE NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO users (username) VALUES ('student') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS watchlists
(
    watchlist_id bigserial PRIMARY KEY,
    user_id bigint NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name text NOT NULL,
    description text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS watchlist_wallets
(
    watchlist_id bigint NOT NULL REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
    wallet_address text NOT NULL,
    note text,
    added_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (watchlist_id, wallet_address)
);

CREATE TABLE IF NOT EXISTS wallet_candidates
(
    wallet_address text PRIMARY KEY,
    source text NOT NULL,
    score numeric(18, 6) NOT NULL,
    roi_proxy_30d numeric(18, 6) NOT NULL,
    realized_pnl_proxy_usd numeric(18, 4) NOT NULL,
    volume_usd numeric(18, 4) NOT NULL,
    tx_count integer NOT NULL,
    tokens_touched integer NOT NULL,
    active_days integer NOT NULL,
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wallet_candidates_score ON wallet_candidates (score DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_candidates_last_seen ON wallet_candidates (last_seen DESC);

CREATE TABLE IF NOT EXISTS alert_rules
(
    rule_id bigserial PRIMARY KEY,
    rule_name text NOT NULL,
    token_address text,
    wallet_address text,
    metric text NOT NULL,
    threshold numeric(18, 4) NOT NULL,
    is_enabled boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_history
(
    alert_id bigserial PRIMARY KEY,
    rule_id bigint NOT NULL REFERENCES alert_rules(rule_id) ON DELETE CASCADE,
    triggered_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_history_time ON alert_history (triggered_at DESC);

CREATE TABLE IF NOT EXISTS ingest_checkpoints
(
    source_name text PRIMARY KEY,
    checkpoint_value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
