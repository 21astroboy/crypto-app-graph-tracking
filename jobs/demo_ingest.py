import hashlib
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext

import clickhouse_connect
import psycopg


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


CLICKHOUSE_HOST = env("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(env("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = env("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_USER = env("CLICKHOUSE_USER", "student")
CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "student")

POSTGRES_HOST = env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(env("POSTGRES_PORT", "5432"))
POSTGRES_DB = env("POSTGRES_DB", "wallet_meta")
POSTGRES_USER = env("POSTGRES_USER", "student")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "student")

WALLET_COUNT = int(env("DEMO_WALLETS", "250"))
DAYS = int(env("DEMO_DAYS", "21"))
EVENTS_PER_DAY = int(env("DEMO_EVENTS_PER_DAY", "900"))

random.seed(42)
getcontext().prec = 60
RUN_ID = env("RUN_ID", f"demo-{uuid.uuid4().hex[:12]}")
INGEST_SOURCE = env("INGEST_SOURCE", "demo")


TOKENS = [
    ("0x" + hashlib.sha1(symbol.encode()).hexdigest()[:40], symbol, name, decimals)
    for symbol, name, decimals in [
        ("ETH", "Ether", 18),
        ("USDC", "USD Coin", 6),
        ("WBTC", "Wrapped Bitcoin", 8),
        ("ARB", "Arbitrum", 18),
        ("OP", "Optimism", 18),
        ("LINK", "Chainlink", 18),
        ("UNI", "Uniswap", 18),
        ("AAVE", "Aave", 18),
        ("PEPE", "Pepe", 18),
        ("WIF", "Dogwifhat", 18),
        ("BONK", "Bonk", 18),
        ("ENA", "Ethena", 18),
        ("PENDLE", "Pendle", 18),
        ("LDO", "Lido DAO", 18),
        ("MKR", "Maker", 18),
    ]
]

TOKEN_PRICES = {
    "ETH": 3500,
    "USDC": 1,
    "WBTC": 68000,
    "ARB": 1.15,
    "OP": 2.25,
    "LINK": 17,
    "UNI": 10,
    "AAVE": 105,
    "PEPE": 0.000012,
    "WIF": 2.8,
    "BONK": 0.000025,
    "ENA": 0.82,
    "PENDLE": 5.5,
    "LDO": 2.1,
    "MKR": 2800,
}


def wallet_address(i: int) -> str:
    return "0x" + hashlib.sha1(f"wallet-{i}".encode()).hexdigest()[:40]


def pool_address(token_a: str, token_b: str) -> str:
    key = "-".join(sorted([token_a, token_b]))
    return "0x" + hashlib.sha1(f"pool-{key}".encode()).hexdigest()[:40]


def wait_for_clickhouse(client) -> None:
    for _ in range(30):
        try:
            client.command("SELECT 1")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("ClickHouse is not ready")


def wait_for_postgres() -> psycopg.Connection:
    conninfo = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
    for _ in range(30):
        try:
            return psycopg.connect(conninfo)
        except Exception:
            time.sleep(2)
    raise RuntimeError("Postgres is not ready")


def seed_postgres() -> None:
    with wait_for_postgres() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO token_dictionary
                    (token_address, symbol, name, chain, decimals, risk_level)
                VALUES (%s, %s, %s, 'ethereum', %s, %s)
                ON CONFLICT (token_address) DO UPDATE
                SET symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    decimals = EXCLUDED.decimals,
                    risk_level = EXCLUDED.risk_level,
                    updated_at = now()
                """,
                [
                    (
                        token,
                        symbol,
                        name,
                        decimals,
                        "high" if symbol in {"PEPE", "WIF", "BONK"} else "normal",
                    )
                    for token, symbol, name, decimals in TOKENS
                ],
            )
            labels = [
                (wallet_address(i), f"smart-wallet-{i:03d}", "demo-roi-top")
                for i in range(min(WALLET_COUNT, 40))
            ]
            cur.executemany(
                """
                INSERT INTO wallet_labels (wallet_address, label, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (wallet_address) DO UPDATE
                SET label = EXCLUDED.label,
                    source = EXCLUDED.source,
                    updated_at = now()
                """,
                labels,
            )
            stable = next(token for token, symbol, _name, _decimals in TOKENS if symbol == "USDC")
            pools = [
                (
                    pool_address(token, stable),
                    "ethereum",
                    "uniswap_v3" if i % 2 == 0 else "uniswap_v2",
                    token,
                    stable,
                    3000 if i % 2 == 0 else None,
                )
                for i, (token, symbol, _name, _decimals) in enumerate(TOKENS)
                if symbol != "USDC"
            ]
            cur.executemany(
                """
                INSERT INTO dex_pools
                    (pool_address, chain, dex_name, token0_address, token1_address, fee_tier)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (pool_address) DO UPDATE
                SET chain = EXCLUDED.chain,
                    dex_name = EXCLUDED.dex_name,
                    token0_address = EXCLUDED.token0_address,
                    token1_address = EXCLUDED.token1_address,
                    fee_tier = EXCLUDED.fee_tier,
                    updated_at = now()
                """,
                pools,
            )
            conn.commit()


def insert_watchlist(client) -> list[str]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    wallets = [wallet_address(i) for i in range(WALLET_COUNT)]
    rows = []
    for rank, wallet in enumerate(wallets, start=1):
        roi = max(-0.6, random.gauss(1.4, 0.75) - rank / WALLET_COUNT * 0.5)
        pnl = Decimal(str(round(random.uniform(2_000, 250_000) * roi, 4)))
        rows.append((wallet, roi, pnl, rank, "demo-roi-top", int(now.timestamp()), 0, now))
    client.insert(
        "raw.wallet_watchlist",
        rows,
        column_names=[
            "wallet_address",
            "roi_30d",
            "realized_pnl_usd",
            "rank",
            "source",
            "version",
            "is_deleted",
            "updated_at",
        ],
    )
    return wallets


def insert_prices(client) -> None:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=DAYS)
    rows = []
    version = int(now.timestamp())
    current = start
    while current <= now:
        hour_index = int((current - start).total_seconds() // 3600)
        for token, symbol, _name, _decimals in TOKENS:
            base = TOKEN_PRICES[symbol]
            drift = 1 + 0.002 * hour_index / 24
            wave = 1 + random.uniform(-0.035, 0.035)
            rows.append((token, current, float(base * drift * wave), "demo-price", version, now))
        current += timedelta(hours=1)
    client.insert(
        "raw.token_prices_hourly",
        rows,
        column_names=["token_address", "price_hour", "price_usd", "source", "version", "loaded_at"],
    )


def insert_transactions(client, wallets: list[str]) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(days=DAYS)
    rows = []
    block_number = 19_000_000
    stable = next(token for token in TOKENS if token[1] == "USDC")[0]
    event_types = ["swap", "swap", "swap", "add_liquidity", "remove_liquidity"]

    for day in range(DAYS):
        day_start = start + timedelta(days=day)
        for event_num in range(EVENTS_PER_DAY):
            wallet = random.choice(wallets)
            token, symbol, _name, _decimals = random.choices(
                TOKENS,
                weights=[9, 15, 4, 7, 7, 6, 6, 4, 9, 7, 7, 4, 4, 3, 2],
                k=1,
            )[0]
            event_time = day_start + timedelta(
                seconds=random.randint(0, 86_399),
                microseconds=0,
            )
            event_type = random.choice(event_types)
            side = random.choices(["buy", "sell"], weights=[0.57, 0.43], k=1)[0]
            amount_usd_float = random.lognormvariate(7.7, 1.0)
            if random.random() < 0.025:
                amount_usd_float *= random.uniform(8, 30)
            amount_usd = Decimal(str(round(amount_usd_float, 4)))
            token_price = Decimal(str(TOKEN_PRICES[symbol]))
            amount_token = (amount_usd / token_price).quantize(Decimal("0.000000000000000001"))
            fee_usd = Decimal(str(round(float(amount_usd) * random.uniform(0.0005, 0.004), 4)))
            tx_hash = "0x" + hashlib.sha1(
                f"{wallet}-{token}-{event_time.isoformat()}-{event_num}".encode()
            ).hexdigest()[:40]
            rows.append(
                (
                    "ethereum",
                    event_time,
                    block_number,
                    tx_hash,
                    event_num,
                    wallet,
                    token,
                    pool_address(token, stable),
                    event_type,
                    side,
                    amount_token,
                    amount_usd,
                    fee_usd,
                    INGEST_SOURCE,
                    RUN_ID,
                    now,
                )
            )
            block_number += random.randint(1, 3)

    client.insert(
        "raw.dex_transactions",
        rows,
        column_names=[
            "chain",
            "block_time",
            "block_number",
            "tx_hash",
            "log_index",
            "wallet_address",
            "token_address",
            "pool_address",
            "event_type",
            "side",
            "amount_token",
            "amount_usd",
            "fee_usd",
            "source",
            "run_id",
            "loaded_at",
        ],
    )


def insert_ingest_run(client, rows_inserted: int) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    client.insert(
        "raw.ingest_runs",
        [(
            RUN_ID,
            INGEST_SOURCE,
            now,
            now,
            "success",
            WALLET_COUNT,
            rows_inserted,
            len(TOKENS),
            "",
            int(now.timestamp()),
        )],
        column_names=[
            "run_id",
            "source",
            "started_at",
            "finished_at",
            "status",
            "wallets_count",
            "rows_inserted",
            "tokens_priced",
            "error",
            "version",
        ],
    )


def insert_wallet_ratings(client, wallets: list[str]) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for wallet in wallets:
        roi_7d = random.gauss(0.35, 0.45)
        roi_30d = random.gauss(1.0, 0.8)
        winrate = min(0.95, max(0.05, random.gauss(0.58, 0.14)))
        graph_score = min(100, max(0, random.gauss(62, 18)))
        smart_score = min(100, max(0, roi_30d * 22 + winrate * 35 + graph_score * 0.35))
        pnl = Decimal(str(round(random.uniform(1_000, 180_000) * max(roi_30d, -0.4), 4)))
        rows.append(
            (
                wallet,
                roi_7d,
                roi_30d,
                winrate,
                random.uniform(4, 180),
                pnl,
                smart_score,
                graph_score,
                int(now.timestamp()),
                now,
            )
        )
    client.insert(
        "mart.wallet_ratings_latest",
        rows,
        column_names=[
            "wallet_address",
            "roi_7d",
            "roi_30d",
            "winrate",
            "avg_hold_time_hours",
            "realized_pnl_usd",
            "smart_score",
            "graph_score",
            "version",
            "updated_at",
        ],
    )


def insert_similarity_edges(client, wallets: list[str]) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(min(120, len(wallets) - 1)):
        for _ in range(3):
            j = random.randint(i + 1, len(wallets) - 1)
            common = random.randint(2, 12)
            jaccard = min(1, common / random.randint(common + 2, common + 20))
            corr = random.uniform(0.1, 0.95)
            score = 100 * (0.65 * jaccard + 0.35 * corr)
            rows.append((wallets[i], wallets[j], common, jaccard, corr, score, now, int(now.timestamp())))
    client.insert(
        "graph.wallet_similarity_edges",
        rows,
        column_names=[
            "wallet_a",
            "wallet_b",
            "common_tokens",
            "jaccard_tokens",
            "time_correlation",
            "similarity_score",
            "calculated_at",
            "version",
        ],
    )


def main() -> None:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    wait_for_clickhouse(client)
    seed_postgres()
    wallets = insert_watchlist(client)
    insert_prices(client)
    insert_transactions(client, wallets)
    insert_wallet_ratings(client, wallets)
    insert_similarity_edges(client, wallets)
    insert_ingest_run(client, DAYS * EVENTS_PER_DAY)
    print(
        f"Inserted demo data: {len(wallets)} wallets, "
        f"{DAYS * EVENTS_PER_DAY} transactions, {len(TOKENS)} tokens."
    )


if __name__ == "__main__":
    main()
