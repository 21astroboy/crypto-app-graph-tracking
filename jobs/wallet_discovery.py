import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

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

DISCOVERY_LOOKBACK_DAYS = int(env("DISCOVERY_LOOKBACK_DAYS", "30"))
DISCOVERY_TOP_N = int(env("DISCOVERY_TOP_N", "50"))
DISCOVERY_MIN_TX = int(env("DISCOVERY_MIN_TX", "5"))
DISCOVERY_MIN_TOKENS = int(env("DISCOVERY_MIN_TOKENS", "2"))
DISCOVERY_SOURCE = env("DISCOVERY_SOURCE", "local_raw_score")
DISCOVERY_EVENT_SOURCE = env("DISCOVERY_EVENT_SOURCE", "all").lower()
DISCOVERY_WATCHLIST_NAME = env("DISCOVERY_WATCHLIST_NAME", "Auto discovery")


def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def pg_conn():
    conninfo = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(conninfo)


def decimal_or_zero(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def score_candidate(row: dict) -> tuple[Decimal, Decimal, Decimal]:
    buy_usd = decimal_or_zero(row["buy_usd"])
    sell_usd = decimal_or_zero(row["sell_usd"])
    volume_usd = decimal_or_zero(row["volume_usd"])
    tx_count = Decimal(int(row["tx_count"]))
    tokens_touched = Decimal(int(row["tokens_touched"]))
    active_days = Decimal(int(row["active_days"]))

    realized_pnl_proxy = sell_usd - buy_usd
    if buy_usd > 0:
        roi_proxy = realized_pnl_proxy / buy_usd
    elif sell_usd > 0:
        roi_proxy = Decimal("1")
    else:
        roi_proxy = Decimal("0")

    positive_roi = max(Decimal("0"), roi_proxy)
    score = (
        positive_roi * Decimal("100")
        + volume_usd.sqrt() * Decimal("0.35")
        + tx_count * Decimal("0.25")
        + tokens_touched * Decimal("2")
        + active_days * Decimal("1.5")
    )
    return score.quantize(Decimal("0.000001")), roi_proxy.quantize(Decimal("0.000001")), realized_pnl_proxy.quantize(Decimal("0.0001"))


def fetch_candidates(ch) -> list[dict]:
    source_filter = {
        "all": "1",
        "demo": "source = 'demo'",
        "real": "source != 'demo'",
    }.get(DISCOVERY_EVENT_SOURCE)
    if source_filter is None:
        raise SystemExit("DISCOVERY_EVENT_SOURCE must be one of: all, demo, real")

    query = f"""
        SELECT
            wallet_address,
            count() AS tx_count,
            uniqExact(token_address) AS tokens_touched,
            uniqExact(toDate(block_time)) AS active_days,
            sumIf(amount_usd, side = 'buy') AS buy_usd,
            sumIf(amount_usd, side = 'sell') AS sell_usd,
            sum(amount_usd) AS volume_usd,
            sumIf(amount_usd, side = 'buy') - sumIf(amount_usd, side = 'sell') AS net_inflow_usd,
            min(block_time) AS first_seen,
            max(block_time) AS last_seen,
            countIf(event_type IN ('uniswap_v2_swap', 'uniswap_v3_swap')) AS decoded_swaps,
            countIf(source != 'demo') AS real_events
        FROM raw.dex_transactions
        WHERE block_time >= now() - INTERVAL {{lookback_days:UInt32}} DAY
          AND wallet_address != ''
          AND wallet_address != '0x0000000000000000000000000000000000000000'
          AND {source_filter}
        GROUP BY wallet_address
        HAVING tx_count >= {{min_tx:UInt32}}
           AND tokens_touched >= {{min_tokens:UInt32}}
        ORDER BY tx_count DESC
        LIMIT {{candidate_limit:UInt32}}
    """
    result = ch.query(
        query,
        parameters={
            "lookback_days": DISCOVERY_LOOKBACK_DAYS,
            "min_tx": DISCOVERY_MIN_TX,
            "min_tokens": DISCOVERY_MIN_TOKENS,
            "candidate_limit": max(DISCOVERY_TOP_N * 10, 100),
        },
    )
    rows = []
    for raw in result.named_results():
        row = dict(raw)
        score, roi_proxy, realized_pnl_proxy = score_candidate(row)
        row["score"] = score
        row["roi_proxy_30d"] = roi_proxy
        row["realized_pnl_proxy_usd"] = realized_pnl_proxy
        rows.append(row)
    rows.sort(key=lambda item: (item["score"], item["last_seen"]), reverse=True)
    return rows[:DISCOVERY_TOP_N]


def ensure_watchlist(cur) -> int:
    cur.execute("SELECT user_id FROM users WHERE username = %s", ("student",))
    user_id = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO watchlists (user_id, name, description)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, name) DO UPDATE
        SET description = EXCLUDED.description
        RETURNING watchlist_id
        """,
        (
            user_id,
            DISCOVERY_WATCHLIST_NAME,
            f"Auto-ranked candidates from ClickHouse raw events, lookback {DISCOVERY_LOOKBACK_DAYS} days.",
        ),
    )
    return int(cur.fetchone()[0])


def upsert_postgres(candidates: list[dict]) -> None:
    if not candidates:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        watchlist_id = ensure_watchlist(cur)
        candidate_rows = []
        watchlist_rows = []
        for rank, row in enumerate(candidates, start=1):
            metrics = {
                "rank": rank,
                "buy_usd": str(row["buy_usd"]),
                "sell_usd": str(row["sell_usd"]),
                "net_inflow_usd": str(row["net_inflow_usd"]),
                "decoded_swaps": int(row["decoded_swaps"]),
                "real_events": int(row["real_events"]),
                "lookback_days": DISCOVERY_LOOKBACK_DAYS,
                "event_source": DISCOVERY_EVENT_SOURCE,
            }
            candidate_rows.append(
                (
                    row["wallet_address"],
                    DISCOVERY_SOURCE,
                    row["score"],
                    row["roi_proxy_30d"],
                    row["realized_pnl_proxy_usd"],
                    row["volume_usd"],
                    int(row["tx_count"]),
                    int(row["tokens_touched"]),
                    int(row["active_days"]),
                    row["first_seen"],
                    row["last_seen"],
                    json.dumps(metrics),
                )
            )
            watchlist_rows.append((watchlist_id, row["wallet_address"], f"rank={rank}; score={row['score']}"))

        cur.executemany(
            """
            INSERT INTO wallet_candidates
                (wallet_address, source, score, roi_proxy_30d, realized_pnl_proxy_usd,
                 volume_usd, tx_count, tokens_touched, active_days, first_seen, last_seen, metrics)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (wallet_address) DO UPDATE
            SET source = EXCLUDED.source,
                score = EXCLUDED.score,
                roi_proxy_30d = EXCLUDED.roi_proxy_30d,
                realized_pnl_proxy_usd = EXCLUDED.realized_pnl_proxy_usd,
                volume_usd = EXCLUDED.volume_usd,
                tx_count = EXCLUDED.tx_count,
                tokens_touched = EXCLUDED.tokens_touched,
                active_days = EXCLUDED.active_days,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen,
                metrics = EXCLUDED.metrics,
                updated_at = now()
            """,
            candidate_rows,
        )
        cur.executemany(
            """
            INSERT INTO watchlist_wallets (watchlist_id, wallet_address, note)
            VALUES (%s, %s, %s)
            ON CONFLICT (watchlist_id, wallet_address) DO UPDATE
            SET note = EXCLUDED.note,
                added_at = now()
            """,
            watchlist_rows,
        )
        conn.commit()


def upsert_clickhouse_watchlist(ch, candidates: list[dict]) -> None:
    if not candidates:
        return
    version = int(time.time())
    rows = [
        (
            row["wallet_address"],
            float(row["roi_proxy_30d"]),
            row["realized_pnl_proxy_usd"],
            rank,
            DISCOVERY_SOURCE,
            version,
            0,
            datetime.now(timezone.utc),
        )
        for rank, row in enumerate(candidates, start=1)
    ]
    ch.insert(
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
    ch.command("SYSTEM RELOAD DICTIONARY mart.smart_wallets_dict")


def main() -> None:
    ch = ch_client()
    candidates = fetch_candidates(ch)
    upsert_postgres(candidates)
    upsert_clickhouse_watchlist(ch, candidates)
    print(
        f"Discovered {len(candidates)} wallet candidates "
        f"from last {DISCOVERY_LOOKBACK_DAYS} days "
        f"(event_source={DISCOVERY_EVENT_SOURCE}), published top {DISCOVERY_TOP_N}."
    )
    if candidates:
        best = candidates[0]
        print(
            "Top candidate: "
            f"{best['wallet_address']} score={best['score']} "
            f"roi_proxy={best['roi_proxy_30d']} tx={best['tx_count']} tokens={best['tokens_touched']}"
        )


if __name__ == "__main__":
    main()
