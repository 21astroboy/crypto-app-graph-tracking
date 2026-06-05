import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
import requests


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


CLICKHOUSE_HOST = env("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(env("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = env("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_USER = env("CLICKHOUSE_USER", "student")
CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "student")

COINGECKO_BASE_URL = env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
PRICE_LOOKBACK_DAYS = int(env("PRICE_LOOKBACK_DAYS", "30"))
PRICE_MAX_TOKENS = int(env("PRICE_MAX_TOKENS", "20"))
PRICE_BATCH_SIZE = int(env("PRICE_BATCH_SIZE", "5"))
PRICE_SLEEP_SECONDS = float(env("PRICE_SLEEP_SECONDS", "2.0"))
PRICE_REFRESH_HOURS = int(env("PRICE_REFRESH_HOURS", "12"))
PRICE_SOURCE = env("PRICE_SOURCE", "coingecko-price-job")
PRICE_EVENT_SOURCE = env("PRICE_EVENT_SOURCE", "all").lower()
RUN_ID = env("RUN_ID", f"price-{uuid.uuid4().hex[:12]}")

ZERO_POOL = "0x0000000000000000000000000000000000000000"


def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def request_json(url: str, params: dict[str, Any], timeout: int = 30) -> Any:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_candidate_tokens(ch) -> list[str]:
    source_filter = {
        "all": "1",
        "demo": "tx.source = 'demo'",
        "real": "tx.source != 'demo'",
    }.get(PRICE_EVENT_SOURCE)
    if source_filter is None:
        raise SystemExit("PRICE_EVENT_SOURCE must be one of: all, demo, real")

    query = f"""
        WITH latest_prices AS
        (
            SELECT
                token_address AS priced_token_address,
                max(loaded_at) AS last_price_loaded_at
            FROM raw.token_prices_hourly
            GROUP BY token_address
        )
        SELECT
            tx.token_address
        FROM raw.dex_transactions AS tx
        LEFT JOIN latest_prices AS lp
            ON tx.token_address = lp.priced_token_address
        WHERE tx.block_time >= now() - INTERVAL {PRICE_LOOKBACK_DAYS} DAY
          AND tx.token_address != ''
          AND tx.token_address != '{ZERO_POOL}'
          AND match(tx.token_address, '^0x[0-9a-fA-F]{{40}}$')
          AND {source_filter}
          AND (
              lp.priced_token_address = ''
              OR lp.last_price_loaded_at < now() - INTERVAL {PRICE_REFRESH_HOURS} HOUR
          )
        GROUP BY tx.token_address
        ORDER BY count() DESC, max(tx.block_time) DESC
        LIMIT {PRICE_MAX_TOKENS}
    """
    result = ch.query(query)
    return [str(row[0]).lower() for row in result.result_rows]


def fetch_prices(tokens: list[str]) -> dict[str, float]:
    prices: dict[str, float] = {}
    batch_size = max(1, PRICE_BATCH_SIZE)
    for i in range(0, len(tokens), batch_size):
        chunk = tokens[i : i + batch_size]
        chunk_prices, should_stop = fetch_price_chunk(chunk)
        prices.update(chunk_prices)
        print(f"priced chunk {i // batch_size + 1}: requested={len(chunk)} priced={len(chunk_prices)}")
        if should_stop:
            break
        time.sleep(PRICE_SLEEP_SECONDS)
    return prices


def fetch_price_chunk(tokens: list[str]) -> tuple[dict[str, float], bool]:
    try:
        payload = request_json(
            f"{COINGECKO_BASE_URL}/simple/token_price/ethereum",
            {"contract_addresses": ",".join(tokens), "vs_currencies": "usd"},
            timeout=20,
        )
        prices = {}
        for token, data in payload.items():
            usd = data.get("usd") if isinstance(data, dict) else None
            if usd is not None:
                prices[token.lower()] = float(usd)
        return prices, False
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        if status_code == 429:
            print(f"price API rate-limited: requested={len(tokens)}")
            return {}, True
        if status_code == 400 and len(tokens) > 1:
            prices: dict[str, float] = {}
            for token in tokens:
                token_prices, should_stop = fetch_price_chunk([token])
                prices.update(token_prices)
                if should_stop:
                    return prices, True
                time.sleep(PRICE_SLEEP_SECONDS)
            return prices, False
        print(f"price token skipped: requested={len(tokens)} http_status={status_code}")
        return {}, False
    except Exception as exc:
        print(f"price token skipped: requested={len(tokens)} error={exc}")
        return {}, False


def insert_prices(ch, prices: dict[str, float]) -> None:
    if not prices:
        return
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    version = int(datetime.now(timezone.utc).timestamp())
    rows = [
        (token, now, float(price), PRICE_SOURCE, version, datetime.now(timezone.utc))
        for token, price in prices.items()
    ]
    ch.insert(
        "raw.token_prices_hourly",
        rows,
        column_names=["token_address", "price_hour", "price_usd", "source", "version", "loaded_at"],
    )


def insert_ingest_run(ch, started_at: datetime, status: str, tokens_requested: int, tokens_priced: int, error: str = "") -> None:
    finished_at = datetime.now(timezone.utc)
    ch.insert(
        "raw.ingest_runs",
        [(
            RUN_ID,
            "price_job",
            started_at,
            finished_at,
            status,
            0,
            tokens_requested,
            tokens_priced,
            error[:1000],
            int(finished_at.timestamp()),
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


def main() -> None:
    started_at = datetime.now(timezone.utc)
    ch = ch_client()
    tokens_requested = 0
    tokens_priced = 0
    try:
        tokens = fetch_candidate_tokens(ch)
        tokens_requested = len(tokens)
        if not tokens:
            insert_ingest_run(ch, started_at, "success", 0, 0)
            print("No tokens need price refresh.")
            return

        prices = fetch_prices(tokens)
        tokens_priced = len(prices)
        insert_prices(ch, prices)
        ch.command("SYSTEM RELOAD DICTIONARY mart.prices_dict")
        insert_ingest_run(ch, started_at, "success", tokens_requested, tokens_priced)
        print(f"Price ingest finished: requested={tokens_requested}, priced={tokens_priced}.")
    except Exception as exc:
        insert_ingest_run(ch, started_at, "failed", tokens_requested, tokens_priced, str(exc))
        raise


if __name__ == "__main__":
    main()
