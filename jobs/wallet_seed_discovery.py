import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


POSTGRES_HOST = env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(env("POSTGRES_PORT", "5432"))
POSTGRES_DB = env("POSTGRES_DB", "wallet_meta")
POSTGRES_USER = env("POSTGRES_USER", "student")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "student")

ETHERSCAN_API_KEY = env("ETHERSCAN_API_KEY", "")
ETHERSCAN_BASE_URL = env("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")
ETHERSCAN_CHAIN_ID = env("ETHERSCAN_CHAIN_ID", "1")

SEED_WALLETS = [
    w.strip().lower()
    for w in env("SEED_WALLETS", env("REAL_WALLETS", "")).split(",")
    if w.strip()
]
SEED_TARGET_WALLETS = int(env("SEED_TARGET_WALLETS", "200"))
SEED_MAX_TRANSFERS_PER_SEED = int(env("SEED_MAX_TRANSFERS_PER_SEED", "1000"))
SEED_SLEEP_SECONDS = float(env("SEED_SLEEP_SECONDS", "1.0"))
SEED_WATCHLIST_NAME = env("SEED_WATCHLIST_NAME", "Etherscan counterparty seeds")


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def pg_conn():
    conninfo = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(conninfo)


def ensure_api_key() -> None:
    if not ETHERSCAN_API_KEY:
        raise SystemExit("ETHERSCAN_API_KEY is required for wallet seed discovery.")


def is_wallet(value: str) -> bool:
    value = value.lower()
    return value.startswith("0x") and len(value) == 42 and value != ZERO_ADDRESS


def request_json(params: dict[str, Any]) -> Any:
    response = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_seed_transfers(wallet: str) -> list[dict[str, Any]]:
    payload = request_json(
        {
            "chainid": ETHERSCAN_CHAIN_ID,
            "module": "account",
            "action": "tokentx",
            "address": wallet,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": SEED_MAX_TRANSFERS_PER_SEED,
            "sort": "desc",
            "apikey": ETHERSCAN_API_KEY,
        }
    )
    result = payload.get("result", [])
    if str(payload.get("status", "")) == "0" and isinstance(result, str):
        if "No transactions found" in result:
            return []
        raise RuntimeError(f"Etherscan seed error for {wallet}: {payload.get('message')} / {result}")
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected Etherscan seed response for {wallet}: {payload}")
    return result


def parse_time(tx: dict[str, Any]) -> datetime:
    return datetime.fromtimestamp(int(tx.get("timeStamp") or 0), tz=timezone.utc)


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
            SEED_WATCHLIST_NAME,
            "Real wallet seeds discovered from Etherscan ERC-20 transfer counterparties.",
        ),
    )
    return int(cur.fetchone()[0])


def upsert_watchlist(candidates: list[dict[str, Any]]) -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        watchlist_id = ensure_watchlist(cur)
        cur.execute("DELETE FROM watchlist_wallets WHERE watchlist_id = %s", (watchlist_id,))
        cur.executemany(
            """
            INSERT INTO watchlist_wallets (watchlist_id, wallet_address, note, added_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (watchlist_id, wallet_address) DO UPDATE
            SET note = EXCLUDED.note,
                added_at = now()
            """,
            [
                (
                    watchlist_id,
                    row["wallet"],
                    (
                        f"seed-rank={rank}; transfers={row['tx_count']}; "
                        f"tokens={len(row['tokens'])}; last_seen={row['last_seen'].isoformat()}"
                    ),
                )
                for rank, row in enumerate(candidates, start=1)
            ],
        )
        conn.commit()


def main() -> None:
    ensure_api_key()
    if not SEED_WALLETS:
        raise SystemExit("Set SEED_WALLETS or REAL_WALLETS to discover real wallet seeds.")

    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "wallet": "",
            "tx_count": 0,
            "tokens": set(),
            "first_seen": None,
            "last_seen": None,
            "seed_hits": 0,
        }
    )
    seed_set = set(SEED_WALLETS)

    for seed in SEED_WALLETS:
        transfers = fetch_seed_transfers(seed)
        print(f"{seed}: fetched {len(transfers)} seed transfers")
        for tx in transfers:
            token = str(tx.get("contractAddress", "")).lower()
            seen_at = parse_time(tx)
            for address in (str(tx.get("from", "")).lower(), str(tx.get("to", "")).lower()):
                if not is_wallet(address) or address in seed_set:
                    continue
                row = stats[address]
                row["wallet"] = address
                row["tx_count"] += 1
                row["tokens"].add(token)
                row["seed_hits"] += 1
                row["first_seen"] = seen_at if row["first_seen"] is None else min(row["first_seen"], seen_at)
                row["last_seen"] = seen_at if row["last_seen"] is None else max(row["last_seen"], seen_at)
        time.sleep(SEED_SLEEP_SECONDS)

    candidates = sorted(
        stats.values(),
        key=lambda row: (row["tx_count"], len(row["tokens"]), row["last_seen"]),
        reverse=True,
    )[:SEED_TARGET_WALLETS]
    upsert_watchlist(candidates)
    print(
        f"Published {len(candidates)} real wallet seeds "
        f"from {len(SEED_WALLETS)} seed wallets into watchlist '{SEED_WATCHLIST_NAME}'."
    )
    if candidates:
        top = candidates[0]
        print(
            f"Top seed candidate: {top['wallet']} transfers={top['tx_count']} "
            f"tokens={len(top['tokens'])} last_seen={top['last_seen'].isoformat()}"
        )


if __name__ == "__main__":
    main()
