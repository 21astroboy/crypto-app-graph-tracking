import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Any

import clickhouse_connect
import psycopg
import requests


getcontext().prec = 90


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

ETHERSCAN_API_KEY = env("ETHERSCAN_API_KEY", "")
ETHERSCAN_BASE_URL = env("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")
ETHERSCAN_CHAIN_ID = env("ETHERSCAN_CHAIN_ID", "1")

COINGECKO_BASE_URL = env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

REAL_WALLETS = [w.strip().lower() for w in env("REAL_WALLETS", "").split(",") if w.strip()]
REAL_MAX_WALLETS = int(env("REAL_MAX_WALLETS", "20"))
REAL_MAX_TX_PER_WALLET = int(env("REAL_MAX_TX_PER_WALLET", "100"))
REAL_START_BLOCK = int(env("REAL_START_BLOCK", "0"))
REAL_END_BLOCK = int(env("REAL_END_BLOCK", "99999999"))
REAL_SLEEP_SECONDS = float(env("REAL_SLEEP_SECONDS", "0.25"))
REAL_BATCH_SIZE = int(env("REAL_BATCH_SIZE", "1000"))
REAL_WORKERS = int(env("REAL_WORKERS", "1"))
REAL_HTTP_RETRIES = int(env("REAL_HTTP_RETRIES", "3"))
REAL_PRICE_MAX_TOKENS = int(env("REAL_PRICE_MAX_TOKENS", "50"))
REAL_PRICE_BATCH_SIZE = int(env("REAL_PRICE_BATCH_SIZE", "10"))
REAL_ENABLE_PRICE_LOOKUP = env("REAL_ENABLE_PRICE_LOOKUP", "true").lower() in {"1", "true", "yes", "y"}
REAL_ENABLE_SWAPS = env("REAL_ENABLE_SWAPS", "true").lower() in {"1", "true", "yes", "y"}
REAL_MAX_RECEIPTS_PER_WALLET = int(env("REAL_MAX_RECEIPTS_PER_WALLET", "20"))
RUN_ID = env("RUN_ID", f"real-{uuid.uuid4().hex[:12]}")

ZERO_POOL = "0x0000000000000000000000000000000000000000"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
UNISWAP_V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
UNISWAP_V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f291749249f928cc2ac818eb64e5e7d4e6a62ce9a"
SWAP_TOPICS = {UNISWAP_V2_SWAP_TOPIC, UNISWAP_V3_SWAP_TOPIC}


def pg_conn():
    conninfo = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(conninfo)


def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def request_json(url: str, params: dict[str, Any], timeout: int = 30) -> Any:
    last_error: Exception | None = None
    for attempt in range(max(1, REAL_HTTP_RETRIES)):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            message = str(payload.get("message", "")).lower() if isinstance(payload, dict) else ""
            result = str(payload.get("result", "")).lower() if isinstance(payload, dict) else ""
            if "rate limit" in message or "rate limit" in result:
                raise RuntimeError(f"rate limited: {payload.get('message')} / {payload.get('result')}")
            return payload
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= max(1, REAL_HTTP_RETRIES):
                break
            time.sleep(REAL_SLEEP_SECONDS * (attempt + 1))
    raise last_error or RuntimeError("request failed")


def ensure_api_key() -> None:
    if not ETHERSCAN_API_KEY:
        raise SystemExit(
            "ETHERSCAN_API_KEY is required for real ingest. "
            "Set it in your shell, then run: make real-ingest"
        )


def fetch_wallets(ch) -> list[str]:
    if REAL_WALLETS:
        return REAL_WALLETS[:REAL_MAX_WALLETS]

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT wallet_address
            FROM watchlist_wallets
            ORDER BY added_at DESC
            LIMIT %s
            """,
            (REAL_MAX_WALLETS,),
        )
        wallets = [r[0].lower() for r in cur.fetchall()]
    if wallets:
        return wallets

    rows = ch.query(
        """
        SELECT wallet_address
        FROM raw.wallet_watchlist FINAL
        WHERE is_deleted = 0
        ORDER BY rank
        LIMIT {limit:UInt32}
        """,
        parameters={"limit": REAL_MAX_WALLETS},
    ).result_rows
    return [r[0].lower() for r in rows]


def checkpoint_key(wallet: str) -> str:
    return f"etherscan_tokentx:{ETHERSCAN_CHAIN_ID}:{wallet}"


def txlist_checkpoint_key(wallet: str) -> str:
    return f"etherscan_txlist:{ETHERSCAN_CHAIN_ID}:{wallet}"


def get_checkpoint(wallet: str) -> int:
    return get_named_checkpoint(checkpoint_key(wallet))


def get_txlist_checkpoint(wallet: str) -> int:
    return get_named_checkpoint(txlist_checkpoint_key(wallet))


def get_named_checkpoint(key: str) -> int:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT checkpoint_value FROM ingest_checkpoints WHERE source_name = %s",
            (key,),
        )
        row = cur.fetchone()
        if row:
            return max(REAL_START_BLOCK, int(row[0]) + 1)
    return REAL_START_BLOCK


def set_checkpoint(wallet: str, block_number: int) -> None:
    set_named_checkpoint(checkpoint_key(wallet), block_number)


def set_txlist_checkpoint(wallet: str, block_number: int) -> None:
    set_named_checkpoint(txlist_checkpoint_key(wallet), block_number)


def set_named_checkpoint(key: str, block_number: int) -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_checkpoints (source_name, checkpoint_value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (source_name) DO UPDATE
            SET checkpoint_value = EXCLUDED.checkpoint_value,
                updated_at = now()
            """,
            (key, str(block_number)),
        )
        conn.commit()


def fetch_erc20_transfers(wallet: str, start_block: int) -> list[dict[str, Any]]:
    params = {
        "chainid": ETHERSCAN_CHAIN_ID,
        "module": "account",
        "action": "tokentx",
        "address": wallet,
        "startblock": start_block,
        "endblock": REAL_END_BLOCK,
        "page": 1,
        "offset": REAL_MAX_TX_PER_WALLET,
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    payload = request_json(ETHERSCAN_BASE_URL, params)
    status = str(payload.get("status", ""))
    message = str(payload.get("message", ""))
    result = payload.get("result", [])
    if status == "0" and isinstance(result, str):
        if "No transactions found" in result:
            return []
        raise RuntimeError(f"Etherscan error for {wallet}: {message} / {result}")
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected Etherscan response for {wallet}: {payload}")
    return result


def fetch_normal_transactions(wallet: str, start_block: int) -> list[dict[str, Any]]:
    params = {
        "chainid": ETHERSCAN_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": wallet,
        "startblock": start_block,
        "endblock": REAL_END_BLOCK,
        "page": 1,
        "offset": REAL_MAX_RECEIPTS_PER_WALLET,
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    payload = request_json(ETHERSCAN_BASE_URL, params)
    result = payload.get("result", [])
    if str(payload.get("status", "")) == "0" and isinstance(result, str):
        if "No transactions found" in result:
            return []
        raise RuntimeError(f"Etherscan txlist error for {wallet}: {payload.get('message')} / {result}")
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected Etherscan txlist response for {wallet}: {payload}")
    return result


def fetch_receipt(tx_hash: str) -> dict[str, Any] | None:
    payload = request_json(
        ETHERSCAN_BASE_URL,
        {
            "chainid": ETHERSCAN_CHAIN_ID,
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": tx_hash,
            "apikey": ETHERSCAN_API_KEY,
        },
    )
    result = payload.get("result")
    return result if isinstance(result, dict) else None


def decimal_amount(value: str, decimals: str) -> Decimal:
    scale = Decimal(10) ** int(decimals or "0")
    if scale == 0:
        return Decimal(0)
    return (Decimal(value or "0") / scale).quantize(Decimal("0.000000000000000001"))


def hex_to_int(value: str) -> int:
    if not value or value.lower() == "0x":
        return 0
    return int(value, 16)


def topic_to_address(topic: str) -> str:
    return "0x" + topic.lower().removeprefix("0x")[-40:]


def load_token_decimals(tokens: set[str]) -> dict[str, int]:
    if not tokens:
        return {}
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT token_address, decimals FROM token_dictionary WHERE token_address = ANY(%s)",
            (list(tokens),),
        )
        return {str(token).lower(): int(decimals) for token, decimals in cur.fetchall()}


def fetch_prices(tokens: set[str]) -> dict[str, float]:
    prices: dict[str, float] = {}
    token_list = [t.lower() for t in tokens if t and t != ZERO_POOL][:REAL_PRICE_MAX_TOKENS]
    for i in range(0, len(token_list), max(1, REAL_PRICE_BATCH_SIZE)):
        chunk = token_list[i : i + max(1, REAL_PRICE_BATCH_SIZE)]
        if not chunk:
            continue
        try:
            payload = request_json(
                f"{COINGECKO_BASE_URL}/simple/token_price/ethereum",
                {"contract_addresses": ",".join(chunk), "vs_currencies": "usd"},
                timeout=20,
            )
            for token, data in payload.items():
                usd = data.get("usd") if isinstance(data, dict) else None
                if usd is not None:
                    prices[token.lower()] = float(usd)
        except Exception as exc:
            print(f"price lookup failed for {len(chunk)} tokens: {exc}")
        time.sleep(REAL_SLEEP_SECONDS)
    return prices


def upsert_token_metadata(transfers: list[dict[str, Any]]) -> None:
    rows = {}
    for tx in transfers:
        token = str(tx.get("contractAddress", "")).lower()
        if not token:
            continue
        rows[token] = (
            token,
            str(tx.get("tokenSymbol") or "UNKNOWN")[:64],
            str(tx.get("tokenName") or tx.get("tokenSymbol") or "Unknown token")[:256],
            "ethereum",
            int(tx.get("tokenDecimal") or 18),
            "normal",
        )
    if not rows:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO token_dictionary
                (token_address, symbol, name, chain, decimals, risk_level)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_address) DO UPDATE
            SET symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                chain = EXCLUDED.chain,
                decimals = EXCLUDED.decimals,
                updated_at = now()
            """,
            list(rows.values()),
        )
        conn.commit()


def upsert_unknown_tokens(tokens: set[str]) -> None:
    if not tokens:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO token_dictionary
                (token_address, symbol, name, chain, decimals, risk_level)
            VALUES (%s, 'UNKNOWN', 'Unknown token', 'ethereum', 18, 'unknown')
            ON CONFLICT (token_address) DO NOTHING
            """,
            [(token,) for token in tokens],
        )
        conn.commit()


def transfer_to_row(tx: dict[str, Any], wallet: str, prices: dict[str, float], loaded_at: datetime):
    token = str(tx.get("contractAddress", "")).lower()
    block_time = datetime.fromtimestamp(int(tx.get("timeStamp") or 0), tz=timezone.utc)
    amount_token = decimal_amount(str(tx.get("value") or "0"), str(tx.get("tokenDecimal") or "0"))
    price = Decimal(str(prices.get(token, 0.0)))
    amount_usd = (amount_token * price).quantize(Decimal("0.0001"))
    tx_from = str(tx.get("from", "")).lower()
    tx_to = str(tx.get("to", "")).lower()
    side = "buy" if tx_to == wallet else "sell" if tx_from == wallet else "transfer"
    event_type = "erc20_transfer"
    return (
        "ethereum",
        block_time,
        int(tx.get("blockNumber") or 0),
        str(tx.get("hash") or ""),
        int(tx.get("logIndex") or 0),
        wallet,
        token,
        ZERO_POOL,
        event_type,
        side,
        amount_token,
        amount_usd,
        Decimal("0.0000"),
        "etherscan_tokentx",
        RUN_ID,
        loaded_at,
    )


def detect_swap_type(log: dict[str, Any]) -> str | None:
    topics = [str(t).lower() for t in log.get("topics", [])]
    if not topics:
        return None
    if topics[0] == UNISWAP_V2_SWAP_TOPIC:
        return "uniswap_v2_swap"
    if topics[0] == UNISWAP_V3_SWAP_TOPIC:
        return "uniswap_v3_swap"
    return None


def decode_wallet_transfer_deltas(receipt: dict[str, Any], wallet: str, decimals: dict[str, int]):
    deltas: dict[str, Decimal] = {}
    for log in receipt.get("logs", []) or []:
        topics = [str(t).lower() for t in log.get("topics", [])]
        if len(topics) < 3 or topics[0] != TRANSFER_TOPIC:
            continue
        token = str(log.get("address", "")).lower()
        from_addr = topic_to_address(topics[1])
        to_addr = topic_to_address(topics[2])
        amount_raw = hex_to_int(str(log.get("data", "0x0")))
        amount = decimal_amount(str(amount_raw), str(decimals.get(token, 18)))
        if to_addr == wallet:
            deltas[token] = deltas.get(token, Decimal(0)) + amount
        if from_addr == wallet:
            deltas[token] = deltas.get(token, Decimal(0)) - amount
    return deltas


def swap_rows_from_receipt(
    receipt: dict[str, Any],
    wallet: str,
    prices: dict[str, float],
    decimals: dict[str, int],
    loaded_at: datetime,
) -> list[tuple]:
    swap_logs = [
        log for log in receipt.get("logs", []) or []
        if detect_swap_type(log) is not None
    ]
    if not swap_logs:
        return []
    deltas = decode_wallet_transfer_deltas(receipt, wallet, decimals)
    bought = [(token, amount) for token, amount in deltas.items() if amount > 0]
    sold = [(token, -amount) for token, amount in deltas.items() if amount < 0]
    if not bought and not sold:
        return []

    tx_hash = str(receipt.get("transactionHash", ""))
    block_number = hex_to_int(str(receipt.get("blockNumber", "0x0")))
    block_time = receipt.get("_block_time")
    if not isinstance(block_time, datetime):
        block_time = datetime.now(timezone.utc)
    gas_used = Decimal(hex_to_int(str(receipt.get("gasUsed", "0x0"))))
    effective_gas_price = Decimal(hex_to_int(str(receipt.get("effectiveGasPrice", "0x0"))))
    fee_eth = gas_used * effective_gas_price / (Decimal(10) ** 18)
    fee_usd = (fee_eth * Decimal(str(prices.get("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 0.0)))).quantize(Decimal("0.0001"))
    rows = []
    primary_swap = swap_logs[0]
    pool = str(primary_swap.get("address", ZERO_POOL)).lower()
    event_type = detect_swap_type(primary_swap) or "dex_swap"
    base_log_index = hex_to_int(str(primary_swap.get("logIndex", "0x0")))

    for offset, (token, amount) in enumerate(sold):
        amount_usd = (amount * Decimal(str(prices.get(token, 0.0)))).quantize(Decimal("0.0001"))
        rows.append((
            "ethereum",
            block_time,
            block_number,
            tx_hash,
            base_log_index * 10 + offset,
            wallet,
            token,
            pool,
            event_type,
            "sell",
            amount,
            amount_usd,
            fee_usd if offset == 0 else Decimal("0.0000"),
            "etherscan_receipt_swap",
            RUN_ID,
            loaded_at,
        ))
    for offset, (token, amount) in enumerate(bought, start=len(rows)):
        amount_usd = (amount * Decimal(str(prices.get(token, 0.0)))).quantize(Decimal("0.0001"))
        rows.append((
            "ethereum",
            block_time,
            block_number,
            tx_hash,
            base_log_index * 10 + offset,
            wallet,
            token,
            pool,
            event_type,
            "buy",
            amount,
            amount_usd,
            Decimal("0.0000"),
            "etherscan_receipt_swap",
            RUN_ID,
            loaded_at,
        ))
    return rows


def fetch_swap_rows(wallets: list[str], prices: dict[str, float], loaded_at: datetime):
    swap_rows = []
    all_tokens: set[str] = set()
    receipts_to_decode: list[tuple[str, dict[str, Any]]] = []
    new_prices: dict[str, float] = {}

    for wallet in wallets:
        start_block = get_txlist_checkpoint(wallet)
        txs = fetch_normal_transactions(wallet, start_block)
        txs = [tx for tx in txs if str(tx.get("isError", "0")) == "0" and str(tx.get("hash", ""))]
        print(f"{wallet}: fetched {len(txs)} normal txs for swap receipts from block {start_block}")
        if txs:
            set_txlist_checkpoint(wallet, max(int(tx.get("blockNumber") or 0) for tx in txs))
        for tx in txs[:REAL_MAX_RECEIPTS_PER_WALLET]:
            receipt = fetch_receipt(str(tx.get("hash")))
            if not receipt:
                continue
            receipt["_block_time"] = datetime.fromtimestamp(int(tx.get("timeStamp") or 0), tz=timezone.utc)
            if any(detect_swap_type(log) for log in receipt.get("logs", []) or []):
                receipts_to_decode.append((wallet, receipt))
                for log in receipt.get("logs", []) or []:
                    topics = [str(t).lower() for t in log.get("topics", [])]
                    if len(topics) >= 3 and topics[0] == TRANSFER_TOPIC:
                        all_tokens.add(str(log.get("address", "")).lower())
            time.sleep(REAL_SLEEP_SECONDS)

    upsert_unknown_tokens(all_tokens)
    missing_prices = {token for token in all_tokens if token not in prices}
    if missing_prices:
        new_prices = fetch_prices(missing_prices)
        prices.update(new_prices)
    decimals = load_token_decimals(all_tokens)
    for wallet, receipt in receipts_to_decode:
        swap_rows.extend(swap_rows_from_receipt(receipt, wallet, prices, decimals, loaded_at))
    return swap_rows, all_tokens, new_prices


def insert_rows(ch, rows) -> None:
    if not rows:
        return
    ch.insert(
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


def insert_ingest_run(ch, source: str, started_at: datetime, status: str, wallets_count: int, rows_inserted: int, tokens_priced: int, error: str = ""):
    finished_at = datetime.now(timezone.utc)
    ch.insert(
        "raw.ingest_runs",
        [(
            RUN_ID,
            source,
            started_at,
            finished_at,
            status,
            wallets_count,
            rows_inserted,
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


def insert_prices(ch, prices: dict[str, float]) -> None:
    if not prices:
        return
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    version = int(datetime.now(timezone.utc).timestamp())
    rows = [
        (token, now, float(price), "coingecko-real", version, datetime.now(timezone.utc))
        for token, price in prices.items()
    ]
    ch.insert(
        "raw.token_prices_hourly",
        rows,
        column_names=["token_address", "price_hour", "price_usd", "source", "version", "loaded_at"],
    )


def fetch_wallet_transfer_batch(wallet: str) -> tuple[str, int, list[dict[str, Any]], int | None]:
    start_block = get_checkpoint(wallet)
    transfers = fetch_erc20_transfers(wallet, start_block)
    max_block = max((int(tx.get("blockNumber") or 0) for tx in transfers), default=None)
    if REAL_SLEEP_SECONDS > 0:
        time.sleep(REAL_SLEEP_SECONDS)
    return wallet, start_block, transfers, max_block


def flush_rows(ch, rows: list[tuple]) -> int:
    if not rows:
        return 0
    insert_rows(ch, rows)
    inserted = len(rows)
    rows.clear()
    return inserted


def main() -> None:
    ensure_api_key()
    started_at = datetime.now(timezone.utc)
    ch = ch_client()
    rows_inserted = 0
    wallets_count = 0
    tokens_priced = 0
    try:
        wallets = fetch_wallets(ch)
        wallets_count = len(wallets)
        if not wallets:
            raise SystemExit("No wallets configured. Set REAL_WALLETS or load raw.wallet_watchlist.")

        workers = max(1, REAL_WORKERS)
        print(
            f"Starting real ingest: wallets={len(wallets)}, max_tx_per_wallet={REAL_MAX_TX_PER_WALLET}, "
            f"workers={workers}, price_lookup={REAL_ENABLE_PRICE_LOOKUP}, swaps={REAL_ENABLE_SWAPS}",
            flush=True,
        )

        all_transfers_for_swaps: list[tuple[str, list[dict[str, Any]]]] = []
        metadata_buffer: list[dict[str, Any]] = []
        tokens: set[str] = set()
        prices: dict[str, float] = {}
        loaded_at = datetime.now(timezone.utc)
        rows: list[tuple] = []
        completed_wallets = 0
        transfer_rows_seen = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_wallet_transfer_batch, wallet): wallet for wallet in wallets}
            for future in as_completed(futures):
                wallet = futures[future]
                try:
                    wallet, start_block, transfers, max_block = future.result()
                except Exception as exc:
                    print(f"{wallet}: failed to fetch transfers: {exc}", flush=True)
                    continue

                completed_wallets += 1
                transfer_rows_seen += len(transfers)
                print(
                    f"{wallet}: fetched {len(transfers)} transfers from block {start_block} "
                    f"({completed_wallets}/{len(wallets)})",
                    flush=True,
                )
                if not transfers:
                    continue

                metadata_buffer.extend(transfers)
                if REAL_ENABLE_SWAPS:
                    all_transfers_for_swaps.append((wallet, transfers))
                tokens.update(str(t.get("contractAddress", "")).lower() for t in transfers)
                if max_block is not None:
                    set_checkpoint(wallet, max_block)

                for tx in transfers:
                    rows.append(transfer_to_row(tx, wallet, prices, loaded_at))
                    if len(rows) >= REAL_BATCH_SIZE:
                        rows_inserted += flush_rows(ch, rows)
                        print(f"Inserted {rows_inserted} raw rows so far.", flush=True)

                if len(metadata_buffer) >= REAL_BATCH_SIZE:
                    upsert_token_metadata(metadata_buffer)
                    metadata_buffer.clear()

        upsert_token_metadata(metadata_buffer)
        rows_inserted += flush_rows(ch, rows)

        if REAL_ENABLE_PRICE_LOOKUP:
            prices = fetch_prices(tokens)
        else:
            prices = {}
        tokens_priced = len(prices)
        insert_prices(ch, prices)

        if REAL_ENABLE_SWAPS:
            swap_rows, _swap_tokens, swap_prices = fetch_swap_rows(wallets, prices, loaded_at)
            insert_prices(ch, swap_prices)
            tokens_priced = len(prices)
            for row in swap_rows:
                rows.append(row)
                if len(rows) >= REAL_BATCH_SIZE:
                    rows_inserted += flush_rows(ch, rows)
                    print(f"Inserted {rows_inserted} raw rows so far.", flush=True)
        rows_inserted += flush_rows(ch, rows)

        ch.command("SYSTEM RELOAD DICTIONARY mart.tokens_dict")
        ch.command("SYSTEM RELOAD DICTIONARY mart.prices_dict")
        insert_ingest_run(ch, "etherscan_real", started_at, "success", wallets_count, rows_inserted, tokens_priced)
        print(
            f"Inserted real ingest data for {len(wallets)} wallets, "
            f"{transfer_rows_seen} transfers, {len(prices)} priced tokens, {rows_inserted} raw rows."
        )
    except Exception as exc:
        insert_ingest_run(ch, "etherscan_real", started_at, "failed", wallets_count, rows_inserted, tokens_priced, str(exc))
        raise


if __name__ == "__main__":
    main()
