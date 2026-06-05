import itertools
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


CLICKHOUSE_HOST = env("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(env("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = env("CLICKHOUSE_DB", "crypto")
CLICKHOUSE_USER = env("CLICKHOUSE_USER", "student")
CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "student")

GRAPH_MAX_WALLETS = int(env("GRAPH_MAX_WALLETS", "300"))
GRAPH_MAX_TOKENS_PER_WALLET = int(env("GRAPH_MAX_TOKENS_PER_WALLET", "60"))
GRAPH_MIN_COMMON_TOKENS = int(env("GRAPH_MIN_COMMON_TOKENS", "2"))
GRAPH_TOP_EDGES = int(env("GRAPH_TOP_EDGES", "1000"))
GRAPH_LOOKBACK_DAYS = int(env("GRAPH_LOOKBACK_DAYS", "30"))
GRAPH_DROP_NOISY_TOKENS = env("GRAPH_DROP_NOISY_TOKENS", "false").lower() in {"1", "true", "yes", "y"}
GRAPH_TRUNCATE_BEFORE_INSERT = env("GRAPH_TRUNCATE_BEFORE_INSERT", "true").lower() in {"1", "true", "yes", "y"}


def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def to_float(value) -> float:
    if isinstance(value, Decimal):
        return float(value)
    if value is None:
        return 0.0
    return float(value)


def fetch_wallet_edges(ch) -> dict[str, dict[str, dict]]:
    query = f"""
        SELECT
            wallet_address,
            token_address,
            first_seen,
            last_seen,
            volume_usd,
            tx_count
        FROM graph.v_wallet_token_edges
        WHERE last_seen >= now() - INTERVAL {GRAPH_LOOKBACK_DAYS} DAY
          AND wallet_address IN
          (
              SELECT wallet_address
              FROM graph.v_wallet_token_edges
              WHERE last_seen >= now() - INTERVAL {GRAPH_LOOKBACK_DAYS} DAY
              GROUP BY wallet_address
              ORDER BY sum(volume_usd) DESC, sum(tx_count) DESC
              LIMIT {GRAPH_MAX_WALLETS}
          )
        ORDER BY wallet_address, volume_usd DESC, tx_count DESC
    """
    result = ch.query(query)

    wallets: dict[str, dict[str, dict]] = defaultdict(dict)
    token_counts: dict[str, int] = defaultdict(int)
    for wallet, token, first_seen, last_seen, volume_usd, tx_count in result.result_rows:
        if len(wallets[wallet]) >= GRAPH_MAX_TOKENS_PER_WALLET:
            continue
        wallets[wallet][token] = {
            "first_seen": first_seen,
            "last_seen": last_seen,
            "volume_usd": to_float(volume_usd),
            "tx_count": int(tx_count),
        }
        token_counts[token] += 1

    # Very common tokens create dense cliques and weak graph signal. Keep them usable,
    # but remove edges for tokens that touch almost every selected wallet.
    max_token_degree = max(25, int(len(wallets) * 0.25))
    noisy_tokens = set()
    if GRAPH_DROP_NOISY_TOKENS and len(token_counts) > 50:
        noisy_tokens = {token for token, count in token_counts.items() if count > max_token_degree}
    if noisy_tokens:
        for token_edges in wallets.values():
            for token in noisy_tokens:
                token_edges.pop(token, None)
    return {wallet: edges for wallet, edges in wallets.items() if edges}


def build_similarity_edges(wallets: dict[str, dict[str, dict]]) -> list[tuple]:
    token_to_wallets: dict[str, set[str]] = defaultdict(set)
    for wallet, token_edges in wallets.items():
        for token in token_edges:
            token_to_wallets[token].add(wallet)

    pair_common: dict[tuple[str, str], set[str]] = defaultdict(set)
    for token, token_wallets in token_to_wallets.items():
        if len(token_wallets) < 2:
            continue
        for wallet_a, wallet_b in itertools.combinations(sorted(token_wallets), 2):
            pair_common[(wallet_a, wallet_b)].add(token)

    calculated_at = datetime.now(timezone.utc)
    version = int(time.time())
    rows = []
    for (wallet_a, wallet_b), common_tokens in pair_common.items():
        if len(common_tokens) < GRAPH_MIN_COMMON_TOKENS:
            continue

        tokens_a = set(wallets[wallet_a])
        tokens_b = set(wallets[wallet_b])
        union_size = len(tokens_a | tokens_b)
        if union_size == 0:
            continue

        jaccard = len(common_tokens) / union_size
        time_correlation = calc_time_correlation(wallets[wallet_a], wallets[wallet_b], common_tokens)
        shared_volume = sum(
            min(wallets[wallet_a][token]["volume_usd"], wallets[wallet_b][token]["volume_usd"])
            for token in common_tokens
        )
        volume_boost = min(1.0, math.log1p(shared_volume) / 12.0)
        similarity_score = (jaccard * 70.0) + (time_correlation * 25.0) + (volume_boost * 5.0)

        rows.append(
            (
                wallet_a,
                wallet_b,
                len(common_tokens),
                round(jaccard, 6),
                round(time_correlation, 6),
                round(similarity_score, 6),
                calculated_at,
                version,
            )
        )

    rows.sort(key=lambda row: (row[5], row[2]), reverse=True)
    return rows[:GRAPH_TOP_EDGES]


def calc_time_correlation(edges_a: dict[str, dict], edges_b: dict[str, dict], common_tokens: set[str]) -> float:
    if not common_tokens:
        return 0.0
    scores = []
    for token in common_tokens:
        last_a = edges_a[token]["last_seen"]
        last_b = edges_b[token]["last_seen"]
        diff_days = abs((last_a - last_b).total_seconds()) / 86400.0
        scores.append(math.exp(-diff_days / 7.0))
    return sum(scores) / len(scores)


def insert_edges(ch, rows: list[tuple]) -> None:
    if GRAPH_TRUNCATE_BEFORE_INSERT:
        ch.command("TRUNCATE TABLE graph.wallet_similarity_edges")
    if not rows:
        return
    ch.insert(
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
    ch = ch_client()
    wallets = fetch_wallet_edges(ch)
    rows = build_similarity_edges(wallets)
    insert_edges(ch, rows)
    print(
        f"Graph similarity finished: wallets={len(wallets)}, "
        f"edges_inserted={len(rows)}, min_common_tokens={GRAPH_MIN_COMMON_TOKENS}."
    )
    if rows:
        best = rows[0]
        print(
            "Top edge: "
            f"{best[0]} <-> {best[1]} score={best[5]} "
            f"common_tokens={best[2]} jaccard={best[3]} time_corr={best[4]}"
        )


if __name__ == "__main__":
    main()
