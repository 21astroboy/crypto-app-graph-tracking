import math
import os
import time
from collections import defaultdict
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

TOKEN_PATH_LOOKBACK_DAYS = int(env("TOKEN_PATH_LOOKBACK_DAYS", "30"))
TOKEN_PATH_MAX_EVENTS = int(env("TOKEN_PATH_MAX_EVENTS", "50000"))
TOKEN_PATH_MIN_TRANSITIONS = int(env("TOKEN_PATH_MIN_TRANSITIONS", "3"))
TOKEN_PATH_MIN_WALLETS = int(env("TOKEN_PATH_MIN_WALLETS", "2"))
TOKEN_PATH_TOP_EDGES = int(env("TOKEN_PATH_TOP_EDGES", "500"))
TOKEN_PATH_TOP_ROUTES = int(env("TOKEN_PATH_TOP_ROUTES", "50"))
TOKEN_PATH_MAX_HOPS = int(env("TOKEN_PATH_MAX_HOPS", "4"))


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


def to_float(value) -> float:
    if isinstance(value, Decimal):
        return float(value)
    if value is None:
        return 0.0
    return float(value)


def fetch_events(ch) -> dict[str, list[dict]]:
    query = f"""
        SELECT
            wallet_address,
            token_address,
            block_time,
            side,
            amount_usd
        FROM raw.dex_transactions
        WHERE block_time >= now() - INTERVAL {TOKEN_PATH_LOOKBACK_DAYS} DAY
          AND wallet_address != ''
          AND token_address != ''
          AND token_address != '0x0000000000000000000000000000000000000000'
        ORDER BY wallet_address, block_time, tx_hash, log_index
        LIMIT {TOKEN_PATH_MAX_EVENTS}
    """
    result = ch.query(query)
    wallets: dict[str, list[dict]] = defaultdict(list)
    for wallet, token, block_time, side, amount_usd in result.result_rows:
        wallets[wallet].append(
            {
                "token": token,
                "time": block_time,
                "side": side,
                "amount_usd": to_float(amount_usd),
            }
        )
    return wallets


def future_return_proxy(events: list[dict], start_index: int, token: str) -> float:
    buy_usd = 0.0
    sell_usd = 0.0
    for event in events[start_index:]:
        if event["token"] != token:
            continue
        if event["side"] == "buy":
            buy_usd += event["amount_usd"]
        elif event["side"] == "sell":
            sell_usd += event["amount_usd"]
    if buy_usd <= 0:
        return 0.0
    return max(-1.0, min(5.0, (sell_usd - buy_usd) / buy_usd))


def build_transition_edges(wallets: dict[str, list[dict]]) -> list[dict]:
    aggregate: dict[tuple[str, str], dict] = {}
    for wallet, events in wallets.items():
        previous_token = None
        for idx, event in enumerate(events):
            token = event["token"]
            if not previous_token or previous_token == token:
                previous_token = token
                continue

            key = (previous_token, token)
            stats = aggregate.setdefault(
                key,
                {
                    "from_token": previous_token,
                    "to_token": token,
                    "wallets": set(),
                    "transition_count": 0,
                    "returns": [],
                    "volume_usd": 0.0,
                },
            )
            stats["wallets"].add(wallet)
            stats["transition_count"] += 1
            stats["returns"].append(future_return_proxy(events, idx, token))
            stats["volume_usd"] += max(0.0, event["amount_usd"])
            previous_token = token

    rows = []
    for stats in aggregate.values():
        support_wallets = len(stats["wallets"])
        transition_count = int(stats["transition_count"])
        if support_wallets < TOKEN_PATH_MIN_WALLETS or transition_count < TOKEN_PATH_MIN_TRANSITIONS:
            continue

        avg_return = sum(stats["returns"]) / len(stats["returns"]) if stats["returns"] else 0.0
        confidence = min(1.0, support_wallets / 10.0) * 0.6 + min(1.0, transition_count / 30.0) * 0.4
        edge_weight = (
            max(0.0, avg_return) * 50.0
            + math.log1p(stats["volume_usd"]) * 4.0
            + support_wallets * 3.0
            + transition_count * 0.5
            + confidence * 10.0
        )
        rows.append(
            {
                "from_token": stats["from_token"],
                "to_token": stats["to_token"],
                "support_wallets": support_wallets,
                "transition_count": transition_count,
                "avg_return_proxy": round(avg_return, 6),
                "volume_usd": round(stats["volume_usd"], 4),
                "confidence": round(confidence, 6),
                "edge_weight": round(edge_weight, 6),
            }
        )
    rows.sort(key=lambda row: row["edge_weight"], reverse=True)
    return rows[:TOKEN_PATH_TOP_EDGES]


class DisjointSet:
    def __init__(self):
        self.parent = {}

    def find(self, item):
        self.parent.setdefault(item, item)
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left, right) -> bool:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return False
        self.parent[root_right] = root_left
        return True


def build_maximum_spanning_forest(edges: list[dict]) -> list[dict]:
    best_pair = {}
    for edge in edges:
        key = tuple(sorted([edge["from_token"], edge["to_token"]]))
        if key not in best_pair or edge["edge_weight"] > best_pair[key]["edge_weight"]:
            best_pair[key] = edge

    dsu = DisjointSet()
    tree = []
    for edge in sorted(best_pair.values(), key=lambda row: row["edge_weight"], reverse=True):
        if dsu.union(edge["from_token"], edge["to_token"]):
            tree.append(edge)
    return tree


def build_routes(edges: list[dict], symbols: dict[str, str]) -> list[dict]:
    outgoing: dict[str, list[dict]] = defaultdict(list)
    for edge in edges:
        outgoing[edge["from_token"]].append(edge)
    for token_edges in outgoing.values():
        token_edges.sort(key=lambda row: row["edge_weight"], reverse=True)

    routes = []

    def dfs(path: list[str], edge_path: list[dict]):
        if edge_path:
            expected_return = sum(edge["avg_return_proxy"] for edge in edge_path)
            confidence = sum(edge["confidence"] for edge in edge_path) / len(edge_path)
            route_weight = sum(edge["edge_weight"] for edge in edge_path)
            support_wallets = min(edge["support_wallets"] for edge in edge_path)
            routes.append(
                {
                    "path_tokens": list(path),
                    "path_symbols": [symbols.get(token, token[:8]) for token in path],
                    "hops": len(edge_path),
                    "expected_return_proxy": round(expected_return, 6),
                    "confidence": round(confidence, 6),
                    "route_weight": round(route_weight, 6),
                    "support_wallets": support_wallets,
                }
            )
        if len(edge_path) >= TOKEN_PATH_MAX_HOPS:
            return
        for edge in outgoing.get(path[-1], [])[:8]:
            next_token = edge["to_token"]
            if next_token in path:
                continue
            dfs(path + [next_token], edge_path + [edge])

    for start_token in list(outgoing)[:80]:
        dfs([start_token], [])

    routes.sort(
        key=lambda row: (
            row["expected_return_proxy"],
            row["confidence"],
            row["route_weight"],
        ),
        reverse=True,
    )
    return routes[:TOKEN_PATH_TOP_ROUTES]


def load_symbols(tokens: set[str]) -> dict[str, str]:
    if not tokens:
        return {}
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT token_address, symbol FROM token_dictionary WHERE token_address = ANY(%s)",
            (list(tokens),),
        )
        return {str(token).lower(): str(symbol) for token, symbol in cur.fetchall()}


def insert_outputs(ch, edges: list[dict], tree_edges: list[dict], routes: list[dict]) -> None:
    calculated_at = datetime.now(timezone.utc)
    version = int(time.time())
    ch.command("TRUNCATE TABLE graph.token_transition_edges")
    ch.command("TRUNCATE TABLE graph.token_spanning_tree_edges")
    ch.command("TRUNCATE TABLE graph.token_route_recommendations")

    if edges:
        ch.insert(
            "graph.token_transition_edges",
            [
                (
                    edge["from_token"],
                    edge["to_token"],
                    edge["support_wallets"],
                    edge["transition_count"],
                    edge["avg_return_proxy"],
                    edge["volume_usd"],
                    edge["confidence"],
                    edge["edge_weight"],
                    calculated_at,
                    version,
                )
                for edge in edges
            ],
            column_names=[
                "from_token",
                "to_token",
                "support_wallets",
                "transition_count",
                "avg_return_proxy",
                "volume_usd",
                "confidence",
                "edge_weight",
                "calculated_at",
                "version",
            ],
        )

    if tree_edges:
        ch.insert(
            "graph.token_spanning_tree_edges",
            [
                (
                    edge["from_token"],
                    edge["to_token"],
                    edge["support_wallets"],
                    edge["transition_count"],
                    edge["edge_weight"],
                    rank,
                    calculated_at,
                    version,
                )
                for rank, edge in enumerate(tree_edges, start=1)
            ],
            column_names=[
                "from_token",
                "to_token",
                "support_wallets",
                "transition_count",
                "edge_weight",
                "tree_rank",
                "calculated_at",
                "version",
            ],
        )

    if routes:
        ch.insert(
            "graph.token_route_recommendations",
            [
                (
                    rank,
                    route["path_tokens"],
                    route["path_symbols"],
                    route["hops"],
                    route["expected_return_proxy"],
                    route["confidence"],
                    route["route_weight"],
                    route["support_wallets"],
                    calculated_at,
                    version,
                )
                for rank, route in enumerate(routes, start=1)
            ],
            column_names=[
                "route_rank",
                "path_tokens",
                "path_symbols",
                "hops",
                "expected_return_proxy",
                "confidence",
                "route_weight",
                "support_wallets",
                "calculated_at",
                "version",
            ],
        )


def main() -> None:
    ch = ch_client()
    wallets = fetch_events(ch)
    edges = build_transition_edges(wallets)
    tree_edges = build_maximum_spanning_forest(edges)
    tokens = {edge["from_token"] for edge in edges} | {edge["to_token"] for edge in edges}
    symbols = load_symbols(tokens)
    routes = build_routes(edges, symbols)
    insert_outputs(ch, edges, tree_edges, routes)
    print(
        f"Token paths finished: wallets={len(wallets)}, "
        f"transition_edges={len(edges)}, tree_edges={len(tree_edges)}, routes={len(routes)}."
    )
    if routes:
        print(
            "Top route: "
            f"{' -> '.join(routes[0]['path_symbols'])} "
            f"return_proxy={routes[0]['expected_return_proxy']} "
            f"confidence={routes[0]['confidence']}"
        )


if __name__ == "__main__":
    main()
