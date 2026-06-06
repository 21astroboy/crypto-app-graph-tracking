# ClickHouse Smart Wallet Profiler: demo script

## Подготовка перед показом

```bash
make up
make demo-flow
```

`demo-flow` очищает ClickHouse demo-слои, сбрасывает Etherscan checkpoints,
загружает demo, легкий real-срез, discovery, prices и graph similarity.

Открой в DataGrip:

- `CH - raw layer`
- `CH - mart layer`
- `CH - graph layer`
- `PG - metadata`
- файл `docs/demo_queries.sql`
- файл `docs/pg_demo_queries.sql`

## 1. Идея проекта

Проект профилирует smart-wallets: собирает события кошельков, складывает сырые
факты в ClickHouse, через materialized views строит витрины для балансов,
активности, smart-money flow, discovery и графовой аналитики.

PostgreSQL используется как OLTP-слой для справочников, watchlists, labels,
alerting metadata и checkpoints.

## 2. Архитектура

Покажи `README.md` или проговори схему:

```mermaid
flowchart LR
    api["Etherscan / CoinGecko / demo generator"] --> jobs["Python jobs"]
    jobs --> raw["ClickHouse raw"]
    raw --> mv["Materialized Views"]
    mv --> mart["ClickHouse mart"]
    mv --> graph["ClickHouse graph"]
    jobs --> pg["PostgreSQL metadata"]
    pg --> dict["ClickHouse dictionaries"]
    dict --> mart
    mart --> dg["DataGrip / Grafana"]
    graph --> dg
```

## 3. Что показать в DataGrip

Выполни запросы из `docs/demo_queries.sql`:

1. `00. Layer health`  
   Показывает, что данные есть во всех слоях.

2. `01. Engines used in the project`  
   Объясни выбор движков:
   `MergeTree` для фактов, `ReplacingMergeTree` для версий,
   `SummingMergeTree` для счетчиков и flow, `AggregatingMergeTree` для состояний.

3. `03. Raw fact table` и `04. Real ingest event mix`  
   Покажи, что есть demo и real события.

4. `06. Wallet balances` и `07. Daily activity`  
   Это автоматические витрины из MV.

5. В `docs/pg_demo_queries.sql`: `01. Discovery candidates` и
   `02. Auto discovery watchlist`  
   Покажи, что система сама ранжирует кандидатов и пишет watchlist.

6. `13. Latest token prices`  
   Отдельная price job независимо обновляет цены.

7. `15. Top wallet similarity edges`, `16. Ego graph`, `17. Explain one edge`  
   Графовая часть: похожесть кошельков по общим токенам и близости активности.

8. `19. Token transition graph`, `20. Maximum spanning tree`,
   `21. Top token route recommendations`  
   Покажи граф токеновых переходов и маршруты как исторические proxy-сигналы,
   а не финансовую рекомендацию.

9. Открой Grafana dashboard `Wallet Graph Explorer`  
   Покажи два графа: wallet similarity network и token transition network.
   Затем покажи maximum spanning tree и top token routes.

10. `18. Ingest run log`  
   Покажи наблюдаемость: видно, какие jobs запускались и сколько строк вставили.

## 4. Ключевые фразы

- Сырые события append-only, поэтому `raw.dex_transactions` использует `MergeTree`
  и TTL на 180 дней.
- Актуальные списки кошельков и рейтинги версионируются через
  `ReplacingMergeTree`.
- Балансы и flow схлопываются через `SummingMergeTree`.
- Дневная активность, first buys и graph edges используют агрегатные состояния в
  `AggregatingMergeTree`.
- Python jobs не считают витрины вручную: они грузят факты, а ClickHouse MV
  автоматически обновляют аналитические слои.
- Postgres нужен не как аналитическое хранилище, а как OLTP-слой: справочники,
  labels, watchlists, checkpoints, alert rules.
- Graph job показывает, что поверх ClickHouse-витрин можно строить ML/graph
  признаки без выгрузки всего raw.

## 5. Что сказать про ограничения

- Real ingest сделан легким, чтобы локальный проект не ел память и не бил API.
- Top ROI в demo считается proxy-метрикой; для production можно подключить
  DEX logs, Bitquery, GMGN, Nansen или свой backfill.
- CoinGecko free API может давать rate limit, поэтому цены вынесены в отдельную
  job и не блокируют транзакционный ingest.
