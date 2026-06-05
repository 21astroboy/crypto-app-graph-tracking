# ClickHouse Smart Wallet Profiler

Локальный учебный проект для курса по NoSQL/ClickHouse: сбор транзакций smart-wallets, расчет балансов, дневной активности, smart-money flows, первых покупок и графовых признаков.

## Что демонстрирует проект

- `MergeTree` для append-only сырых транзакций.
- `ReplacingMergeTree` для актуального списка кошельков и рейтингов.
- `SummingMergeTree` для балансов и денежных потоков.
- `AggregatingMergeTree` для агрегатных состояний: дневная активность, first buy, graph edges.
- `Materialized View` для автоматического наполнения витрин.
- PostgreSQL как OLTP-слой для справочников, алертов и labels.
- ClickHouse dictionaries поверх PostgreSQL и ClickHouse-таблиц.
- Skip-индексы `bloom_filter` для ad-hoc поиска по токену, пулу и tx hash.
- Grafana dashboard поверх ClickHouse.

## Быстрый старт

```bash
docker compose up -d clickhouse postgres grafana
docker compose --profile jobs up demo-ingest
```

Или через `make`:

```bash
make up
make ingest
make discover
make graph
make token-paths
```

Для полной подготовки стенда перед показом:

```bash
make demo-flow
```

## Маленький real ingest

Для реальных данных используется легкий режим без большого backfill:

- ERC-20 transfers по кошелькам через Etherscan API V2;
- DEX swap detection по transaction receipts для Uniswap V2/V3 Swap events;
- текущий hourly price snapshot через CoinGecko simple token price;
- checkpoint-и по каждому кошельку в PostgreSQL;
- маленькие лимиты `REAL_MAX_WALLETS`, `REAL_MAX_TX_PER_WALLET`, `REAL_BATCH_SIZE`;
- без `pandas` и больших in-memory датафреймов.

```bash
cp .env.example .env
# заполни ETHERSCAN_API_KEY и при желании REAL_WALLETS
make real-ingest
```

Важно: `real-ingest` нормализует реальные ERC-20 transfers в события `erc20_transfer`
и дополнительно декодирует swap-транзакции Uniswap V2/V3 из receipt logs. Для swap
он пишет две строки: `sell` для токена, который кошелек отдал, и `buy` для токена,
который получил. Это проходит через те же MV и витрины. Поддержку 1inch/0x/Curve
можно добавить следующим шагом.

## Price ingest

`price-ingest` отдельно обновляет hourly цены для токенов, которые недавно
встречались в `raw.dex_transactions`. Он выбирает только токены без свежей цены,
ходит в CoinGecko маленькими пачками и пишет результат в
`raw.token_prices_hourly`.

```bash
make price-ingest
```

Так транзакции не зависят от доступности price API: real ingest может загрузить
raw-события, а цены догружаются отдельной job.

Источник токенов для цен можно ограничить через `PRICE_EVENT_SOURCE=all|demo|real`.

## Graph similarity

`graph-similarity` пересчитывает wallet-wallet similarity поверх
`graph.v_wallet_token_edges`:

- строит пары кошельков по общим токенам;
- считает `jaccard_tokens` по множествам токенов;
- считает `time_correlation` по близости последней активности в общих токенах;
- пишет top edges в `graph.wallet_similarity_edges`.
- для demo noisy-token фильтр выключен (`GRAPH_DROP_NOISY_TOKENS=false`), чтобы
  было видно связный граф; на больших real-данных его можно включить.
- по умолчанию job очищает `graph.wallet_similarity_edges` перед full recompute.

```bash
make graph
```

Это отдельный графовый слой поверх ClickHouse-витрин: его удобно показывать для
поиска похожих стратегий и кластеров кошельков.

## Token paths

`token-paths` строит граф переходов `token -> token` по историческим действиям
кошельков:

- считает, как часто кошельки переходили от одного токена к другому;
- оценивает `avg_return_proxy`, `confidence` и `edge_weight`;
- строит maximum spanning forest по самым сильным token-token связям;
- сохраняет top маршруты в `graph.token_route_recommendations`.

```bash
make token-paths
```

Это не торговая рекомендация, а исторический proxy-сигнал: какие маршруты
smart-money выглядели сильными на загруженных данных.

## Discovery smart-wallet candidates

`wallet-discovery` строит легкий локальный рейтинг кандидатов по событиям из
`raw.dex_transactions`:

- считает активность, количество токенов, volume, ROI/PnL proxy;
- пишет подробные метрики в PostgreSQL `wallet_candidates`;
- добавляет top-N в PostgreSQL watchlist `Auto discovery`;
- публикует top-N в ClickHouse `raw.wallet_watchlist`.
- умеет фильтровать источник событий через `DISCOVERY_EVENT_SOURCE=all|demo|real`.

```bash
make discover
```

Это первый слой автообновляемого списка кошельков. Для боевого top ROI его можно
дополнить discovery из DEX logs / Bitquery / GMGN / Nansen.

После загрузки демо-данных можно открыть:

- Grafana: http://localhost:3000
- Login/password: `admin` / `admin`
- Dashboard: `Smart Wallet Overview`
- Dashboard: `Wallet Graph Explorer`

## DataGrip

ClickHouse:

- Host: `localhost`
- HTTP port: `8123`
- Native port: `9000`
- User: `student`
- Password: `student`
- Databases: `raw`, `mart`, `graph`, `crypto`

PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `wallet_meta`
- User: `student`
- Password: `student`

## Основные сущности

`raw.dex_transactions`  
Сырые swap/buy/sell/add_liquidity/remove_liquidity события. Это факты, поэтому используется `MergeTree`.

`raw.wallet_watchlist`  
Ежедневно обновляемый список top ROI кошельков. Используется `ReplacingMergeTree(version, is_deleted)`, потому что одна и та же сущность получает новые версии.

`wallet_candidates` в PostgreSQL  
OLTP-таблица с последними рассчитанными кандидатами, score и JSON-метриками.
Нужна для ручной проверки, алертов и управляемого watchlist.

`raw.token_prices_hourly`  
Hourly цены токенов. Используется `ReplacingMergeTree(version)`, потому что цена за час может быть уточнена.

`mart.wallet_token_balances`  
Схлопнутые балансы и net-flow по паре wallet-token. Используется `SummingMergeTree`.

`mart.wallet_daily_activity`  
Дневная активность кошельков. Используется `AggregatingMergeTree` и состояния `countState`, `uniqState`, `sumState`, `minState`, `maxState`.

`mart.token_smart_money_flow_5m`  
Пятиминутный smart-money flow по токенам. Используется `SummingMergeTree`.

`mart.first_wallet_buys`  
Первые покупки токена smart-кошельком. Используется `AggregatingMergeTree` с `minState` и `argMinState`.

`graph.wallet_token_edges`  
Графовая витрина wallet-token. Используется `AggregatingMergeTree`.

`graph.wallet_similarity_edges`  
Результат Python graph job: похожесть кошельков. Используется `ReplacingMergeTree(version)`.

`graph.token_transition_edges`, `graph.token_spanning_tree_edges`, `graph.token_route_recommendations`  
Граф переходов между токенами, максимальный остов и top маршруты. Используются
для анализа исторически сильных token paths.

`Wallet Graph Explorer` в Grafana  
Визуализирует `graph.wallet_similarity_edges` через Node Graph и таблицы:
top similarity edges, ego graph и объяснение связи через общие токены.
Также показывает token transition network, maximum spanning tree и top token
routes.

`mart.tokens_dict`, `mart.wallet_labels_dict`, `mart.prices_dict`, `mart.smart_wallets_dict`  
Словари ClickHouse для быстрых lookup-ов из PostgreSQL-справочников и ClickHouse-витрин.

## Запросы для демонстрации

Открой файлы [docs/demo_queries.sql](/Users/kirill/Documents/Clickhouse/docs/demo_queries.sql)
и [docs/pg_demo_queries.sql](/Users/kirill/Documents/Clickhouse/docs/pg_demo_queries.sql)
в DataGrip. Первый выполняется в ClickHouse-коннектах, второй в `PG - metadata`.

## Полезные команды

```bash
make help        # список команд
make ps          # статус контейнеров
make ch          # clickhouse-client внутри контейнера
make psql        # psql внутри контейнера
make demo-flow   # подготовить свежие demo, real, discovery, prices и graph
make discover    # пересчитать кандидатов и обновить watchlist
make price-ingest # обновить hourly цены токенов
make graph       # пересчитать похожесть кошельков
make token-paths # пересчитать token-transition graph и маршруты
make reset-demo  # очистить CH demo-таблицы и залить один чистый набор данных
make stop        # остановить контейнеры, сохранив volumes
```

## Замечание про старый SMT-проект

На машине также есть похожий проект `smt` из `/Users/kirill/Desktop/ClickHouse/cryp`.
Он использует те же локальные порты: `8123`, `9000`, `5432`, `3000`.
Поэтому одновременно два стенда на стандартных портах не поднимутся; перед переключением останови один из них.

## Как перейти с demo ingest на реальный API

Текущий `jobs/demo_ingest.py` имитирует внешний blockchain/DEX API. Для реального контура нужно заменить генератор на:

- загрузку watchlist раз в день;
- загрузку новых транзакций раз в `N` минут;
- hourly загрузку цен;
- checkpoint/cursor таблицу в Postgres;
- retries и rate limit;
- отдельную graph job для пересчета `graph.wallet_similarity_edges`.

ClickHouse-таблицы и MV при этом можно оставить почти без изменений.
