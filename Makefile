SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: up
up: ## Start ClickHouse, Postgres and Grafana
	docker compose up -d clickhouse postgres grafana
	@echo ""
	@echo "  ClickHouse HTTP   http://localhost:8123"
	@echo "  ClickHouse native localhost:9000"
	@echo "  Postgres          localhost:5432"
	@echo "  Grafana           http://localhost:3000  admin/admin"
	@echo ""

.PHONY: ingest
ingest: ## Load deterministic demo data
	docker compose --profile jobs up --build demo-ingest

.PHONY: real-ingest
real-ingest: ## Load a small real ERC-20 transfer batch through Etherscan
	docker compose --profile jobs up --build real-ingest

.PHONY: discover
discover: ## Rank wallet candidates from raw events and refresh the watchlist
	docker compose --profile jobs up --build wallet-discovery

.PHONY: price-ingest
price-ingest: ## Refresh hourly token prices for recently seen tokens
	docker compose --profile jobs up --build price-ingest

.PHONY: graph
graph: ## Recompute wallet similarity graph from wallet-token edges
	docker compose --profile jobs up --build graph-similarity

.PHONY: token-paths
token-paths: ## Recompute token transition graph, maximum spanning tree and top routes
	docker compose --profile jobs up --build token-paths

.PHONY: demo-flow
demo-flow: ## Prepare a fresh local demo dataset and all derived layers
	$(MAKE) reset-demo
	docker compose exec -T postgres psql -U student -d wallet_meta -c "DELETE FROM ingest_checkpoints WHERE source_name LIKE 'etherscan_%';"
	$(MAKE) real-ingest
	$(MAKE) discover
	$(MAKE) price-ingest
	$(MAKE) graph
	$(MAKE) token-paths

.PHONY: reset-demo
reset-demo: ## Truncate demo ClickHouse tables and load demo data again
	docker compose exec -T clickhouse clickhouse-client --user student --password student --multiquery --query "TRUNCATE TABLE raw.wallet_watchlist; TRUNCATE TABLE raw.token_prices_hourly; TRUNCATE TABLE raw.dex_transactions; TRUNCATE TABLE raw.ingest_runs; TRUNCATE TABLE mart.wallet_token_balances; TRUNCATE TABLE mart.wallet_daily_activity; TRUNCATE TABLE mart.token_smart_money_flow_5m; TRUNCATE TABLE mart.first_wallet_buys; TRUNCATE TABLE mart.wallet_ratings_latest; TRUNCATE TABLE graph.wallet_token_edges; TRUNCATE TABLE graph.wallet_similarity_edges; TRUNCATE TABLE graph.token_transition_edges; TRUNCATE TABLE graph.token_spanning_tree_edges; TRUNCATE TABLE graph.token_route_recommendations;"
	docker compose --profile jobs up --build demo-ingest

.PHONY: ps
ps: ## Show container status
	docker compose ps

.PHONY: logs
logs: ## Follow all service logs
	docker compose logs -f --tail=100

.PHONY: logs-ch
logs-ch: ## Follow ClickHouse logs
	docker compose logs -f --tail=100 clickhouse

.PHONY: ch
ch: ## Open clickhouse-client in the ClickHouse container
	docker compose exec clickhouse clickhouse-client --user student --password student

.PHONY: psql
psql: ## Open psql in the Postgres container
	docker compose exec postgres psql -U student -d wallet_meta

.PHONY: apply-ch
apply-ch: ## Apply ClickHouse init SQL files to the running container
	@for f in clickhouse/init/*.sql; do \
		echo "-> $$f"; \
		docker compose exec -T clickhouse clickhouse-client --user student --password student --multiquery < $$f || exit 1; \
	done

.PHONY: apply-pg
apply-pg: ## Apply Postgres init SQL files to the running container
	@for f in postgres/init/*.sql; do \
		echo "-> $$f"; \
		docker compose exec -T postgres psql -U student -d wallet_meta -f - < $$f || exit 1; \
	done

.PHONY: stop
stop: ## Stop containers while keeping volumes
	docker compose stop

.PHONY: down
down: ## Remove containers while keeping volumes
	docker compose down

.PHONY: help
help: ## Show this help
	@echo ""
	@echo "ClickHouse Smart Wallet Profiler"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""
