# Kafka Market Data Pipeline

A small real-time data pipeline: live crypto trades from Coinbase's public websocket are streamed into Kafka, aggregated into 1-minute OHLCV candles, and stored in Postgres. Everything runs with Docker Compose, with tests and CI on GitHub Actions.

## Architecture

```
Coinbase websocket -> Producer -> Kafka topic "trades" -> Processor -> Postgres (candles)
                                          |
                                          +-> "trades.dlq" (dead-letter topic for bad messages)
```

- **Producer** (`producer/`): subscribes to trade ("match") events for BTC-USD, ETH-USD and SOL-USD and publishes JSON messages keyed by symbol. Reconnects with exponential backoff.
- **Kafka** (single broker, KRaft mode, no ZooKeeper): topic `trades` with 3 partitions.
- **Processor** (`processor/`): a consumer group (`candle-builder`) that builds 1-minute candles in memory and upserts closed windows into Postgres.
- **Postgres**: table `candles` with primary key `(symbol, window_start)`.

## Run it

Requires Docker and Docker Compose.

```bash
echo "POSTGRES_PASSWORD=choose-a-password" > .env
docker compose up -d --build

# create the topics (first run only)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic trades --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic trades.dlq --partitions 1 --replication-factor 1

# after a minute or two, look at the candles
docker compose exec postgres psql -U app -d market \
  -c "SELECT symbol, window_start, open, high, low, close, volume, trade_count FROM candles ORDER BY window_start DESC LIMIT 10;"
```

Run the tests locally with `pip install pytest ruff && pytest`.

## CI

GitHub Actions (`.github/workflows/ci.yml`) lints with ruff, runs the unit tests, and builds both Docker images on every push and pull request.

## Design decisions

- **Message key = symbol.** Kafka guarantees ordering only within a partition, and the same key always maps to the same partition, so each symbol's trades are processed in order.
- **At-least-once delivery.** Auto-commit is off. Offsets are committed only after candles are written to Postgres, so a crash causes re-reads, never data loss.
- **Idempotent writes.** The upsert on `(symbol, window_start)` makes re-processing harmless, which gives effectively-once results.
- **Dead-letter topic.** Messages that fail parsing are sent to `trades.dlq` instead of crashing the consumer.
- **Idempotent producer** (`enable.idempotence`) avoids duplicates from producer retries.
- **Separate internal/external Kafka listeners**, so containers use `kafka:9092` and tools on the host use `localhost:9094`.
- **Aggregation logic is separated** (`aggregate.py`) so it can be unit tested without Kafka.

## Known limitations and next steps

- Open (in-progress) windows are held in memory. If the processor crashes mid-window, offsets for that partial window may already be committed, so one candle can end up incomplete. A stateful stream processor (Kafka Streams or Flink) or committing only offsets older than the oldest open window would fix this.
- Late or out-of-order trades beyond the 5-second grace period are not handled.
- Single broker with replication factor 1. Production would use 3+ brokers with replication factor 3, or a managed service like AWS MSK.
- No schema management. Next step: Avro with Schema Registry.
- Not deployed to the cloud yet. Next step: an EC2 deploy job in GitHub Actions over SSH.
- No monitoring beyond consumer-lag checks. Next step: Grafana on top of Postgres.

## Data

Public Coinbase Exchange websocket feed. No API key needed.
