# Kafka Order Pipeline

A Kafka-based producer/consumer system for Avro-serialized order messages,
with real-time running-average aggregation, retry logic for simulated
transient failures, and a Dead Letter Queue (DLQ) for permanently failed
messages.



## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.9+

## Setup

```bash
pip install -r requirements.txt
docker compose up -d
```

Wait ~30 seconds, then confirm everything is healthy:

```bash
docker compose ps
```

`kafka` should show `healthy`; `schema-registry` and `kafka-ui` should show `running`.

## Running the demo

Open three terminals.

**Terminal 1 — consumer** (start first so no messages are missed):

```bash
python consumer.py
```

**Terminal 2 — producer:**

```bash
python producer.py
```

Watch Terminal 1: you'll see a running average print after every
successfully processed order (`[AGG] count=... avg=...`), occasional
`Validation failed (negative_price) ... -> DLQ` lines (producer intentionally
emits ~10% invalid orders), and occasional `Max retries exceeded ... -> DLQ`
lines (consumer simulates a ~50% transient failure rate per message, retried
3 times with backoff before giving up).

**Terminal 3 — DLQ reader** (run any time after some DLQ messages exist):

```bash
python dlq_reader.py
```

You can also inspect topics, messages, and the registered Avro schema
visually at [http://localhost:8080](http://localhost:8080) (Kafka UI) and
[http://localhost:8081/subjects](http://localhost:8081/subjects) (Schema
Registry).

Stop the producer and consumer with Ctrl+C when done.

## Shutting down

```bash
docker compose down
```

## Project layout

- `docker-compose.yml` — Kafka (KRaft), Schema Registry, Kafka UI.
- `schemas/order.avsc` — Avro schema for order messages.
- `constants.py` — shared topic names and DLQ error-reason constants.
- `order_utils.py` — order generation and validation (unit tested).
- `aggregator.py` — running-average tracker (unit tested).
- `retry.py` — retry-with-backoff wrapper (unit tested).
- `producer.py` — publishes orders to Kafka.
- `consumer.py` — validates, retries, aggregates, and dead-letters orders.
- `dlq_reader.py` — prints DLQ contents for the live demo.

## Running the unit tests

```bash
pytest -v
```
