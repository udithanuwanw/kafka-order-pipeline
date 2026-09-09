# Kafka Order Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Kafka producer/consumer pipeline for Avro-serialized order messages with real-time running-average aggregation, retry logic for transient failures, and a Dead Letter Queue for permanent failures, demoable live via Docker Compose.

**Architecture:** A single-node Kafka (KRaft) + Schema Registry + Kafka UI stack runs via Docker Compose. `producer.py` generates orders (occasionally invalid, to exercise the DLQ) and publishes Avro-encoded messages to topic `orders`. `consumer.py` validates each message (permanent failures → DLQ immediately), simulates a flaky downstream call with retry+backoff (exhausted retries → DLQ), and maintains a running average of price for successful messages. Small, independently unit-testable pure modules (`order_utils.py`, `aggregator.py`, `retry.py`) back the two scripts; the scripts themselves are verified by live demo since they need a running broker.

**Tech Stack:** Python 3, `confluent-kafka[avro]`, Confluent Schema Registry, Docker Compose, pytest.

**Spec:** [docs/superpowers/specs/2026-09-09-kafka-order-pipeline-design.md](../specs/2026-09-09-kafka-order-pipeline-design.md)

## Global Constraints

- Language: Python 3.
- Kafka client: `confluent-kafka[avro]` (via Confluent Schema Registry).
- Local infra: Docker Compose, single-node Kafka in KRaft mode (no Zookeeper), Confluent Schema Registry, Kafka UI.
- Topics: `orders` (main), `orders-dlq` (dead letter queue) — both use the same Avro schema.
- Schema fields (exact, from assignment): `orderId` (string), `product` (string), `price` (float).
- DLQ metadata (`error_reason`, `retry_count`) travels as Kafka message headers, not extra Avro fields.
- Retry: max 3 retries, exponential backoff starting at 0.5s (0.5, 1, 2).
- Simulated transient failure rate: ~20% per valid message (`TRANSIENT_FAILURE_RATE = 0.2`).
- Simulated invalid-order rate from producer: ~10% (`invalid_rate=0.1`), via negative price.
- No second Avro schema, no persistence of aggregation state across restarts, no automated integration tests — this is a demo-oriented class assignment.

---

### Task 1: Project scaffolding — Docker Compose stack, schema, shared constants

**Files:**
- Create: `docker-compose.yml`
- Create: `schemas/order.avsc`
- Create: `requirements.txt`
- Create: `.gitignore`
- Create: `constants.py`

**Interfaces:**
- Produces: `constants.py` exports `TOPIC_ORDERS = "orders"`, `TOPIC_DLQ = "orders-dlq"`, `ERROR_VALIDATION = "validation_error"`, `ERROR_MAX_RETRIES = "max_retries_exceeded"`. Later tasks import these instead of hardcoding strings.

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
version: "3.8"

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 10s
      timeout: 10s
      retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    container_name: schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
```

- [ ] **Step 2: Create `schemas/order.avsc`**

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.assignment.orders",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

- [ ] **Step 3: Create `requirements.txt`**

```
confluent-kafka[avro]==2.5.3
pytest==8.3.2
```

- [ ] **Step 4: Create `.gitignore`**

```
__pycache__/
*.pyc
.venv/
venv/
.pytest_cache/
```

- [ ] **Step 5: Create `constants.py`**

```python
TOPIC_ORDERS = "orders"
TOPIC_DLQ = "orders-dlq"
ERROR_VALIDATION = "validation_error"
ERROR_MAX_RETRIES = "max_retries_exceeded"
```

- [ ] **Step 6: Install dependencies and validate the compose file**

Run: `pip install -r requirements.txt`
Expected: installs without error.

Run: `docker compose config`
Expected: prints the resolved compose config with no errors (validates YAML + interpolation).

- [ ] **Step 7: Bring the stack up and verify health**

Run: `docker compose up -d`
Then: `docker compose ps`
Expected: `kafka`, `schema-registry`, `kafka-ui` all show state `running` (kafka may show `healthy` once its healthcheck passes — wait ~30s and re-run `docker compose ps` if not).

Run: `docker compose down`
Expected: stack stops cleanly (leave it down until Task 5, when you'll need it running again).

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml schemas/order.avsc requirements.txt .gitignore constants.py
git commit -m "chore: scaffold Kafka/Schema Registry stack, Avro schema, shared constants"
```

---

### Task 2: Order generation & validation (`order_utils.py`)

**Files:**
- Create: `order_utils.py`
- Test: `tests/test_order_utils.py`

**Interfaces:**
- Produces: `generate_order(seq: int, invalid_rate: float = 0.1) -> dict` (keys `orderId`, `product`, `price`); `validate_order(order: dict) -> str | None` (returns `None` if valid, else `"missing_order_id"` / `"missing_product"` / `"negative_price"`). Task 5 (producer) uses `generate_order`; Task 6 (consumer) uses `validate_order`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_order_utils.py`:

```python
from order_utils import generate_order, validate_order


def test_generate_order_has_required_fields():
    order = generate_order(seq=0, invalid_rate=0.0)
    assert set(order.keys()) == {"orderId", "product", "price"}


def test_generate_order_id_uses_sequence():
    order = generate_order(seq=5, invalid_rate=0.0)
    assert order["orderId"] == "1005"


def test_generate_order_invalid_rate_zero_always_valid():
    for seq in range(50):
        order = generate_order(seq=seq, invalid_rate=0.0)
        assert order["price"] >= 0


def test_generate_order_invalid_rate_one_always_invalid():
    for seq in range(50):
        order = generate_order(seq=seq, invalid_rate=1.0)
        assert order["price"] < 0


def test_validate_order_valid():
    order = {"orderId": "1001", "product": "Item1", "price": 10.0}
    assert validate_order(order) is None


def test_validate_order_missing_order_id():
    order = {"orderId": "", "product": "Item1", "price": 10.0}
    assert validate_order(order) == "missing_order_id"


def test_validate_order_missing_product():
    order = {"orderId": "1001", "product": "", "price": 10.0}
    assert validate_order(order) == "missing_product"


def test_validate_order_negative_price():
    order = {"orderId": "1001", "product": "Item1", "price": -5.0}
    assert validate_order(order) == "negative_price"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_order_utils.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'order_utils'`

- [ ] **Step 3: Implement `order_utils.py`**

```python
import random
from typing import Optional

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def generate_order(seq: int, invalid_rate: float = 0.1) -> dict:
    order_id = str(1000 + seq)
    product = random.choice(PRODUCTS)
    price = round(random.uniform(5.0, 500.0), 2)
    if random.random() < invalid_rate:
        price = -abs(price)
    return {"orderId": order_id, "product": product, "price": price}


def validate_order(order: dict) -> Optional[str]:
    if not order.get("orderId"):
        return "missing_order_id"
    if not order.get("product"):
        return "missing_product"
    if order.get("price", 0) < 0:
        return "negative_price"
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_order_utils.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add order_utils.py tests/test_order_utils.py
git commit -m "feat: add order generation and validation"
```

---

### Task 3: Running average aggregator (`aggregator.py`)

**Files:**
- Create: `aggregator.py`
- Test: `tests/test_aggregator.py`

**Interfaces:**
- Produces: `RunningAverage` class with `.update(price: float) -> None`, `.count: int`, `.average: float` property. Task 6 (consumer) instantiates one `RunningAverage` and calls `.update()` on every successfully processed order.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_aggregator.py`:

```python
from aggregator import RunningAverage


def test_running_average_initial_zero():
    agg = RunningAverage()
    assert agg.count == 0
    assert agg.average == 0.0


def test_running_average_single_update():
    agg = RunningAverage()
    agg.update(10.0)
    assert agg.count == 1
    assert agg.average == 10.0


def test_running_average_multiple_updates():
    agg = RunningAverage()
    for price in (10.0, 20.0, 30.0):
        agg.update(price)
    assert agg.count == 3
    assert agg.average == 20.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_aggregator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aggregator'`

- [ ] **Step 3: Implement `aggregator.py`**

```python
class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price: float) -> None:
        self.count += 1
        self.total += price

    @property
    def average(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total / self.count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_aggregator.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add aggregator.py tests/test_aggregator.py
git commit -m "feat: add running-average aggregator"
```

---

### Task 4: Retry-with-backoff wrapper (`retry.py`)

**Files:**
- Create: `retry.py`
- Test: `tests/test_retry.py`

**Interfaces:**
- Produces: `TransientError(Exception)`; `process_with_retry(func: Callable[[], T], max_retries: int = 3, backoff_seconds: float = 0.5, sleep_fn: Callable[[float], None] = time.sleep) -> Tuple[bool, int, Optional[T]]` returning `(success, attempts_made, result)`. Task 6 (consumer) wraps `simulate_downstream_call` with this.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_retry.py`:

```python
from retry import process_with_retry, TransientError


def test_process_with_retry_succeeds_first_try():
    sleeps = []

    def func():
        return "ok"

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is True
    assert attempts == 1
    assert result == "ok"
    assert sleeps == []


def test_process_with_retry_succeeds_after_two_failures():
    sleeps = []
    call_count = {"n": 0}

    def func():
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise TransientError("flaky")
        return "ok"

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is True
    assert attempts == 3
    assert result == "ok"
    assert sleeps == [0.1, 0.2]


def test_process_with_retry_gives_up_after_max_retries():
    sleeps = []

    def func():
        raise TransientError("always fails")

    success, attempts, result = process_with_retry(
        func, max_retries=3, backoff_seconds=0.1, sleep_fn=sleeps.append
    )

    assert success is False
    assert attempts == 4
    assert result is None
    assert sleeps == [0.1, 0.2, 0.4]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_retry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'retry'`

- [ ] **Step 3: Implement `retry.py`**

```python
import time
from typing import Callable, Optional, Tuple, TypeVar

T = TypeVar("T")


class TransientError(Exception):
    """Raised to simulate a flaky downstream dependency."""


def process_with_retry(
    func: Callable[[], T],
    max_retries: int = 3,
    backoff_seconds: float = 0.5,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Tuple[bool, int, Optional[T]]:
    attempts = 0
    while True:
        attempts += 1
        try:
            result = func()
            return True, attempts, result
        except TransientError:
            if attempts > max_retries:
                return False, attempts, None
            sleep_fn(backoff_seconds * (2 ** (attempts - 1)))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_retry.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add retry.py tests/test_retry.py
git commit -m "feat: add retry-with-backoff wrapper"
```

---

### Task 5: Producer script (`producer.py`)

**Files:**
- Create: `producer.py`

**Interfaces:**
- Consumes: `generate_order(seq, invalid_rate)` from `order_utils.py`; `TOPIC_ORDERS` from `constants.py`.
- Produces: a running process that publishes Avro-encoded order messages to `orders` every second. Verified live (needs a running broker), not via pytest.

- [ ] **Step 1: Implement `producer.py`**

```python
import logging
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from constants import TOPIC_ORDERS
from order_utils import generate_order

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_PATH = "schemas/order.avsc"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        logger.info("Delivered to %s [partition %d]", msg.topic(), msg.partition())


def main():
    with open(SCHEMA_PATH, "r") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    seq = 0
    try:
        while True:
            order = generate_order(seq)
            seq += 1
            producer.produce(
                topic=TOPIC_ORDERS,
                key=order["orderId"],
                value=avro_serializer(
                    order, SerializationContext(TOPIC_ORDERS, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            logger.info("Produced order %s", order)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify live against the broker**

Run: `docker compose up -d` (if not already up), wait ~30s, then `docker compose ps` to confirm `kafka` is healthy.
Run: `python producer.py`
Expected: log lines `Produced order {...}` once per second, and `Delivered to orders [partition 0]` shortly after each one, with no errors. Stop with Ctrl+C — expect a clean `Shutting down producer` log and no traceback.

Run: open `http://localhost:8081/subjects` in a browser or `curl http://localhost:8081/subjects`
Expected: JSON list containing `"orders-value"`, confirming the Avro schema was registered.

- [ ] **Step 3: Commit**

```bash
git add producer.py
git commit -m "feat: add order producer"
```

---

### Task 6: Consumer script (`consumer.py`) — validation, retry, DLQ, aggregation

**Files:**
- Create: `consumer.py`

**Interfaces:**
- Consumes: `validate_order` from `order_utils.py`; `RunningAverage` from `aggregator.py`; `process_with_retry`, `TransientError` from `retry.py`; `TOPIC_ORDERS`, `TOPIC_DLQ`, `ERROR_VALIDATION`, `ERROR_MAX_RETRIES` from `constants.py`.
- Produces: a running process that reads `orders`, prints `[AGG] count=<n> avg=<avg>` per successful message, and republishes failed messages to `orders-dlq` with `error_reason`/`retry_count` headers. Verified live, not via pytest.

- [ ] **Step 1: Implement `consumer.py`**

```python
import logging
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from aggregator import RunningAverage
from constants import ERROR_MAX_RETRIES, TOPIC_DLQ, TOPIC_ORDERS
from order_utils import validate_order
from retry import TransientError, process_with_retry

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_PATH = "schemas/order.avsc"
GROUP_ID = "order-consumer-group"
MAX_RETRIES = 3
BACKOFF_SECONDS = 0.5
TRANSIENT_FAILURE_RATE = 0.2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")


def simulate_downstream_call(order: dict) -> dict:
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientError(f"downstream hiccup processing {order['orderId']}")
    return order


def send_to_dlq(producer, avro_serializer, order, reason, retry_count):
    headers = [
        ("error_reason", reason.encode("utf-8")),
        ("retry_count", str(retry_count).encode("utf-8")),
    ]
    producer.produce(
        topic=TOPIC_DLQ,
        key=order.get("orderId", ""),
        value=avro_serializer(order, SerializationContext(TOPIC_DLQ, MessageField.VALUE)),
        headers=headers,
    )
    producer.poll(0)


def main():
    with open(SCHEMA_PATH, "r") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ORDERS])

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    stats = RunningAverage()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            order = avro_deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )

            reason = validate_order(order)
            if reason is not None:
                logger.warning("Validation failed (%s) for %s -> DLQ", reason, order)
                send_to_dlq(dlq_producer, avro_serializer, order, reason, retry_count=0)
                consumer.commit(msg)
                continue

            success, attempts, _ = process_with_retry(
                lambda: simulate_downstream_call(order),
                max_retries=MAX_RETRIES,
                backoff_seconds=BACKOFF_SECONDS,
            )

            if success:
                stats.update(order["price"])
                logger.info(
                    "[AGG] count=%d avg=%.2f (order=%s, attempts=%d)",
                    stats.count, stats.average, order["orderId"], attempts,
                )
            else:
                logger.warning(
                    "Max retries exceeded for %s after %d attempts -> DLQ",
                    order["orderId"], attempts,
                )
                send_to_dlq(dlq_producer, avro_serializer, order, ERROR_MAX_RETRIES, attempts)

            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify live against the broker**

Ensure `docker compose up -d` is running and `python producer.py` is running in another terminal (from Task 5).
Run: `python consumer.py`
Expected:
- Lines like `[AGG] count=1 avg=123.45 (order=1000, attempts=1)` for successful orders, with `count` increasing by 1 each time and `avg` recalculating correctly.
- Occasional `Validation failed (negative_price) for {...} -> DLQ` lines (roughly 1 in 10 orders, since the producer's default `invalid_rate=0.1`).
- Occasional `Max retries exceeded for ... after 4 attempts -> DLQ` lines (roughly 1 in a few hundred orders at a 20% per-message failure rate over 4 attempts — if none appear in a short run, that's expected; the path is still exercised, just rarer).
- No unhandled tracebacks. Stop both scripts with Ctrl+C — expect clean `Shutting down consumer`/`Shutting down producer` logs.

- [ ] **Step 3: Commit**

```bash
git add consumer.py
git commit -m "feat: add order consumer with validation, retry, DLQ, and aggregation"
```

---

### Task 7: DLQ reader helper (`dlq_reader.py`)

**Files:**
- Create: `dlq_reader.py`

**Interfaces:**
- Consumes: `TOPIC_DLQ` from `constants.py`.
- Produces: a running process that prints every DLQ message plus its `error_reason`/`retry_count` headers, for use during the live demo. Verified live, not via pytest.

- [ ] **Step 1: Implement `dlq_reader.py`**

```python
import logging

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from constants import TOPIC_DLQ

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_PATH = "schemas/order.avsc"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dlq_reader")


def main():
    with open(SCHEMA_PATH, "r") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-reader-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC_DLQ])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            order = avro_deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
            headers = dict(msg.headers() or [])
            reason = headers.get("error_reason", b"").decode("utf-8")
            retry_count = headers.get("retry_count", b"").decode("utf-8")
            logger.info(
                "DLQ message: order=%s reason=%s retry_count=%s",
                order, reason, retry_count,
            )
    except KeyboardInterrupt:
        logger.info("Shutting down dlq_reader")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify live against the broker**

With `docker compose up -d`, `producer.py`, and `consumer.py` already running (or having run recently so some DLQ messages exist), run: `python dlq_reader.py`
Expected: one `DLQ message: order=... reason=validation_error retry_count=0` or `reason=max_retries_exceeded retry_count=4` line per message previously dead-lettered, then it idles waiting for new ones (Ctrl+C to stop).

- [ ] **Step 3: Commit**

```bash
git add dlq_reader.py
git commit -m "feat: add DLQ reader helper for live demo"
```

---

### Task 8: README with setup, run, and live demo instructions

**Files:**
- Create: `README.md`

**Interfaces:**
- None — this is documentation only, consumed by whoever runs the demo (you, or a grader).

- [ ] **Step 1: Write `README.md`**

```markdown
# Kafka Order Pipeline

A Kafka-based producer/consumer system for Avro-serialized order messages,
with real-time running-average aggregation, retry logic for simulated
transient failures, and a Dead Letter Queue (DLQ) for permanently failed
messages.

See [docs/superpowers/specs/2026-09-09-kafka-order-pipeline-design.md](docs/superpowers/specs/2026-09-09-kafka-order-pipeline-design.md)
for the full design.

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
lines (consumer simulates a ~20% transient failure rate per message, retried
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
```

- [ ] **Step 2: Verify the documented commands work end-to-end**

Follow the README from a clean state: `docker compose down` (if anything is running), then re-run every command block in order (`pip install`, `docker compose up -d`, `docker compose ps`, consumer, producer, dlq_reader).
Expected: everything behaves exactly as described in the README, with no undocumented manual steps needed.

Also run: `pytest -v`
Expected: PASS (13 passed: 7 from `test_order_utils.py`, 3 from `test_aggregator.py`, 3 from `test_retry.py`).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add setup, run, and live demo instructions"
```
