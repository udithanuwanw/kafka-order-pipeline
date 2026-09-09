# Kafka Order Pipeline — Design Spec

Date: 2026-09-09

## Goal

Build a Kafka-based producer/consumer system for "order" messages that
satisfies the course assignment (Chapter 3): Avro serialization,
real-time aggregation (running average of price), retry logic for
transient failures, and a Dead Letter Queue (DLQ) for permanently
failed messages. The system must be demoable live and tracked in a
Git repository.

## Stack

- **Language:** Python 3.
- **Kafka client:** `confluent-kafka` (includes Avro serializer
  support via Confluent Schema Registry).
- **Local infra:** Docker Compose running a single-node Kafka broker
  (KRaft mode, no Zookeeper), Confluent Schema Registry, and Kafka UI
  (for visually inspecting topics and the DLQ during the live demo).

## Schema

`schemas/order.avsc` — exactly the fields specified in the assignment:

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

Both the main topic (`orders`) and the DLQ topic (`orders-dlq`) use
this same schema — no second schema is needed. DLQ metadata (why a
message failed, how many retries were attempted) travels as Kafka
message headers rather than as extra Avro fields, keeping the schema
identical to the assignment's spec.

## Components

### `producer.py`

- Loops, generating one order per iteration (configurable delay).
- `orderId`: sequential integer as string (e.g. "1001", "1002", ...).
- `product`: random choice from a small fixed list (`Item1`..`Item5`).
- `price`: random float in a reasonable range (e.g. 5.00–500.00).
- With a small probability (e.g. 10%), intentionally emits an invalid
  order (negative price, or empty product) so the DLQ path is visible
  during the live demo without needing a separate fault-injection
  tool.
- Serializes with `AvroSerializer` against the schema, publishes to
  `orders`.

### `consumer.py`

Single consumer group reading from `orders`. For each message:

1. **Validation (permanent failure path).** If `orderId` or `product`
   is empty, or `price` is negative, the message is invalid. This is
   not retried — it is immediately republished to `orders-dlq` with
   header `error_reason=validation_error`.
2. **Simulated transient failure (retry path).** For valid messages,
   a `simulate_downstream_call()` function randomly raises a
   `TransientError` with some probability (e.g. 20%) representing a
   flaky downstream dependency. On failure, the consumer retries the
   same message up to `MAX_RETRIES` (e.g. 3) with exponential backoff
   (e.g. 0.5s, 1s, 2s). If it still fails after the last retry, the
   message is republished to `orders-dlq` with header
   `error_reason=max_retries_exceeded` and `retry_count` set.
3. **Success → aggregation.** On success (first try or after a retry
   succeeds), the consumer updates an in-memory running count and
   running average of `price` and prints
   `[AGG] count=<n> avg=<avg>` to stdout.

Offsets are committed per message after it's been fully handled
(success, or safely dead-lettered) so a crash doesn't silently drop a
message that was mid-retry.

### DLQ

`orders-dlq` is a plain Kafka topic, same Avro schema as `orders`.
Nothing consumes it automatically as part of this assignment; the
live demo shows failed messages landing there via Kafka UI. A small
`dlq_reader.py` (optional helper) can print DLQ contents plus headers
for the demo.

## Failure model rationale

Two independent, clearly distinguishable failure paths were chosen
specifically so the demo can show both without ambiguity:
- Validation failures are deterministic and reproducible (the
  producer seeds a few every run).
- Transient failures are probabilistic per-message and exercise the
  retry/backoff code path independently of validation.

## Out of scope

- Exactly-once processing guarantees.
- A second Avro schema for DLQ envelopes (headers are sufficient here).
- Persisting the running average across restarts.
- Automated tests beyond what's easy to demo manually — this is a
  demo-oriented class assignment, not a production system.

## Repo layout

```
takehome/
├── docker-compose.yml
├── schemas/order.avsc
├── producer.py
├── consumer.py
├── dlq_reader.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Demo script (in README)

1. `docker compose up -d`
2. Create topics (or rely on auto-create) `orders`, `orders-dlq`.
3. Run `consumer.py` in one terminal.
4. Run `producer.py` in another terminal.
5. Watch running average print in the consumer terminal.
6. Open Kafka UI, show messages landing in `orders-dlq` with headers
   explaining why.
