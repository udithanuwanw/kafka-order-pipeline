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
    producer.flush(10)


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
