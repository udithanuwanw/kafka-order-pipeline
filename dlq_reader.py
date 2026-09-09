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
