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
