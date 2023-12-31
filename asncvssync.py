from dataclasses import dataclass, field
import json
import random
import time  # Added for sleep
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise3.purchases"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(self.__dict__)


def serialize_purchase(purchase):
    """Serializes the Purchase object to JSON"""
    return json.dumps(purchase.__dict__)


def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    # Write a synchronous production loop.
    while True:
        # Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        # sending it to Kafka!
        purchase = Purchase()
        serialized_purchase = serialize_purchase(purchase)

        # Produce the serialized purchase to Kafka
        p.produce(topic_name, value=serialized_purchase)

        # Wait for any outstanding messages to be delivered and delivery reports received
        p.flush()

        # Adding a short sleep to control the speed of production
        time.sleep(1)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    create_topic(client)

    try:
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


def create_topic(client):
    """Creates the topic with the given topic name"""
    # Removed redeclaration of client parameter
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print(f"Failed to create topic {TOPIC_NAME}: {e}")


if __name__ == "__main__":
    main()
