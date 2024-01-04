import asyncio
from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker

faker = Faker()
REST_PROXY_URL = "http://localhost:8082"
TOPIC_NAME = "lesson4.solution7.click_events"
CONSUMER_GROUP = f"solution7-consumer-group-{random.randint(0,10000)}"

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

async def consume():
    """Consumes from REST Proxy"""

    # Define a consumer name
    consumer_name = f"consumer-{random.randint(0,10000)}"

    # Define the appropriate headers
    # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
        "Content-Type": "application/vnd.kafka.avro.v2+json"
    }

    # Define the consumer group creation payload, use avro
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)
    data = {
        "name": consumer_name,
        "format": "avro",
        "auto.offset.reset": "earliest"
    }

    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers,
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(
            f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return

    print("REST Proxy consumer group created")

    resp_data = resp.json()

    # Create the subscription payload
    # See: https://docs.confluent.io/current/kafka-rest/api.html#consumers
    data = {
        "topics": [TOPIC_NAME],
    }

    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}/instances/{consumer_name}/subscription",
        data=json.dumps(data),
        headers=headers,
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(
            f"Failed to subscribe consumer to topic: {json.dumps(resp.json(), indent=2)}"
        )
        return

    print(f"Consumer subscribed to topic {TOPIC_NAME}")

    while True:
        # TODO: Fetch data from the REST Proxy
        #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
        resp = requests.get(
            f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}/instances/{consumer_name}/records",
            headers=headers,
        )

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            print(f"Failed to fetch records: {json.dumps(resp.json(), indent=2)}")
            break

        records = resp.json()

        for record in records:
            value = record["value"]
            print(f"Received record: {value}")

        await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(consume())
