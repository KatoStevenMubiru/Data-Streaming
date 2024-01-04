from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from faker import Faker

faker = Faker()
REST_PROXY_URL = "http://localhost:8082"
TOPIC_NAME = "KSM"  # Replace with your actual Kafka topic name

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

def produce():
    """Produces data using REST Proxy"""

    # Set the appropriate headers
    # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json"
    }

    # Define the JSON Payload to be sent to REST Proxy
    # To create data, use `asdict(ClickEvent())`
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    data = {
        "records": [
            {
                "value": asdict(ClickEvent())
            }
        ]
    }

    # What URL should be used?
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/{TOPIC_NAME}",
        data=json.dumps(data),
        headers=headers
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")


def main():
    """Runs the simulation against REST Proxy"""
    try:
        while True:
            produce()
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
