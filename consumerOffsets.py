import asyncio
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # TODO: Set the auto offset reset to earliest
    # See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest",  # Set auto offset reset to earliest
        }
    )

    # Subscribe to the topic
    c.subscribe([topic_name])

    while True:
        # Poll for messages
        message = c.poll(1.0)

        if message is None:
            print("No message received by consumer")
        elif message.error() is not None:
            print(f"Error from consumer: {message.error()}")
        else:
            print(f"Consumed message {message.key()}: {message.value()}")
