"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

KAFKA_BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

logger = logging.getLogger(__name__)

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER_URL,
            "group.id": topic_name_pattern,
            "default.topic.config": {"auto.offset.reset": "earliest" if offset_earliest else "latest"}
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING
                # Logging added to indicate partition offset assignment
                logger.info(f"Partition {partition.partition} set to start from beginning")

        consumer.assign(partitions)
        logger.info("partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except SerializerError as e:
            logger.error(f"Message deserialization failed for {self.topic_name_pattern}: {e}")
            return 0
        except Exception as e:
            logger.error(f"Cannot poll message from {self.topic_name_pattern}: {e}")
            return 0
        
        if message is None:
            return 0
        elif message.error() is not None:
            logger.error(f"Error from consumer {message.error()}")
            return 0
        else:
            self.message_handler(message)
            # Logging added to indicate successful message processing
            logger.info(f"Consumer Message Key: {message.key()}, Value: {message.value()}")
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        # TODO: Cleanup the kafka consumer
        self.consumer.close()