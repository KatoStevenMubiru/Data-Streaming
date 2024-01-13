"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    # The connector configuration has been completed as per the directions above.
    # The poll interval is set to 60000ms (1 minute) to moderate the load on the database and Kafka.
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "batch.max.rows": "500",
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "cta_admin",
            "connection.password": "chicago",
            "table.whitelist": "stations",
            "mode": "incrementing",
            "incrementing.column.name": "stop_id",
            "topic.prefix": "org.chicago.cta.",
            "poll.interval.ms": "60000",
        }
    }

    # Create the connector using the configured settings
    logging.debug("creating a new Kafka Connect connector...")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except:
        logging.error(f"Failed to create connector: {resp.status_code}, {resp.text}")

if __name__ == "__main__":
    configure_connector()