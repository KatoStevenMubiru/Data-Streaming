import asyncio
import json
import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "exercise2"

def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below for a JDBC source connector.
    #       You should whitelist the `clicks` table, use incrementing mode, and the
    #       incrementing column name should be id.
    #
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,  # Set the connector name
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # JDBC source connector class
                    "connection.url": "jdbc:mysql://your-mysql-host:3306/your-database",  # Set your MySQL connection URL
                    "connection.user": "your-username",  # Set your MySQL username
                    "connection.password": "your-password",  # Set your MySQL password
                    "mode": "incrementing",  # Use incrementing mode
                    "incrementing.column.name": "id",  # Set the incrementing column name to 'id'
                    "table.whitelist": "clicks",  # Whitelist the 'clicks' table
                    "tasks.max": 1,  # Set the maximum number of tasks
                    "topic.prefix": "jdbc-",  # Set a topic prefix if needed
                    # Add other necessary configuration options based on your setup
                },
            }
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    print("connector created successfully")

async def log():
    """Continually appends to the end of a file"""
    with open(f"/tmp/{CONNECTOR_NAME}.log", "a") as f:
        iteration = 0
        while True:
            f.write(f"log number {iteration}\n")
            f.flush()
            await asyncio.sleep(1.0)
            iteration += 1

async def log_task():
    """Runs the log task"""
    task = asyncio.create_task(log())
    configure_connector()
    await task

def run():
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    run()
