"""Contains functionality related to Weather"""
import json
import logging
import requests

logger = logging.getLogger(__name__)

# URL for the Kafka REST Proxy
REST_PROXY_URL = "http://localhost:8082/topics/weather"

# Define the Weather model
class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        # TODO: Process incoming weather messages. Set the temperature and status.
        try:
            weather_data = json.loads(message.value())
            self.temperature = weather_data.get('temperature', 70.0)
            self.status = weather_data.get('status', 'sunny')
            logger.info("Weather data processed successfully")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {e}")
        except KeyError as e:
            logger.error(f"Missing key in weather data: {e}")
        except Exception as e:
            logger.error(f"Error processing weather message: {e}")

    # TODO: Complete the code to emit weather events to Kafka REST Proxy
    def run(self):
        """Sends the weather data to Kafka REST Proxy"""
        # Headers for sending data to Kafka REST Proxy with the appropriate content type
        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        # Define the key schema for the weather data
        key_schema = {"type": "string"}
        # Define the value schema for the weather data, which matches the schema in weather_value.json
        value_schema = {
            "namespace": "com.udacity",
            "name": "weather.value",
            "type": "record",
            "fields": [
                {"name": "temperature", "type": "float"},
                {"name": "status", "type": "string"}
            ]
        }
        # Create the data payload with the key, value schema, and records
        data = {
            "key_schema": json.dumps(key_schema),
            "value_schema": json.dumps(value_schema),
            "records": [
                {
                    "key": "weather",
                    "value": {
                        "temperature": self.temperature,
                        "status": self.status
                    }
                }
            ]
        }
        # Send the data to the Kafka REST Proxy
        response = requests.post(REST_PROXY_URL, headers=headers, data=json.dumps(data))
        try:
            response.raise_for_status()
            logger.info("Sent weather data to Kafka REST Proxy")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to send weather data to Kafka REST Proxy: {e}")