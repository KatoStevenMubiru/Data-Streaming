"""Contains functionality related to Weather"""
import logging
import j

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        #
        #
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

