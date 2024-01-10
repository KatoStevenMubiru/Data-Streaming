"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("postgres-stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("transformed-stations", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
"transformed-stations-table", 
default=TransformedStation,
partitions=1,
changelog_topic=out_topic
)
#(transformed-stations-table-changelog) has only one partition, which is a recommended practice for Faust applications.



#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
@app.agent(topic)
async def transform(stations):
    # This function transforms incoming `Station` records into `TransformedStation` records
    async for station in stations:
        # Example: Filtering stations based on a specific condition (replace with your criteria)
        if station.order == 1:
            # Determine the line color based on station attributes
            line = None
            if station.red:
                line = 'red'
            elif station.blue:
                line = 'blue'
            else:
                line = 'green'

            # Store the transformed station in the Faust table
            table[station.station_id] = TransformedStation(
                station_id=station.station_id,
                station_name=station.station_name,
                order=station.order,
                line=line
            )

if __name__ == "__main__":
    app.main()
