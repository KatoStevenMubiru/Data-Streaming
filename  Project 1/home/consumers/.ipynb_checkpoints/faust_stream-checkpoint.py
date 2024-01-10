"""Defines trends calculations for stations"""
import logging
import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record, serializer='json'):
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
class TransformedStation(faust.Record, serializer='json'):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations.table.v1", value_type=Station)

# Define the output Kafka Topic
out_topic = app.topic("transformed-stations", value_type=TransformedStation, partitions=1)

# Define a Faust Table
table = app.Table(
    "transformed-stations-table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

# Using Faust, transform input `Station` records into `TransformedStation` records.
@app.agent(topic)
async def stream(stations):
    async for station in stations.group_by(Station.station_id):
        line_co = None
        if station.red:
            line_co = "red"
        elif station.blue:
            line_co = "blue"
        else:
            line_co = "green"
        
        # Update the Faust table with the transformed station
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_co
        )

if __name__ == "__main__":
    app.main()