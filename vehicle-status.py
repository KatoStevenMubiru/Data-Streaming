from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
kafkaMessageSchema = StructType([
    StructField("truckNumber", StringType()),
    StructField("destination", StringType()),
    StructField("milesFromShop", IntegerType()),
    StructField("odometerReading", IntegerType())
])

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder \
    .appName("VehicleStatusStreamingApp") \
    .getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
vehicleStatusDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle-status") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
vehicleStatusDF = vehicleStatusDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe
vehicleStatusDF = vehicleStatusDF.withColumn("value", from_json("value", kafkaMessageSchema))

# TO-DO: create a temporary streaming view called "VehicleStatus"
vehicleStatusDF.createOrReplaceTempView("VehicleStatus")

# TO-DO: using spark.sql, select * from VehicleStatus
resultDF = spark.sql("SELECT value.* FROM VehicleStatus")

# TO-DO: write the stream to the console, and configure it to run indefinitely
query = resultDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()