from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder \
    .appName("GearPositionStreamingApp") \
    .getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: read the gear-position kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
gearPositionDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gear-position") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
gearPositionDF = gearPositionDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# TO-DO: create a temporary streaming view called "GearPosition" based on the streaming dataframe
gearPositionDF.createOrReplaceTempView("GearPosition")

# TO-DO: query the temporary view "GearPosition" using spark.sql
gearPositionQueryDF = spark.sql("SELECT * FROM GearPosition")

# Write the dataframe from the last query to a kafka broker at localhost:9092, with a topic called gear-position-updates
query = gearPositionQueryDF \
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "gear-position-updates") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start()

query.awaitTermination()