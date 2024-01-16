from pyspark.sql import SparkSession

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder \
    .appName("ATMVisitsStreamingApp") \
    .getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
atmVisitsDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "atm-visits") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
atmVisitsDF = atmVisitsDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# TO-DO: create a temporary streaming view called "ATMVisits" based on the streaming dataframe
atmVisitsDF.createOrReplaceTempView("ATMVisits")

# TO-DO: query the temporary view with spark.sql, with this query: "select * from ATMVisits"
atmVisitsQueryDF = spark.sql("SELECT * FROM ATMVisits")

# TO-DO: write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker localhost:9092, and configure it to retrieve the earliest messages
query = atmVisitsQueryDF \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "atm-visit-updates") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start()

query.awaitTermination()