from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# TO-DO: create a Spark Session, and name the app something relevant
spark = SparkSession.builder \
    .appName("KafkaConsoleBankingApp") \
    .getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: read a stream from the kafka topic 'balance-updates', with the bootstrap server localhost:9092, reading from the earliest message
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "balance-updates") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the key and value columns as strings and select them using a select expression function
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# TO-DO: write the dataframe to the console, and keep running indefinitely
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()