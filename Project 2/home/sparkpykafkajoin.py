from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis
redisMessageSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("Score", StringType())
        ])
    )),
])

# TO-DO: create a StructType for the Customer JSON that comes from Redis
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
])

# TO-DO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", DoubleType()),
    StructField("riskDate", StringType()),
])

#TO-DO: create a spark application object
spark = SparkSession.builder.appName("KafkaJoin").getOrCreate()

#TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("CAST(value AS STRING)")

# TO-DO: parse the single column "value" with a json object in it
redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view
encodedCustomerDF = spark.sql("""
SELECT zSetEntries[0].element as encodedCustomer
FROM RedisSortedSet
""")

# TO-DO: base64 decode the JSON from Redis
decodedCustomerDF = encodedCustomerDF.withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomerDF.withColumn("customer", from_json("customer", customerSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want
emailAndBirthDayStreamingDF = spark.sql("""
SELECT email, birthDay
FROM CustomerRecords
WHERE email IS NOT NULL AND birthDay IS NOT NULL
""")

# TO-DO: Split the birth year as a separate field from the birthday
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select("email", "birthYear")

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
stediEventsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr("CAST(value AS STRING)")

# TO-DO: parse the JSON from the single column "value" with a json object in it
stediEventsStreamingDF.withColumn("value", from_json("value", stediEventsSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view
customerRiskStreamingDF = spark.sql("""
SELECT customer, score
FROM CustomerRisk
""")

# TO-DO: join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
joinStreamingDF = emailAndBirthYearStreamingDF.join(
    customerRiskStreamingDF,
    expr("""
    email = customer
    """)
)

# TO-DO: sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
joinStreamingDF.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stedi-risk-score-birthdate") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start() \
    .awaitTermination()

# Replace "/path/to/checkpoint/dir" with an actual directory path where Spark can save checkpoints.