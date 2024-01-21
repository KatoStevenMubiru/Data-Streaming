from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

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

# TO-DO: create a spark application object
spark = SparkSession.builder.appName("RedisKafkaStreamToConsole").getOrCreate()

# TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
rawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
rawStreamingDF = rawStreamingDF.selectExpr("CAST(value AS STRING)")

# TO-DO: parse the single column "value" with a json object in it
parsedStreamingDF = rawStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(col('value.*'))

# TO-DO: storing them in a temporary view called RedisSortedSet
parsedStreamingDF.withColumn("zSetEntries", expr("zSetEntries[0].element")) \
    .createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view
encodedCustomerDF = spark.sql("""
SELECT zSetEntries AS encodedCustomer
FROM RedisSortedSet
""")

# TO-DO: base64 decode the JSON from Redis
decodedCustomerDF = encodedCustomerDF.withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomerDF.withColumn("customer", from_json("customer", customerSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

# TO-DO: select only the fields we want, where they are not null
emailAndBirthDayStreamingDF = spark.sql("""
SELECT email, birthDay
FROM CustomerRecords
WHERE email IS NOT NULL AND birthDay IS NOT NULL
""")

# TO-DO: split the birth year as a separate field from the birthday
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select("email", "birthYear")

# TO-DO: sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
query = emailAndBirthYearStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct