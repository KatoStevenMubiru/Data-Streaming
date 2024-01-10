from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
text_file_path = '/home/workspace/Test.txt'

# TO-DO: create a Spark session
spark = SparkSession.builder \
    .appName("LetterCount") \
    .getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file
text_data = spark.read.text(text_file_path)

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
count_a = text_data.filter(text_data.value.contains('a')).count()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
count_b = text_data.filter(text_data.value.contains('b')).count()

# TO-DO: print the count for letter 'd' and letter 's'
count_d = text_data.filter(text_data.value.contains('d')).count()
count_s = text_data.filter(text_data.value.contains('s')).count()
print(f"Count for letter 'd': {count_d}")
print(f"Count for letter 's': {count_s}")

# TO-DO: stop the spark application
spark.stop()