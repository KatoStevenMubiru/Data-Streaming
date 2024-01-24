#!/bin/bash

# Define the path to the spark-submit executable
# Replace this with the actual path if necessary
SPARK_SUBMIT="/path/to/spark/bin/spark-submit"

# Define the path to your python script
PYTHON_SCRIPT="/home/workspace/gear-position.py"

# Submit the application to the Spark cluster
$SPARK_SUBMIT --master [Spark URI] $PYTHON_SCRIPT