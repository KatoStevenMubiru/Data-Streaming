# Project: Real-Time Train Status with Apache Kafka

## Overview

This project involves building a real-time event streaming pipeline using Apache Kafka and related technologies. Leveraging public data from the Chicago Transit Authority (CTA), the pipeline simulates and displays the status of train lines in real-time. Upon completion, users can monitor a website to observe trains moving from station to station.

![Final Project Output](image-url-here) *Replace with actual image URL*

## Prerequisites

To set up the project locally, ensure you have:

- Docker
- Python 3.7
- At least 16GB of RAM and a 4-core CPU

Alternatively, the project can be completed in the provided Project Workspace.

## Additional Resources

- [Confluent Python Client Documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/)
- [Confluent Python Client Usage and Examples](https://github.com/confluentinc/confluent-kafka-python#usage)
- <Citation title="REST Proxy API Reference" href="https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)" />

## Directory Layout

The project is structured into `producers` and `consumers` directories. Files marked with an asterisk (*) are to be modified by the student.

## Running and Testing

### Running in the Classroom Project Workspace

The Project Workspace is preconfigured with all necessary services. No additional setup is required.

### Running on Your Computer

Ensure Docker-Compose is allocated at least 4GB of RAM. Start the Kafka ecosystem using:

`bash
docker-compose up

## Running the Simulation
Producer:
cd producers
python simulation.py

Faust Stream Processing Application:
cd consumers
faust -A faust_stream worker -l info

KSQL Creation Script:
cd consumers
python ksql.py

Consumer:
cd consumers
python server.py


## Author

Kato Steven Mubiru

## NOTE : I built my project through the Udacity workspaces , with preinstalled configurations.