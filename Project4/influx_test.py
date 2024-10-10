# Import necessary libraries
from confluent_kafka import Consumer  # Library for interacting with Kafka messaging system
from influxdb_client import InfluxDBClient, Point  # Library for connecting to InfluxDB and creating data points
from influxdb_client.client.write_api import SYNCHRONOUS  # For synchronous writing to InfluxDB
import json  # Library for handling JSON data

# Set up the Kafka consumer instance with necessary configurations
kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Address of the Kafka broker
    'group.id': 'my-group9',                 # Consumer group ID
    'auto.offset.reset': 'latest'             # Start consuming from the latest message
}
kafka_consumer = Consumer(kafka_consumer_config)  # Create a Kafka consumer instance
kafka_consumer.subscribe(['output24'])  # Subscribe the consumer to the specified topic 

# Configuration for the InfluxDB bucket
bucket = "spark_test2"  # Name of the InfluxDB bucket
org = "influx_work_space"  # Name of the organization in InfluxDB
token = "f-kRjrcA4vUeewZuY8EmYheNdawJ9t-kTnpyt7qEV_qOpI0iRXF7Bw5OSyJX_Nuf-iiAiz-DH8b5IP87GliLsw=="  # Access token for InfluxDB

# Set up the InfluxDB client and write API
influxdb_client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)  # Create an InfluxDB client instance
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)  # Create a synchronous write API instance

# Consume data from Kafka and write it to InfluxDB
while True:
    msg = kafka_consumer.poll(4.0)  # Poll for new messages, waiting for up to 4 seconds
    if msg is None:
        print("There is no data to be consumed, waiting ...")  # Notify if no messages are available
        break  # Exit the loop if no messages are found

    else:
        # Decode the received Kafka message and convert it from bytes to a JSON object
        data = json.loads(msg.value().decode('utf-8'))  
        print(data)  # Print the received data for debugging purposes

        # Convert the received JSON data into a format compatible with InfluxDB
        point = Point("water_samples") \
            .field("ph", data["ph"]) \
            .field("Hardness", data["Hardness"]) \
            .field("Solids", data["Solids"]) \
            .field("Chloramines", data["Chloramines"]) \
            .field("Sulfate", data["Sulfate"]) \
            .field("Conductivity", data["Conductivity"]) \
            .field("Organic_carbon", data["Organic_carbon"]) \
            .field("Trihalomethanes", data["Trihalomethanes"]) \
            .field("Turbidity", data["Turbidity"]) \
            .field("Potability", data["Potability"])
        
        # Write the constructed point to the specified InfluxDB bucket
        write_api.write(bucket=bucket, record=point)  # Write the generated point to the database
