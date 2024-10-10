import json  # Import the json module for parsing JSON data
from confluent_kafka import Consumer  # Import the Kafka consumer from the confluent_kafka library
import avro.schema  # Import the avro schema module for working with Avro schemas
from avro.datafile import DataFileReader, DataFileWriter  # Import classes for reading and writing Avro data files
from avro.io import DatumReader, DatumWriter  # Import reader and writer classes for Avro data

# Defining the Avro schema as a string
schema_str = """
    {
        "namespace": "data.avro",
        "type": "record",
        "name": "water_sample",
        "fields": [
            {"name": "ph", "type": "string"},  # pH level of the water sample
            {"name": "Hardness", "type": "string"},  # Hardness of the water sample
            {"name": "Solids", "type": "string"},  # Total solids in the water sample
            {"name": "Chloramines", "type": "string"},  # Chloramines content in the water sample
            {"name": "Sulfate", "type": "string"},  # Sulfate content in the water sample
            {"name": "Conductivity", "type": "string"},  # Conductivity of the water sample
            {"name": "Organic_carbon", "type": "string"},  # Organic carbon content in the water sample
            {"name": "Trihalomethanes", "type": "string"},  # Trihalomethanes content in the water sample
            {"name": "Turbidity", "type": "string"},  # Turbidity of the water sample
            {"name": "Potability", "type": "string"}  # Potability status of the water sample
        ]
    }
"""
# Parse the schema string into an Avro schema object
schema = avro.schema.parse(schema_str)

# Set up Kafka configuration settings for the consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'my-group1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest message
}

# Create a Kafka consumer instance with the specified configuration
consumer = Consumer(conf)
topic = 'test_ka'  # Define the Kafka topic to subscribe to
consumer.subscribe([topic])  # Subscribe to the defined topic

# Start an infinite loop to continuously poll for messages from Kafka
while True:
    msg = consumer.poll(4.0)  # Poll the Kafka topic for messages with a timeout of 4 seconds
    if msg is None:  # Check if no message was received
        print("no data to be consumed for now ...")  # Print a message if no data is available
        break  # Exit the loop if no data is found
    else:
        # Decode the message value from bytes to string
        strison = msg.value().decode('utf-8')
        # Parse the string into a Python dictionary
        json_dict = json.loads(strison)

        # Open an Avro file for writing the data
        with open("D:\\avro_results\\data1.avro", "wb") as avro_file:  # Create and open the Avro file for writing
            writer = DataFileWriter(avro_file, DatumWriter(), schema)  # Create a DataFileWriter for the Avro file
            writer.append(json_dict)  # Append the parsed JSON data to the Avro file
            writer.close()  # Close the writer to finalize the Avro file

            # Open the Avro file for reading the written data
            with open("D:\\avro_results\\data1.avro", "rb") as avro_file:
                reader = DataFileReader(avro_file, DatumReader())  # Create a DataFileReader for the Avro file
                for water_sample in reader:  # Iterate over the records in the Avro file
                    print(water_sample)  # Print each water sample record to verify data writing

                reader.close()  # Close the reader after processing all records
