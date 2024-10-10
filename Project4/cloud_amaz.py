from confluent_kafka import Consumer  # Import the Kafka consumer from the confluent_kafka library
import json  # Import the json module for parsing JSON data
import boto3  # Import the boto3 library for AWS services interaction
import os  # Import the os module for operating system interactions

# Configure AWS credentials using environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'YOUR_AWS_ACCESS_KEY_ID'  # AWS Access Key ID (replace with your actual key)
os.environ['AWS_SECRET_ACCESS_KEY'] = 'YOUR_AWS_SECRET_ACCESS_KEY'  # AWS Secret Access Key (replace with your actual secret)
os.environ['AWS_DEFAULT_REGION'] = 'eu-north-1'  # Default AWS region for resource access

# Set up the Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'my-group5',  # Consumer group ID
    'auto.offset.reset': 'latest'  # Start consuming from the latest message
}

# Create a Kafka consumer instance with the specified configuration
kafka_consumer = Consumer(kafka_consumer_config)
kafka_consumer.subscribe(['output24'])  # Subscribe to the specified Kafka topic

# Configure the DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')  # Initialize DynamoDB resource
table = dynamodb.Table('dataWater')  # Reference the DynamoDB table named 'dataWater'

i = 0  # Initialize a counter for unique water data IDs

# Consume data from Kafka and write it to DynamoDB
while True:
    msg = kafka_consumer.poll(5.0)  # Poll the Kafka topic for messages with a timeout of 5 seconds
    if msg is None:
        print("There is no data to be consumed, waiting ...")  # Print a message if no data is available
        break  # Exit the loop if no data is found
    try:
        # Convert the JSON message from Kafka into a Python dictionary
        item = json.loads(msg.value().decode('utf-8'))  # Decode and parse the message
        item['water_dataID'] = str(i)  # Assign a unique ID to the item
        i += 1  # Increment the counter
        print(item)  # Print the item to verify its contents

        # Insert the item into the DynamoDB table
        table.put_item(Item=item)  # Insert the item into DynamoDB
        print("Data inserted into DynamoDB: ", item)  # Confirm successful insertion
    except Exception as e:
        # Handle any exceptions that occur during data insertion
        print("Error during data insertion into DynamoDB: ", e)

    except KeyboardInterrupt:
        # Handle keyboard interruption gracefully
        print('Interrupted data reading from Kafka')  # Print a message on interruption

