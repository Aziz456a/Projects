# Import necessary libraries
from confluent_kafka import Producer  # Library for interacting with Kafka messaging system
import time  # Library for adding delays in execution
import csv  # Library for reading CSV files
import json  # Library for handling JSON data

if __name__ == "__main__":
    print("Application starting...")  # Notify that the application is starting
    
    # Configuration for Kafka producer, specifying the bootstrap server address
    conf = {'bootstrap.servers': 'localhost:9092'}  # Configuration of Kafka server name and port
    
    # Set up Kafka topics for producing messages
    kafka_topic = 'topi222'  # Main Kafka topic
    kafka_topic_1 = 'test_ka'  # Secondary Kafka topic

    # Create a Kafka producer instance
    producer = Producer(conf)  

    # Read data from CSV file and send each row to the Kafka topics
    with open("C:\\Users\\Aziz\\water_potability.csv", 'r') as file:
        reader = csv.DictReader(file)  # Create a CSV reader that reads the file as a dictionary
        l = 0  # Initialize a line counter
        
        for row in reader:
            if l == 60:  # Limit the number of lines read to 60 for testing
                break  # Exit the loop after reading 60 lines
            
            # Convert the current row into a JSON string
            json_row = json.dumps(row)  
            value = json_row  # Assign the JSON string to a variable for production
            
            # Produce the JSON string to the specified Kafka topics
            producer.produce(kafka_topic, value=value)  # Produce message to the main topic
            producer.produce(kafka_topic_1, value=value)  # Produce message to the secondary topic in parallel
            
            time.sleep(3)  # Wait for 3 seconds to control the data flow speed
            
            # Force the producer to send any messages currently in the buffer
            producer.flush()  

            l += 1  # Increment the line counter
