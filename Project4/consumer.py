from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, struct, udf
from pyspark.sql.types import StructType, StructField, StringType
import random

# Log the application start
print("Application Consumer Starting")

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("ph", StringType(), True),
    StructField("Hardness", StringType(), True),
    StructField("Solids", StringType(), True),
    StructField("Chloramines", StringType(), True),
    StructField("Sulfate", StringType(), True),
    StructField("Conductivity", StringType(), True),
    StructField("Organic_carbon", StringType(), True),
    StructField("Trihalomethanes", StringType(), True),
    StructField("Turbidity", StringType(), True),
    StructField("Potability", StringType(), True)
])

# Function to replace missing or empty values with random values
def replace_missing(value, min_value, max_value):
    return str(round(random.uniform(min_value, max_value), 14)) if value is None or value.strip() == '' else value

# Register UDFs for each column to handle missing values
replace_missing_udf = udf(lambda value, min_val, max_val: replace_missing(value, min_val, max_val), StringType())

# Configuration of Spark
spark = SparkSession.builder \
    .appName("KafkaStream") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# Reading data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topi222") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from binary to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON string into columns using the defined schema
df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

# Replace all the missing values with random values for each column
df = df.withColumn("ph", replace_missing_udf("ph", 0, 14))
df = df.withColumn("Hardness", replace_missing_udf("Hardness", 100, 300))
df = df.withColumn("Solids", replace_missing_udf("Solids", 10000, 30000))
df = df.withColumn("Chloramines", replace_missing_udf("Chloramines", 2, 10))
df = df.withColumn("Sulfate", replace_missing_udf("Sulfate", 200, 400))
df = df.withColumn("Conductivity", replace_missing_udf("Conductivity", 200, 600))
df = df.withColumn("Organic_carbon", replace_missing_udf("Organic_carbon", 10, 20))
df = df.withColumn("Trihalomethanes", replace_missing_udf("Trihalomethanes", 30, 100))
df = df.withColumn("Turbidity", replace_missing_udf("Turbidity", 2, 5))
df = df.withColumn("Potability", replace_missing_udf("Potability", 0, 1))

# Filter all the lines that have ph > 7
df = df.where("ph < 7")

# Convert the data back to a JSON string
df = df.select(to_json(struct("*")).alias("value"))

print('Read stream succeeded')
print('Starting writing data...')

# Writing data back to Kafka
query = df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output24") \
    .option("checkpointLocation", "/tmp/checkpoint78777") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# Await termination of the streaming query
query.awaitTermination()
