import csv
import pandas as pd
from confluent_kafka import Producer
import time

# Replace 'localhost:9092' with your Kafka broker address
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'demo2'

# Load the preprocessed dataset
dataset_path = 'D:/level 3/big data/US_Accidents_March23.csv'
df = pd.read_csv(dataset_path)

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
}

# Create Kafka producer
producer = Producer(producer_config)

def produce_to_kafka(row):
    try:
        # Convert the row to a CSV string
        csv_data = row.to_csv(index=False)

        # Produce the CSV data to the Kafka topic
        producer.produce(kafka_topic, value=csv_data)

        print("Produced:", csv_data)

    except Exception as e:
        print(f"Error producing to Kafka: {e}")

# Simulate real-time streaming by sending rows to Kafka every second
for index, row in df.iterrows():
    produce_to_kafka(row)
    time.sleep(1)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()