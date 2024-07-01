
# Import necessary libraries
from kafka import KafkaProducer
import pandas as pd
import json
import time
from time import sleep


# function to deserialize data from JSON format
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)
# function to read data from a CSV file and send it to a Kafka topic
def read_csv_and_send_to_kafka(csv_file_path, topic):
    df = pd.read_csv(csv_file_path)
    for _, row in df.iterrows():
        message = row.to_dict()
        print(f"Sending message: {message}")
        producer.send(topic, message)
        time.sleep(2)
        producer.flush()  # it ensure the message is sent

# main function to execute the producer script
if __name__ == "__main__":
    csv_file_path = 'stockMarket.csv' 
    topic = 'stock_topic'  # Kafka topic
    read_csv_and_send_to_kafka(csv_file_path, topic)

