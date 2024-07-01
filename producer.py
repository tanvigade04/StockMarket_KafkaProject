
from kafka import KafkaProducer
import pandas as pd
import json
import time
from time import sleep

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

def read_csv_and_send_to_kafka(csv_file_path, topic):
    df = pd.read_csv(csv_file_path)
    for _, row in df.iterrows():
        message = row.to_dict()
        print(f"Sending message: {message}")
        producer.send(topic, message)
        time.sleep(2)
        producer.flush()  # Ensure the message is sent

if __name__ == "__main__":
    csv_file_path = 'stockMarket.csv'  # Replace with your CSV file path
    topic = 'stock_topic'  # Replace with your Kafka topic
    read_csv_and_send_to_kafka(csv_file_path, topic)

