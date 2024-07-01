from kafka import KafkaConsumer
import json
from s3fs import S3FileSystem

def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


consumer = KafkaConsumer(
    "stock_topic",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=json_deserializer
)
s3 = S3FileSystem()
for count, i in enumerate(consumer):
    with s3.open("s3://stock-market-kafka-project1/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)

if __name__ == "__main__":
    for message in consumer:
        stock_data = message.value
        print(f"Received data: {stock_data}")
