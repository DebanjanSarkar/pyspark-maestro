# Method posts events to Kafka Server
# run command in kafka server to create topic : 
# ./usr/bin/kafka-topics --create --topic device_data --bootstrap-server kafka:9092 

"""
To import `kafka`, install kafka-python first, using following command:
pip install kafka-python

"""

from kafka import KafkaProducer, KafkaConsumer
import time
import random
from generate_device_events import generate_events
import uuid


KAFKA_HOST = "localhost"
KAFKA_PORT = "9092"
KAFKA_TOPIC = "device_data2"

__bootstrap_server = f"{KAFKA_HOST}:{KAFKA_PORT}"


def post_to_kafka(data, kafka_topic):
    print('Data: \n===========\n'+ str(data), end="\n\n")
    producer = KafkaProducer(bootstrap_servers=__bootstrap_server)
    generated_key = bytes(str(uuid.uuid4()), 'utf-8')
    producer.send(kafka_topic, key=generated_key, value=data)
    # producer.flush()
    producer.close()
    print(f"Posted to topic: {kafka_topic} \n============\n\n")


if __name__ == "__main__":
    _offset = 10000
    while True:
        data = bytes(str(generate_events(offset=_offset)), 'utf-8')
        post_to_kafka( data, KAFKA_TOPIC )
        time.sleep(random.randint(0, 5))
        _offset += 1

