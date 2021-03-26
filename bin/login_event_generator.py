# coding: utf-8

import uuid
import time
from kafka import KafkaProducer

bootstrap_servers="127.0.0.1:9092"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers.split(","))
topic = "test"

class Task():

    def producer(self):
        timestamp = (int(round(time.time() * 1000)))
        msg = '{"userId":"test_user_id", "ip":"127.0.0.1", "type":"sucess", "timestamp":%d}'%(timestamp)
        key = str(uuid.uuid1())
        future = producer.send(topic, key=key.encode('utf-8'), value=msg.encode('utf-8'))
        future.get(timeout=10)
        print(msg)


if __name__ == "__main__":
    t = Task()
    t.producer()