# coding: utf-8

import uuid
import json
import random
import datetime
import time
from kafka import KafkaProducer

bootstrap_servers="127.0.0.1:9092"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers.split(","))
topic = "test"

class Task():

    def producer(self, ruleId):
        t = (int(round(time.time() * 1000)))
        msg = '{"ruleId":%d, "userId":"test_user_id", "ip":"127.0.0.1", "type":"sucess", "timestamp":%d}'%(ruleId, t)
        key = str(uuid.uuid1())
        future = producer.send(topic, key=key.encode('utf-8'), value=msg.encode('utf-8'))
        result = future.get(timeout=10)
        print(msg)


if __name__ == "__main__":
    t = Task()
    t.producer(1)
    t.producer(2)