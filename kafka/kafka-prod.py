import subprocess
from kafka import KafkaProducer
from json import dumps
import time

class KafkaProducerWrapper:
    def __init__(self, topic, data):
        self.topic = topic
        self.data = data

    def send_data_to_kafka(self):
        kafka1_server = '172.31.9.242:19092'
        kafka2_server = '172.31.9.242:29092'
        kafka3_server = '172.31.9.242:39092'
        
        data = self.data
        print('Sending data to Kafka:', data) 
        
        bootstrap_servers = [kafka1_server, kafka2_server, kafka3_server]
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 key_serializer=None,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))
        producer.send(self.topic, value=data).get(timeout=100)
        print('Send Completed')

