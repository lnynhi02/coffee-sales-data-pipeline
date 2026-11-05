#========================================================
# SCRIPT: Kafka Client Wrapper for Producer and Consumer
#========================================================
# Purpose:
#   Encapsulate Kafka producer and consumer creation
#========================================================
import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaHandler:
    def __init__(self, bootstrap_server: str):
        self.bootstrap_server = bootstrap_server

    def get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda m: json.dumps(m).encode('utf-8') 
        )
    
    def get_consumer(self, topic: str, group_id: str = None):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_server,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            auto_offset_reset='latest',
        )