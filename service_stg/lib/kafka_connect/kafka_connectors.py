import json
import os
from typing import Dict, Optional
from confluent_kafka import Consumer, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'ssl.ca.location': cert_path,
            'error_cb': error_callback,
        }
        self.topic = topic
        self.p = Producer(params)

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'ssl.ca.location': cert_path,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'debug': 'all',
            'client.id': 'someclientkey',
            'socket.timeout.ms': 30000,
            'session.timeout.ms': 45000,
            'max.poll.interval.ms': 300000
        }
        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.c.poll(timeout=timeout)
        if msg is None:
            return None
        if msg.error():
            raise Exception(msg.error())
        value = msg.value()
        if value is None:
            return None
        return json.loads(value.decode('utf-8'))
