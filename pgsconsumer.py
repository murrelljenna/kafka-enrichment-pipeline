from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine

import json
import psycopg2
import threading
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
args = config['postgresql']

db = create_engine(f"postgresql://{args['user']}:{args['password']}@{args['host']}:{args['port']}")
Base = declarative_base()

class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    street_number = Column(String)
    street_name = Column(String)
    postal_code = Column(String)

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(    
            'enriched_buildings',
            **config['kafka'],
            auto_offset_reset="earliest",
            group_id="osm-enricher",
            value_deserializer=lambda v: json.loads(v),
        )

        while not self.stop_event.is_set():
            for message in consumer:
                building = message.value
                enriched_building = self.enrich(building)
                print(enriched_building)
                self.send(enriched_building)
                if self.stop_event.is_set():
                    break

        consumer.close()

    def enrich(self, building):
        # Make API request to OSM to get extra building information.
        return building

    def send(self, building):
        p = KafkaProducer(
            **config['kafka'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        p.send('enriched_buildings', building)
        p.flush()

consumer = KafkaConsumer(    
    'enriched_buildings',
    auto_offset_reset="earliest",
    security_protocol="SSL",
    group_id="osm-enricher",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    bootstrap_servers='kafka-1f7d3844-murrelljenna-e4d0.aivencloud.com:15231',
    value_deserializer=lambda v: json.loads(v),
)

for message in consumer:
    print(message)

def main():
    conn = psycopg2.connect(
        **config['postgresql']
    )
