import sys
import json
import threading
import configparser

from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker

config_file = "config.ini"
config = configparser.ConfigParser()

# ConfigParser will not throw error if file doesn't exist.
# Opening config before passing to configparser will.
with open(config_file) as f:
    config.read_file(f)

config.read("config.ini")
args = config["postgresql"]

db = create_engine(
    f"postgresql://{args['user']}:{args['password']}@{args['host']}:{args['port']}/{args['database']}"
)
Base = declarative_base()


class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    street_number = Column(String)
    street_name = Column(String)
    postal_code = Column(String)


Base.metadata.create_all(db)


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(
            "enriched_buildings",
            **config["kafka"],
            auto_offset_reset="earliest",
            group_id="app-endpoint",
            value_deserializer=lambda v: json.loads(v),
        )

        Session = sessionmaker(db)
        session = Session()

        while not self.stop_event.is_set():
            for message in consumer:
                building = message.value
                self.send(session, building)
                if self.stop_event.is_set():
                    break

        session.close()
        consumer.close()

    def send(self, session, building):
        address = Address(**building)
        session.add(address)
        session.commit()


def main():
    c = Consumer()
    c.start()
    c.join()


if __name__ == "__main__":
    main()
