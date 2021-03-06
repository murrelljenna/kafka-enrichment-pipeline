import sys, time
import json
import threading
import configparser

from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    street_number = Column(String)
    street_name = Column(String)
    postal_code = Column(String)


class Consumer(threading.Thread):
    def __init__(self, config, engine):
        threading.Thread.__init__(self)
        self.engine = engine
        self.config = config
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(
            "enriched_address",
            **self.config["kafka"],
            auto_offset_reset="earliest",
            group_id="app-endpoint",
            value_deserializer=lambda v: json.loads(v),
        )

        Session = sessionmaker(self.engine)
        session = Session()

        while not self.stop_event.is_set():
            for message in consumer:
                raw_address = message.value
                print(f"Received address: {str(raw_address)} from enriched_address")
                self.send(session, raw_address)
                if self.stop_event.is_set():
                    break

        session.close()
        consumer.close()

    def send(self, session, raw_address):
        print(f"Sending address: {str(raw_address)} to postgres")
        address = Address(**raw_address)
        session.add(address)
        session.commit()


def main():
    config_file = "config.ini"
    config = configparser.ConfigParser()

    # ConfigParser will not throw error if file doesn't exist.
    # Opening config before passing to configparser will.
    with open(config_file) as f:
        config.read_file(f)

    args = config["postgresql"]

    db = create_engine(
        f"postgresql://{args['user']}:{args['password']}@{args['host']}:{args['port']}/{args['database']}"
    )

    Base.metadata.create_all(db)

    consumer = Consumer(config, db)
    consumer.setDaemon(True)
    print("Starting consumer")
    consumer.start()

    try:
        while 1:
            time.sleep(.1)
    except KeyboardInterrupt:
        print("Stopping consumer")
        consumer.stop()


if __name__ == "__main__":
    main()
