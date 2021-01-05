from kafka import KafkaProducer, KafkaConsumer
import json
import psycopg2
import threading
import configparser

config_file = "config.ini"
config = configparser.ConfigParser()

with open(config_file) as f:
    config.read_file(f)


class Enricher(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(
            "raw_address",
            **config["kafka"],
            auto_offset_reset="earliest",
            group_id="osm-enricher",
            value_deserializer=lambda v: json.loads(v),
        )

        while not self.stop_event.is_set():
            for message in consumer:
                address = message.value

                enriched_address = self.enrich(address)

                self.send(enriched_address)
                if self.stop_event.is_set():
                    break

        consumer.close()

    def enrich(self, address):
        # Make API request to OpenStreetMap to get extra building information.
        return address

    def send(self, address):
        print(str(address))
        p = KafkaProducer(
            **config["kafka"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        p.send("enriched_address", address)
        p.flush()


def main():
    enricher = Enricher()
    enricher.start()


if __name__ == "__main__":
    main()
