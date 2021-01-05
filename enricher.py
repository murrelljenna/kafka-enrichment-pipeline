from kafka import KafkaProducer, KafkaConsumer
import time
import json
import psycopg2
import threading
import configparser


class Enricher(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(
            "raw_address",
            **self.config["kafka"],
            auto_offset_reset="earliest",
            group_id="osm-enricher",
            value_deserializer=lambda v: json.loads(v),
        )

        while not self.stop_event.is_set():
            for message in consumer:
                address = message.value

                print(f"Received address: {str(address)} from raw_address")
                enriched_address = self.enrich(address)

                self.send(enriched_address)
                if self.stop_event.is_set():
                    break

        consumer.close()

    def enrich(self, address):
        # Make API request to OpenStreetMap to get extra building information.
        return address

    def send(self, address):
        print(f"Sending address: {str(address)} to enriched_address")
        p = KafkaProducer(
            **self.config["kafka"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        p.send("enriched_address", address)
        p.flush()


def main():
    config_file = "config.ini"
    config = configparser.ConfigParser()

    with open(config_file) as f:
        config.read_file(f)

    enricher = Enricher(config)
    enricher.setDaemon(True)
    print("Starting enricher")
    enricher.start()

    try:
        while 1:
            time.sleep(.1)
    except KeyboardInterrupt:
        print("Stopping enricher")
        enricher.stop()

if __name__ == "__main__":
    main()
