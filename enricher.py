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
            "raw_buildings",
            **config["kafka"],
            auto_offset_reset="earliest",
            group_id="osm-enricher",
            value_deserializer=lambda v: json.loads(v),
        )

        while not self.stop_event.is_set():
            for message in consumer:
                building = message.value

                enriched_building = self.enrich(building)

                self.send(enriched_building)
                if self.stop_event.is_set():
                    break

        consumer.close()

    def enrich(self, building):
        # Make API request to OSM to get extra building information.
        return building

    def send(self, building):
        p = KafkaProducer(
            **config["kafka"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        p.send("enriched_buildings", building)
        p.flush()


def main():
    enricher = Enricher()
    enricher.start()


if __name__ == "__main__":
    main()
