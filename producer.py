from kafka import KafkaProducer
import sys
import configparser
import json

config_file = "config.ini"
config = configparser.ConfigParser()

with open(config_file) as f:
    config.read_file(f)


def on_success(record_data):
    print(f"Received from JSON. Sent to: {record_data.topic}")


def on_error(e):
    print(e)


def main():
    p = KafkaProducer(
        **config["kafka"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Check that argument is provided here
    try:
        with open(sys.argv[1]) as f:
            data = json.load(f)

            for address in data:
                p.send("raw_buildings", address).add_callback(on_success).add_errback(
                    on_error
                )

            p.flush()
    except NameError:
        print("Path to JSON file not specified as first argument")
        sys.exit(1)


if __name__ == "__main__":
    main()
