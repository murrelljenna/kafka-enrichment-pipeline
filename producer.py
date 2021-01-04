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
        request_timeout_ms=1000,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        sys.argv[1]
    except IndexError:
        print("Path to JSON file not specified as first argument")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        # Load our json file, send data for each to kafka.
        data = json.load(f)

        for address in data:
            try:
                # Send only the keys we're looking for.
                processed_address = {'street_number': address['street_number'], 'street_name': address['street_name'], 'postal_code': address['postal_code']}
            except KeyError:
                # Json entry is missing a key - don't bother sending.
                continue

            p.send("raw_buildings", processed_address).add_callback(on_success).add_errback(
                on_error
            )

        p.flush()


if __name__ == "__main__":
    main()
