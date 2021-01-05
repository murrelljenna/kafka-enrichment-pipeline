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

# Returns a fresh dictionary made up just of the keys we want - extra key/values in the JSON will be cut off.
def process_address(address):
    return {'street_number': address['street_number'], 'street_name': address['street_name'], 'postal_code': address['postal_code']}


def main():
    producer = KafkaProducer(
        **config["kafka"],
        request_timeout_ms=1000,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Test that an argument has been provided.
    if len(sys.argv) < 2:
        print("Path to JSON file not specified as first argument")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        # Load our json file, send data for each to kafka.
        data = json.load(f)

        for address in data:
            try:
                # Send only the keys we're looking for.
                processed_address = process_address(address)
                print(str(processed_address))
            except KeyError:
                # Json entry is missing a key - don't bother sending.
                continue

            producer.send("raw_address", processed_address).add_callback(on_success).add_errback(
                on_error
            )

        producer.flush()


if __name__ == "__main__":
    main()
