from kafka import KafkaProducer
import sys
import configparser
import json

# Returns a fresh dictionary made up just of the keys we want - extra key/values in the JSON will be cut off.
def clean_address(address):
    return {
        "street_number": address["street_number"],
        "street_name": address["street_name"],
        "postal_code": address["postal_code"],
    }


def main():
    config_file = "config.ini"
    config = configparser.ConfigParser()

    with open(config_file) as f:
        config.read_file(f)

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
                address = clean_address(address)
            except KeyError:
                # Json entry is missing a key - don't bother sending.
                print(
                    f"Address object {str(address)} is missing a key and has been ignored"
                )
                continue

            print(f"Sending address {str(address)}")

            producer.send("raw_address", address)

        producer.flush()


if __name__ == "__main__":
    main()
