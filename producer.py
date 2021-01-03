from kafka import KafkaProducer
import sys
import configparser
import json

config_file = 'config.ini'
config = configparser.ConfigParser()
with open(config_file) as f:
    config.read_file(f)

def on_success(record_data):
    print(f"Sent: record_data")

def on_error(e):
    print(e)

def main():
    p = KafkaProducer(
        **config['kafka'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    with open(sys.argv[1]) as input:
        data = json.load(input)
        print(data)

        p.send('raw_buildings', {'street_number': '25', 'street_name': 'Mabelle Ave.', 'postal_code': 'M9A 4Y1'}).add_callback(on_success).add_errback(on_error)

        p.flush()

if __name__ == "__main__":
    main()
