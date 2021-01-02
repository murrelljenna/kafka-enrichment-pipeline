from kafka import KafkaProducer
import configparser
import json

config = configparser.ConfigParser()
config.read('config.ini')

p = KafkaProducer(
    **config['kafka'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

p.send('raw_buildings', {'street_number': '25', 'street_name': 'Mabelle Ave.', 'postal_code': 'M9A 4Y1'})

p.flush()
