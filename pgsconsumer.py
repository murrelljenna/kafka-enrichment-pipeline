from kafka import KafkaProducer, KafkaConsumer

import json

consumer = KafkaConsumer(    
    'enriched_buildings',
    auto_offset_reset="earliest",
    security_protocol="SSL",
    group_id="osm-enricher",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    bootstrap_servers='kafka-1f7d3844-murrelljenna-e4d0.aivencloud.com:15231',
    value_deserializer=lambda v: json.loads(v),
)

for message in consumer:
    print(message)
