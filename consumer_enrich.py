from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrich_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value["amount"] > 3000:
        message.value["risk_level"] = "HIGH"
    elif message.value["amount"] > 1000:
        message.value["risk_level"] = "MEDIUM"
    else:
        message.value["risk_level"] = "LOW"
    
    
    print(f"Enriched: {message.value}")
