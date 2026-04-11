from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='group_stats',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = {} 
msg_count = 0

for message in consumer:
    cat = message.value["category"]
    amt = message.value["amount"]
    
    if cat not in stats:
        stats[cat] = {"n": 0, "sum": 0.0, "min": amt, "max": amt}
    
    stats[cat]["n"] += 1
    stats[cat]["sum"] += amt
    stats[cat]["min"] = min(stats[cat]["min"], amt)
    stats[cat]["max"] = max(stats[cat]["max"], amt)
    
    msg_count += 1
    if msg_count % 10 == 0:
        print(f"STATYSTYKI KATEGORII ({msg_count})")
        print(stats)
