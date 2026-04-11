from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='group_counting_simple',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = Counter()
msg_count = 0

print("Liczenie transakcji per sklep...")

for message in consumer:
    store_counts[message.value["store"]] += 1
    total_amount[message.value["store"]] += message.value["amount"]
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f" Raport sklepów (Wiadomość {msg_count}) ")
        for s in sorted(store_counts.keys()):
            avg = total_amount[s] / store_counts[s]
            print(f"{s}: {store_counts[s]} szt. | Suma: {total_amount[s]:.2f} | Śr: {avg:.2f}")
