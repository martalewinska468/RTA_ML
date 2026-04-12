from kafka import KafkaConsumer
import json
import time


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='group_anomaly_clean',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)


user_history = {}

print("Monitoring anomalii prędkości (3+ transakcje w 60s)...")

for message in consumer:
    event = message.value
    user_id = event['user_id']
    current_time = time.time()
    
    
    if user_id not in user_history:
        user_history[user_id] = []
    
    
    user_history[user_id].append(current_time)
    
    
    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= 60]
    
    
    if len(user_history[user_id]) > 3:
        print(f"!!! ALERT: Anomalia dla użytkownika {user_id} !!!")
        print(f"Liczba transakcji w 60s: {len(user_history[user_id])}")
        print(f"Ostatnia transakcja: {event}")
        print("-" * 40)
