from kafka import KafkaConsumer
import json
import time
from collections import defaultdict

# Konfiguracja - używamy 'broker:9092' tak jak w działającym kodzie stats
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='group_anomaly',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# Słownik do przechowywania czasów transakcji użytkowników
user_history = defaultdict(list)

print("Monitoring anomalii prędkości (3+ transakcje w 60s)...")

for message in consumer:
    event = message.value
    user_id = event['user_id']
    current_time = time.time()
    
    # 1. Dodaj czas obecnej transakcji do historii
    user_history[user_id].append(current_time)
    
    # 2. Usuń czasy starsze niż 60 sekund
    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= 60]
    
    # 3. Sprawdź warunek (więcej niż 3 transakcje)
    if len(user_history[user_id]) > 3:
        print(f"!!! ALERT: Anomalia dla użytkownika {user_id} !!!")
        print(f"Liczba transakcji w 60s: {len(user_history[user_id])}")
        print(f"Ostatnia transakcja: {event}")
        print("-" * 40)
