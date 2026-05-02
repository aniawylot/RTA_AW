from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly-group-v1',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# przechowujemy czasy transakcji per user
user_events = defaultdict(deque)

print("Wykrywanie anomalii (3 transakcje / 60s)...")

for message in consumer:
    tx = message.value
    user = tx["user_id"]
    now = time.time()

    user_events[user].append(now)

    while user_events[user] and now - user_events[user][0] > 60:
        user_events[user].popleft()

    if len(user_events[user]) > 3:
        print(f"🚨 ANOMALIA: {user} → {len(user_events[user])} transakcji w 60s!")
        
