from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='count-group-v7',
    auto_offset_reset='latest'
)

print("Liczenie transakcji per sklep...")

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx["store"]
    amount = tx["amount"]

    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print("Sklep | Liczba | Suma | Średnia")

        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count
            print(f"{s} | {count} | {round(total,2)} | {round(avg,2)}")
