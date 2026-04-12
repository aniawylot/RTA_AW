from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
store_sums = {}
msg_count = 0

print("Liczenie transakcji per sklep...")

for message in consumer:
    tx = message.value

    store = tx["store"]
    amount = tx["amount"]

    # aktualizacja stanu
    store_counts[store] += 1
    store_sums[store] = store_sums.get(store, 0) + amount

    msg_count += 1

    # co 10 wiadomości pokazujemy raport
    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print("Sklep | Liczba | Suma | Średnia")

        for s in store_counts:
            count = store_counts[s]
            total = store_sums[s]
            avg = total / count

            print(f"{s} | {count} | {round(total,2)} | {round(avg,2)}")
            