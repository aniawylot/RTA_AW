from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "sum": 0.0,
    "min": float("inf"),
    "max": float("-inf")
})

msg_count = 0

print("Statystyki per kategoria...")

for message in consumer:
    tx = message.value

    cat = tx["category"]
    amount = tx["amount"]

    # aktualizacja statystyk
    stats[cat]["count"] += 1
    stats[cat]["sum"] += amount
    stats[cat]["min"] = min(stats[cat]["min"], amount)
    stats[cat]["max"] = max(stats[cat]["max"], amount)

    msg_count += 1

    # raport co 10 wiadomości
    if msg_count % 10 == 0:
        print("\n--- RAPORT KATEGORIE ---")
        print("Kategoria | Liczba | Suma | Min | Max | Średnia")

        for c, v in stats.items():
            avg = v["sum"] / v["count"]
            print(f"{c} | {v['count']} | {round(v['sum'],2)} | {round(v['min'],2)} | {round(v['max'],2)} | {round(avg,2)}") 
