from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        "tx_id": f"TX{random.randint(1000,9999)}",
        "user_id": f"u{random.randint(1,20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"]),
        "category": random.choice(["elektronika", "odzież", "żywność", "książki"]),
        "timestamp": datetime.now().isoformat()
    }

while True:
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print("Wysłano:", tx)
    time.sleep(1)



