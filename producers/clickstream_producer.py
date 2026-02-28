import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "clickstream-events"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = [f"U{str(i).zfill(4)}" for i in range(1, 201)]
products = [f"P{str(i).zfill(4)}" for i in range(1, 51)]

def generate_event():
    user_id = random.choice(users)
    product_id = random.choice(products)

    heavy_view_products = ["P0001", "P0002", "P0003"]
    if random.random() < 0.4:
        product_id = random.choice(heavy_view_products)

    event_type = random.choices(
        ["view", "add_to_cart", "purchase"],
        weights=[0.8, 0.15, 0.05],
        k=1
    )[0]

    event = {
        "user_id": user_id,
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return event

def main():
    print("Producing clickstream events...")
    while True:
        event = generate_event()
        producer.send(TOPIC, event)
        print(event)
        time.sleep(0.1)

if __name__ == "__main__":
    main()