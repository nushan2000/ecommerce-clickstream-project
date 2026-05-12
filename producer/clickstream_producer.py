import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = [
    {"product_id": "P101", "category": "Laptop"},
    {"product_id": "P102", "category": "Mobile"},
    {"product_id": "P103", "category": "Headphones"},
    {"product_id": "P104", "category": "Smartwatch"},
    {"product_id": "P105", "category": "Camera"}
]

event_types = [
    "view", "view", "view", "view", "view",
    "add_to_cart",
    "purchase"
]

print("Clickstream producer started...")

while True:
    product = random.choice(products)

    event = {
        "user_id": f"U{random.randint(1000, 1050)}",
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": random.choice(event_types),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("ecommerce_events", event)
    producer.flush()

    print("Sent:", event)

    time.sleep(1)