import os
import json
import random
import time
from datetime import datetime, UTC

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# SASL settings
KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL")  # SASL_SSL or SASL_PLAINTEXT
# PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

# Optional SSL CA file for remote clusters
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE")  # example: certs/ca.pem

# Load data
script_dir = os.path.dirname(os.path.abspath(__file__))
df_restaurants = pd.read_csv(os.path.join(
    script_dir, "data", "restaurants.csv"))
df_customers = pd.read_csv(os.path.join(script_dir, "data", "customers.csv"))
df_menu_items = pd.read_csv(os.path.join(script_dir, "data", "menu_items.csv"))

RESTAURANTS = df_restaurants["restaurant_id"].tolist()
CUSTOMERS = df_customers["customer_id"].tolist()
MENU_BY_RESTAURANT = df_menu_items.groupby("restaurant_id").apply(
    lambda x: x.to_dict("records")
).to_dict()

ORDER_TYPES = ["dine_in", "takeaway", "delivery"]
PAYMENT_METHODS = ["cash", "card", "wallet"]
ORDER_STATUSES = ["pending", "confirmed", "preparing", "ready", "delivered"]


def generate_order():
    order_date = datetime.now(UTC)
    restaurant_id = random.choice(RESTAURANTS)
    customer_id = random.choice(CUSTOMERS)

    menu_items = MENU_BY_RESTAURANT[restaurant_id]
    num_items = random.randint(1, min(5, len(menu_items)))
    selected_items = random.sample(menu_items, num_items)

    items = []
    total_amount = 0.0

    for item in selected_items:
        quantity = random.randint(1, 3)
        subtotal = item["price"] * quantity
        total_amount += subtotal

        items.append({
            "item_id": item["item_id"],
            "name": item["name"],
            "category": item["category"],
            "quantity": quantity,
            "unit_price": item["price"],
            "subtotal": round(subtotal, 2)
        })

    order_id = f"ORD-{order_date.strftime('%Y%m%d')}-{random.randint(100000, 999999)}"

    return {
        "order_id": order_id,
        "timestamp": order_date.isoformat(),
        "restaurant_id": restaurant_id,
        "customer_id": customer_id,
        "order_type": random.choice(ORDER_TYPES),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "created_at": order_date.isoformat()
    }


def build_kafka_producer():
    config = {
        "bootstrap_servers": [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")],
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda v: v.encode("utf-8"),
        "retries": 5,
        "linger_ms": 100,
        "request_timeout_ms": 30000,
        "api_version_auto_timeout_ms": 10000,
    }

    if KAFKA_SECURITY_PROTOCOL:
        config["security_protocol"] = KAFKA_SECURITY_PROTOCOL

    if KAFKA_SASL_MECHANISM:
        config["sasl_mechanism"] = KAFKA_SASL_MECHANISM

    if KAFKA_USERNAME:
        config["sasl_plain_username"] = KAFKA_USERNAME

    if KAFKA_PASSWORD:
        config["sasl_plain_password"] = KAFKA_PASSWORD

    if KAFKA_SSL_CAFILE:
        cafile_path = KAFKA_SSL_CAFILE
        if not os.path.isabs(cafile_path):
            cafile_path = os.path.join(script_dir, cafile_path)

        if not os.path.exists(cafile_path):
            raise FileNotFoundError(f"CA file not found: {cafile_path}")

        config["ssl_cafile"] = cafile_path

    return KafkaProducer(**config)


def stream_to_kafka(interval_seconds=3, max_orders=None):
    producer = build_kafka_producer()

    print(f"\nStreaming to Kafka topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    order_count = 0

    try:
        while True:
            order = generate_order()

            future = producer.send(
                KAFKA_TOPIC,
                key=order["order_id"],
                value=order
            )

            try:
                record_metadata = future.get(timeout=10)
                print(
                    f"[{order_count + 1}] {order['order_id']} | "
                    f"{order['restaurant_id']} | AED {order['total_amount']} | "
                    f"Delivered to {record_metadata.topic} "
                    f"[{record_metadata.partition}] at offset {record_metadata.offset}"
                )
            except Exception as e:
                print(f"Delivery failed for {order['order_id']}: {e}")

            print(json.dumps(order, indent=4))
            print()

            order_count += 1
            if max_orders and order_count >= max_orders:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    stream_to_kafka(interval_seconds=3)
