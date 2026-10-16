"""Bài 2 - Producer: sự kiện người dùng, phân loại partition theo event_type."""
import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from common import BOOTSTRAP, ensure_topic

TOPIC = "user-events"
PARTITION_MAP = {"click": 0, "view": 1, "purchase": 2}


def custom_partitioner(event_type):
    """Partitioner tự viết: click->0, view->1, purchase->2."""
    return PARTITION_MAP[event_type]


ensure_topic(TOPIC, partitions=3)
producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "event-producer"})


def on_delivery(err, msg):
    if err:
        print(f"Gửi lỗi: {err}")


try:
    while True:
        event_type = random.choices(["click", "view", "purchase"], weights=[50, 40, 10])[0]
        event = {
            "user_id": f"user_{random.randint(1, 50)}",
            "event_type": event_type,
            "item_id": f"item_{random.randint(1, 100)}",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        producer.produce(
            TOPIC,
            key=event["user_id"],
            value=json.dumps(event),
            partition=custom_partitioner(event_type),  # chỉ định partition tường minh
            callback=on_delivery,
        )
        producer.poll(0)
        print(f"sent {event_type:<8} -> partition {custom_partitioner(event_type)}")
        time.sleep(0.2)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
