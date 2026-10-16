"""Bài 1 - Producer: giả lập log ứng dụng, gắn log_level vào Kafka Headers."""
import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from common import BOOTSTRAP, ensure_topic

TOPIC = "app-logs"
SERVICES = ["auth-service", "order-service", "payment-service", "web-frontend"]
LEVELS = ["INFO", "ERROR", "DEBUG"]
WEIGHTS = [60, 15, 25]
MESSAGES = {
    "INFO": ["User logged in", "Request completed", "Cache refreshed"],
    "ERROR": ["Database connection timeout", "Payment failed", "NullPointerException"],
    "DEBUG": ["Entering function process()", "Payload size = 512B", "Cache miss"],
}

ensure_topic(TOPIC, partitions=3)  # 3 partitions để chạy được tối đa 3 consumer song song
producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "log-producer"})

try:
    while True:
        level = random.choices(LEVELS, weights=WEIGHTS)[0]
        service = random.choice(SERVICES)
        log = {
            "log_level": level,
            "service_name": service,
            "message": random.choice(MESSAGES[level]),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        producer.produce(
            TOPIC,
            key=service,
            value=json.dumps(log),
            headers=[("log_level", level.encode("utf-8"))],
        )
        producer.poll(0)
        print(f"sent {level:<5} {service}")
        time.sleep(0.3)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
