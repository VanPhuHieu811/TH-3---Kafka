"""Bài cơ bản - Consumer: hiển thị, xử lý Online/Offline, lưu trạng thái hiện tại."""
import json

from confluent_kafka import Consumer, KafkaError
from common import BOOTSTRAP, ensure_topic

TOPIC = "network-status"
STATE_FILE = "network_state.json"

ensure_topic(TOPIC, partitions=1)
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "network-monitor",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

current_state = {}  # {device: status}

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Lỗi: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        device, status = event["device"], event["status"]
        old = current_state.get(device)
        current_state[device] = status

        print(f"[{event['timestamp']}] {device:<10} ({event['type']}): {status}")
        if status == "Offline":
            print(f"   !!! CẢNH BÁO: {device} đang OFFLINE")
        elif old == "Offline":
            print(f"   ✔ {device} đã hoạt động trở lại")

        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(current_state, f, indent=2, ensure_ascii=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Trạng thái cuối:", current_state)
