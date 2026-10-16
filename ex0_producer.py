"""Bài cơ bản - Producer: giả lập thiết bị mạng gửi trạng thái Online/Offline."""
import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from common import BOOTSTRAP, ensure_topic

TOPIC = "network-status"
DEVICES = [
    ("server-01", "server"),
    ("router-01", "router"),
    ("switch-01", "switch"),
    ("switch-02", "switch"),
]

ensure_topic(TOPIC, partitions=1)
producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "network-producer"})


def on_delivery(err, msg):
    if err:
        print(f"Gửi lỗi: {err}")
    else:
        print(f"-> {msg.key().decode()} | partition={msg.partition()} offset={msg.offset()}")


try:
    while True:
        name, dtype = random.choice(DEVICES)
        status = random.choices(["Online", "Offline"], weights=[80, 20])[0]
        event = {
            "device": name,
            "type": dtype,
            "status": status,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        # key = tên thiết bị -> cùng thiết bị luôn vào cùng partition (giữ đúng thứ tự)
        producer.produce(TOPIC, key=name, value=json.dumps(event), callback=on_delivery)
        producer.poll(0)  # kích hoạt callback
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Producer đã dừng.")
