"""Bài 1 - Consumer: tách ERROR, ghi error.log, thống kê theo mức độ mỗi 10 giây.
Chạy nhiều bản (nhiều terminal) cùng group.id để xử lý song song."""
import json
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from common import BOOTSTRAP, ensure_topic

TOPIC = "app-logs"
WINDOW = 10  # giây

ensure_topic(TOPIC, partitions=3)
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "log-processors",  
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])


def get_header(msg, name):
    for k, v in (msg.headers() or []):
        if k == name:
            return v.decode("utf-8")
    return None


counts = Counter()
window_start = time.time()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Lỗi: {msg.error()}")
            else:
                level = get_header(msg, "log_level")  
                log = json.loads(msg.value().decode("utf-8"))
                counts[level] += 1

                if level == "ERROR":  # xử lý ưu tiên
                    line = f"{log['timestamp']} [ERROR] {log['service_name']}: {log['message']}"
                    with open("error.log", "a", encoding="utf-8") as f:
                        f.write(line + "\n")
                    print("!!", line)

        # Kiểm tra cửa sổ thời gian 
        if time.time() - window_start >= WINDOW:
            print(
                f"--- {WINDOW}s qua | INFO={counts['INFO']} "
                f"ERROR={counts['ERROR']} DEBUG={counts['DEBUG']} ---"
            )
            counts.clear()
            window_start = time.time()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
