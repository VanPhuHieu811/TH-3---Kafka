"""Bài 2 - Consumer: đếm click/view/purchase mỗi phút và ghi log kết quả."""
import json
import logging
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from common import BOOTSTRAP, ensure_topic

TOPIC = "user-events"
WINDOW = 60  # giây

logging.basicConfig(
    filename="event_stats.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    encoding="utf-8",
)

ensure_topic(TOPIC, partitions=3)
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "event-analytics",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

counts = Counter()
partitions_seen = {}  # event_type -> set(partition) để kiểm chứng partitioner
window_start = time.time()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Lỗi: {msg.error()}")
            else:
                event = json.loads(msg.value().decode("utf-8"))
                et = event["event_type"]
                counts[et] += 1
                partitions_seen.setdefault(et, set()).add(msg.partition())

        if time.time() - window_start >= WINDOW:
            result = (
                f"click={counts['click']} view={counts['view']} purchase={counts['purchase']} "
                f"| partitions={ {k: sorted(v) for k, v in partitions_seen.items()} }"
            )
            print("[1 phút]", result)
            logging.info(result)
            counts.clear()
            window_start = time.time()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
