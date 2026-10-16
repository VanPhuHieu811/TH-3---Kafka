from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"


def ensure_topic(name, partitions=1, config=None):
    """Tạo topic nếu chưa tồn tại (1 broker nên replication_factor=1)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if name in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=1, config=config or {})]
    )
    for topic, fut in futures.items():
        try:
            fut.result()
            print(f"[admin] Đã tạo topic '{topic}' ({partitions} partitions)")
        except Exception as e:
            print(f"[admin] Không tạo được topic '{topic}': {e}")
