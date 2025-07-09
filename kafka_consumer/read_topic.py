import json
from kafka import KafkaConsumer

TOPICS = ["weather", "flights"]

consumer = KafkaConsumer(
    *TOPICS,  # Subscribe to both topics
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='multi-topic-reader',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to topics: {', '.join(TOPICS)}")
for msg in consumer:
    print(f"## [{msg.topic}] {msg.value}")
