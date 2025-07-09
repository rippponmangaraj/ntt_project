import sys
import os
import json
from kafka import KafkaConsumer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import from local modules
import config
from kafka_consumer.join_processor import process_and_enrich_realtime
from config import BOOTSTRAP_SERVERS, FLIGHT_TOPIC, WEATHER_TOPIC, KAFKA_GROUP_ID, OUTPUT_PATH

def read_kafka_topic(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    data = []
    for msg in consumer:
        data.append(msg.value)
    return data

def main():
    print(" Reading weather topic...")
    weather_messages = read_kafka_topic(WEATHER_TOPIC)

    print(" Reading flights topic...")
    flight_messages = read_kafka_topic(FLIGHT_TOPIC)

    print("  Enriching and saving to file...")
    process_and_enrich_realtime(flight_messages, weather_messages, OUTPUT_PATH)

    print(f"\n Output written to: {OUTPUT_PATH}")

if __name__ == '__main__':
    main()
