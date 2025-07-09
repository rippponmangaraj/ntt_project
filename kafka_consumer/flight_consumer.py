from kafka import KafkaConsumer
import json

def consume_flights():
    consumer = KafkaConsumer(
        'flights',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flight-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening to 'flights' topic...")
    for message in consumer:
        print(f"[flights] {json.dumps(message.value, indent=2)}")

if __name__ == '__main__':
    consume_flights()
