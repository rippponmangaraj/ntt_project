import json
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError

def create_topics():
    print("Setting up Kafka Admin")
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='kafka-interview-setup'
    )

    topic_list = [
        NewTopic(name='flights', num_partitions=2, replication_factor=1),
        NewTopic(name='weather', num_partitions=2, replication_factor=1)
    ]

    try:
        print('Creating Kafka topics...')
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        print(" Topics already exist. Skipping creation.")
    finally:
        admin_client.close()
        print("Kafka Admin setup complete.\n")

def send_messages(topic, folder_path):
    if not os.path.isdir(folder_path):
        print(f" Folder not found: {folder_path}")
        return

    print(f"Sending data from '{folder_path}' to topic '{topic}'")
    files = sorted(os.listdir(folder_path))
    for file in files:
        if file.endswith(".json"):
            path = os.path.join(folder_path, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    producer.send(topic, value=data)
                    print(f"  Sent: {file}")
            except Exception as e:
                print(f"  Error reading {file}: {e}")
    producer.flush()
    print(f"All messages sent to '{topic}'\n")

if __name__ == '__main__':
    # Step 1: Create topics
    create_topics()

    # Step 2: Create a Kafka producer
    print("Creating Kafka Producer")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Step 3: Send weather and flight messages
    send_messages("weather", "../samples/weather")
    send_messages("flights", "../samples/flights")

    # Step 4: Close producer
    print(" Closing producer")
    producer.close()
    print(" Done!")
