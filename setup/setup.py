import json
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import (
    TopicAlreadyExistsError,
    KafkaError,
    NoBrokersAvailable
)


def create_topics():
    print("Setting up Kafka Admin")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='kafka-interview-setup'
        )
    except NoBrokersAvailable as e:
        print(f" Kafka broker not available: {e}")
        return
    except Exception as e:
        print(f" Error creating Kafka admin client: {e}")
        return

    topic_list = [
        NewTopic(name='flights', num_partitions=2, replication_factor=1),
        NewTopic(name='weather', num_partitions=2, replication_factor=1)
    ]

    try:
        print('Creating Kafka topics...')
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        print("⚠️ Topics already exist. Skipping creation.")
    except KafkaError as e:
        print(f" Kafka error while creating topics: {e}")
    except Exception as e:
        print(f" General error during topic creation: {e}")
    finally:
        try:
            admin_client.close()
        except Exception as e:
            print(f"⚠️ Error closing Kafka admin client: {e}")
        print(" Kafka Admin setup complete.\n")


def send_messages(topic, folder_path):
    if not os.path.isdir(folder_path):
        print(f" Folder not found: {folder_path}")
        return

    print(f"📤 Sending data from '{folder_path}' to topic '{topic}'")
    files = sorted(os.listdir(folder_path))
    for file in files:
        if file.endswith(".json"):
            path = os.path.join(folder_path, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    try:
                        producer.send(topic, value=data)
                        print(f"   Sent: {file}")
                    except KafkaError as e:
                        print(f"   Kafka send error for {file}: {e}")
            except json.JSONDecodeError as e:
                print(f"   Invalid JSON in {file}: {e}")
            except Exception as e:
                print(f"   Error reading {file}: {e}")
    try:
        producer.flush()
    except KafkaError as e:
        print(f" Kafka flush error: {e}")
    print(f" All messages sent to '{topic}'\n")


if __name__ == '__main__':
    # Step 1: Create topics
    create_topics()

    # Step 2: Create a Kafka producer
    print(" Creating Kafka Producer")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            acks='all',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable as e:
        print(f" Kafka broker not available: {e}")
        exit(1)
    except Exception as e:
        print(f" Failed to create Kafka producer: {e}")
        exit(1)

    # Step 3: Send weather and flight messages
    try:
        send_messages("weather", "../samples/weather")
        send_messages("flights", "../samples/flights")
    except Exception as e:
        print(f" Error during message sending: {e}")

    # Step 4: Close producer
    print(" Closing producer")
    try:
        producer.close()
        print(" Done!")
    except Exception as e:
        print(f" Error closing producer: {e}")
