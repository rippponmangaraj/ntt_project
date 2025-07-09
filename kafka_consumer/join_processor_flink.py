import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

def iso_to_unix(iso_str):
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp())

def enrich_flight_with_weather(flight, weather_buffer):
    flight_data = json.loads(flight)
    touchdown_time = flight_data["landing"]["touchdown_time"]

    # Find the latest weather before or at touchdown
    latest_weather = None
    for w in reversed(weather_buffer):
        weather_ts = iso_to_unix(json.loads(w)["ts"])
        if weather_ts <= touchdown_time:
            latest_weather = json.loads(w)
            break

    enriched = {
        "flight": flight_data,
        "latest_weather": latest_weather if latest_weather else {}
    }
    return json.dumps(enriched)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-enrichment-group',
    }

    flight_consumer = FlinkKafkaConsumer(
        topics='flights',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    weather_consumer = FlinkKafkaConsumer(
        topics='weather',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    flight_stream = env.add_source(flight_consumer)
    weather_stream = env.add_source(weather_consumer)

    # Collect weather in buffer (should use state in production)
    weather_buffer = []

    def collect_weather(weather):
        weather_buffer.append(weather)
        if len(weather_buffer) > 1000:
            weather_buffer.pop(0)

    # Process streams
    weather_stream.map(lambda x: collect_weather(x), output_type=Types.STRING())

    enriched_stream = flight_stream.map(lambda f: enrich_flight_with_weather(f, weather_buffer),
                                        output_type=Types.STRING())

    producer = FlinkKafkaProducer(
        topic='enriched-flights',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    enriched_stream.add_sink(producer)

    env.execute("Flight-Weather Enrichment Job")

if __name__ == '__main__':
    main()
