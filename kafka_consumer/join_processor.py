from kafka import KafkaConsumer
import json
import bisect
import os
from datetime import datetime, timezone
from config import OUTPUT_PATH

# Helper: convert ISO 8601 timestamp to UNIX epoch
def iso_to_unix(iso_time):
    dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

# Main real-time enrichment processor
def process_and_enrich_realtime():
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    consumer = KafkaConsumer(
        'flights', 'weather',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='join-processor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    weather_data = []
    weather_timestamps = []

    print(" Real-time enrichment started. Listening to 'flights' and 'weather' topics...")

    for message in consumer:
        if message.topic == 'weather':
            try:
                weather_msg = message.value
                ts = iso_to_unix(weather_msg['ts'])
                idx = bisect.bisect_right(weather_timestamps, ts)
                weather_timestamps.insert(idx, ts)
                weather_data.insert(idx, weather_msg)
                print(f" Weather received and buffered: {weather_msg['ts']}")
            except Exception as e:
                print(f"Error processing weather message: {e}")

        elif message.topic == 'flights':
            try:
                flight_msg = message.value
                flight_info = flight_msg['landing']
                touchdown_ts = flight_info['touchdown_time']  

                idx = bisect.bisect_right(weather_timestamps, touchdown_ts) - 1
                matched_weather = weather_data[idx] if idx >= 0 else {"note": "No weather data available"}

                enriched = {
                    "flight": flight_info,
                    "matched_weather": matched_weather
                }

                print(f"\n  Enriched Record:\n{json.dumps(enriched, indent=2)}")

                # Append to output file
                with open(OUTPUT_PATH, "a") as f:
                    f.write(json.dumps(enriched) + "\n")

            except Exception as e:
                print(f" Error processing flight message: {e}")

if __name__ == "__main__":
    process_and_enrich_realtime()
