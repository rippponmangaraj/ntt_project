import json
# from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream import StreamExecutionEnvironment


class EnrichmentFunction(CoProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        descriptor = ListStateDescriptor("weather_history", Types.STRING())
        self.weather_state = runtime_context.get_list_state(descriptor)

    def process_element1(self, weather_json_str, ctx):
        """Called for weather stream"""
        self.weather_state.add(weather_json_str)

    def process_element2(self, flight_json_str, ctx):
        """Called for flight stream"""
        flight_json = json.loads(flight_json_str)
        flight_info = flight_json.get("landing", {})
        touchdown_time = flight_info.get("touchdown_time", None)

        matched_weather = {"note": "No weather data available"}

        if touchdown_time is not None:
            # Filter through weather history
            best_match_ts = -1
            for w_json_str in self.weather_state.get():
                w_obj = json.loads(w_json_str)
                weather_ts = w_obj.get("ts")
                if weather_ts is not None and weather_ts <= touchdown_time:
                    if weather_ts > best_match_ts:
                        best_match_ts = weather_ts
                        matched_weather = w_obj

        enriched = {
            "flight": flight_info,
            "matched_weather": matched_weather
        }

        print(f"\n✅ Enriched record:\n{json.dumps(enriched, indent=2)}")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-enrichment-group'
    }

    weather_consumer = FlinkKafkaConsumer(
        topics='weather',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    flights_consumer = FlinkKafkaConsumer(
        topics='flights',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    weather_stream = env.add_source(weather_consumer)
    flights_stream = env.add_source(flights_consumer)

    enriched = weather_stream.connect(flights_stream).process(EnrichmentFunction())

    env.execute("Flight Weather Enrichment with Timestamp Matching")


if __name__ == '__main__':
    main()
