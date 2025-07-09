import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.transforms.external import ExternalTransform
from apache_beam.io.kafka import ReadFromKafka


OUTPUT_PATH = '/Users/ripponmangaraj/Desktop/assessment-ntt/output/enriched_flights.json'

class EnrichFlightsDoFn(beam.DoFn):
    def process(self, element):
        flight = element['flight']
        weather_list = element['weather']

        matched_weather = {"note": "No weather data available"}
        touchdown_ts = flight.get("landing", {}).get("touchdown_time", 0)

        for w in sorted(weather_list, key=lambda x: x["ts"]):
            if w["ts"] <= touchdown_ts:
                matched_weather = w
            else:
                break

        enriched = {
            "flight": flight["landing"],
            "matched_weather": matched_weather
        }

        # Return JSON string for writing to file
        yield json.dumps(enriched)

def run():
    options = PipelineOptions(
        runner="DirectRunner",  # Or "DataflowRunner" in cloud
        streaming=True,
    )

    with beam.Pipeline(options=options) as p:

        kafka_config = {
            "bootstrap.servers": "localhost:9092"
        }

        weather = (
            p
            | "ReadWeather" >> beam.io.ReadFromKafka(
                consumer_config=kafka_config,
                topics=["weather"]
            )
            | "ParseWeather" >> beam.Map(lambda x: json.loads(x.value.decode("utf-8")))
            | "WeatherByKey" >> beam.Map(lambda w: ("LHR", w))
        )

        flights = (
            p
            | "ReadFlights" >> beam.io.ReadFromKafka(
                consumer_config=kafka_config,
                topics=["flights"]
            )
            | "ParseFlights" >> beam.Map(lambda x: json.loads(x.value.decode("utf-8")))
            | "FlightsByKey" >> beam.Map(lambda f: ("LHR", f))
        )

        enriched = (
            {'weather': weather, 'flight': flights}
            | "JoinOnAirport" >> beam.CoGroupByKey()
            | "EnrichFlights" >> beam.ParDo(EnrichFlightsDoFn())
        )

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

        # Beam WriteToText will write multiple shards, so we specify:
        enriched | "WriteToFile" >> beam.io.WriteToText(
            file_path_prefix=OUTPUT_PATH,
            file_name_suffix=".json",
            shard_name_template="",  # Avoids "-00000-of-00001" suffixes
            append_trailing_newlines=True
        )

if __name__ == '__main__':
    run()
