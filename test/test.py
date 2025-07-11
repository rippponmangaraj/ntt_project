import pytest
from unittest.mock import patch, MagicMock
import json

import kafka_reader  


@pytest.fixture
def fake_kafka_messages():
    return [
        {'flight_id': 1, 'status': 'on_time'},
        {'flight_id': 2, 'status': 'delayed'}
    ]


@patch('kafka_reader.KafkaConsumer')
def test_read_kafka_topic(mock_kafka_consumer, fake_kafka_messages):
    mock_instance = MagicMock()
    mock_instance.__iter__.return_value = [
        MagicMock(value=json.dumps(msg).encode('utf-8')) for msg in fake_kafka_messages
    ]
    mock_kafka_consumer.return_value = mock_instance

    with patch('kafka_reader.json.loads', side_effect=lambda x: json.loads(x)):
        result = kafka_reader.read_kafka_topic("fake-topic")
        assert result == fake_kafka_messages


@patch('kafka_reader.process_and_enrich_realtime')
@patch('kafka_reader.read_kafka_topic')
def test_main(mock_read_kafka_topic, mock_process_and_enrich):
    # Mock returned messages
    mock_read_kafka_topic.side_effect = [
        [{'weather': 'sunny'}],  # WEATHER_TOPIC
        [{'flight': 'AB123'}]    # FLIGHT_TOPIC
    ]

    kafka_reader.main()

    assert mock_read_kafka_topic.call_count == 2
    mock_process_and_enrich.assert_called_once_with(
        [{'flight': 'AB123'}],
        [{'weather': 'sunny'}],
        kafka_reader.OUTPUT_PATH
    )
