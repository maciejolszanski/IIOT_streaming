import pytest
from unittest.mock import MagicMock, patch
import os
import sys

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.debug_consumer import DebugConsumer

@patch('src.utils.debug_consumer.Consumer')
def test_debug_consumer_misc(mock_kafka_cls):
    consumer = DebugConsumer()
    
    # Test deserialize
    with patch('src.utils.debug_consumer.schemaless_reader', return_value={'ok': 1}):
        res = consumer._deserialize_avro(b'abc')
        assert res == {'ok': 1}
    
    # Test msg is None
    consumer.consumer.poll.side_effect = [None, KeyboardInterrupt()]
    consumer.run()
    
    # Test partition EOF
    mock_eof = MagicMock()
    # KafkaError._PARTITION_EOF is usually -176 or similar, we should use the actual constant if possible
    from confluent_kafka import KafkaError
    mock_eof.error.return_value.code.return_value = KafkaError._PARTITION_EOF
    consumer.consumer.poll.side_effect = [mock_eof, KeyboardInterrupt()]
    consumer.run()

@patch('src.utils.debug_consumer.Consumer')
def test_debug_consumer_count_limit(mock_kafka_cls):
    consumer = DebugConsumer()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'v'
    mock_msg.key.return_value = b'k'
    
    with patch.object(consumer, '_deserialize_avro', return_value={}):
        consumer.consumer.poll.return_value = mock_msg
        consumer.run(max_messages=2)
        assert consumer.consumer.poll.call_count == 2

@patch('src.utils.debug_consumer.Consumer')
def test_debug_consumer_errors(mock_kafka_cls):
    consumer = DebugConsumer()
    
    # Other error
    mock_msg_other = MagicMock()
    mock_msg_other.error.return_value.code.return_value = -999
    
    # Deserialization error
    mock_msg_valid = MagicMock()
    mock_msg_valid.error.return_value = None
    mock_msg_valid.value.return_value = b'v'
    
    with patch.object(consumer, '_deserialize_avro', side_effect=Exception("Fail")):
        consumer.consumer.poll.side_effect = [mock_msg_other, mock_msg_valid, KeyboardInterrupt()]
        consumer.run(max_messages=10)
