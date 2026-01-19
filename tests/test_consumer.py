import pytest
from unittest.mock import MagicMock, patch, ANY
import os
import sys
from datetime import datetime

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.consumers.telemetry_consumer import TelemetryConsumer, AvroTransformer, TimescaleSink

# --- AvroTransformer Tests ---
def test_transformer_init():
    # We rely on the actual schema file existing
    schema_path = os.path.join(os.path.dirname(__file__), '../schemas/telemetry.avsc')
    transformer = AvroTransformer(schema_path)
    assert transformer.schema is not None

def test_transformer_to_sql_tuple():
    schema_path = os.path.join(os.path.dirname(__file__), '../schemas/telemetry.avsc')
    transformer = AvroTransformer(schema_path)
    
    record = {
        'machine_id': 'M1',
        'sensor_type': 'temp',
        'value': 12.3,
        'timestamp': 1678886400000, # 2023-03-15 16:00:00 UTC
        'status_code': 1
    }
    
    result = transformer.to_sql_tuple(record)
    # Tuple: (dt, machine_id, sensor_type, value, status_code)
    assert result[1] == 'M1'
    assert result[2] == 'temp'
    assert result[3] == 12.3
    assert result[4] == 1
    assert isinstance(result[0], datetime)
    assert result[0].tzinfo is not None # Should be timezone aware

# --- TimescaleSink Tests ---
@patch('psycopg2.connect')
def test_sink_connect_and_schema(mock_connect):
    # Setup mock cursor
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    
    sink = TimescaleSink()
    
    mock_connect.assert_called()
    # Check if schema creation was attempted
    assert mock_cursor.execute.call_count >= 1
    # We expect CREATE TABLE and create_hypertable calls

@patch('psycopg2.connect')
@patch('src.consumers.telemetry_consumer.execute_values')
def test_sink_write_batch(mock_exec_values, mock_connect):
    sink = TimescaleSink()
    
    batch = [(datetime.now(), 'M1', 'temp', 10.0, 0)]
    success = sink.write_batch(batch)
    
    assert success is True
    mock_exec_values.assert_called_once()
    sink.conn.commit.assert_called()

@patch('psycopg2.connect')
def test_sink_write_failure(mock_connect):
    sink = TimescaleSink()
    # Make commit fail
    sink.conn.commit.side_effect = Exception("DB Error")
    
    batch = [(datetime.now(), 'M1', 'temp', 10.0, 0)]
    success = sink.write_batch(batch)
    
    assert success is False
    sink.conn.rollback.assert_called()

# --- TelemetryConsumer Tests ---
@patch('src.consumers.telemetry_consumer.Consumer')
@patch('src.consumers.telemetry_consumer.TimescaleSink')
@patch('src.consumers.telemetry_consumer.AvroTransformer')
def test_consumer_process_message(mock_transformer_cls, mock_sink_cls, mock_kafka_cls):
    consumer_app = TelemetryConsumer()
    
    mock_transformer = consumer_app.transformer
    mock_sink = consumer_app.sink
    
    # Prepare message
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'simulated_bytes'
    
    # Mock deserialization
    mock_transformer.to_sql_tuple.return_value = ('2023-01-01', 'M1', 'temp', 22.0, 0)
    mock_sink.write_batch.return_value = True
    
    # Process 1 message with batch size 1 to trigger flush
    consumer_app._process_message(mock_msg, batch_size=1)
    
    mock_transformer.deserialize.assert_called_with(b'simulated_bytes')
    mock_sink.write_batch.assert_called_once()
    consumer_app.consumer.commit.assert_called()
    assert len(consumer_app.batch) == 0

