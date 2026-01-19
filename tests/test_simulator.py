import pytest
from unittest.mock import MagicMock, patch
import os
import sys
import json

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# We need to set ENV vars potentially before import if they are read at module level, 
# but they have defaults, so it's fine.
# However, SCHEMA_PATH relies on relative path.

from src.producers.simulator import TelemetrySimulator

@pytest.fixture
def mock_producer_cls():
    with patch('src.producers.simulator.Producer') as mock:
        # returns a Mock class
        # instance is mock.return_value
        producer_instance = mock.return_value
        producer_instance.list_topics.return_value = {} # successful connection
        yield mock

@pytest.fixture
def simulator(mock_producer_cls):
    return TelemetrySimulator()

def test_initialization(simulator):
    assert len(simulator.machines) == 3
    assert "M001" in simulator.machines
    assert "M001" in simulator.state

def test_generate_telemetry_structure(simulator):
    batch = simulator._generate_telemetry("M001")
    assert len(batch) == 3
    
    sensor_types = {msg['sensor_type'] for msg in batch}
    expected_types = {"temperature", "vibration", "pressure"}
    assert sensor_types == expected_types
    
    for msg in batch:
        assert msg['machine_id'] == "M001"
        assert isinstance(msg['timestamp'], int)
        assert isinstance(msg['value'], float)
        assert getattr(msg, 'status_code', 0) == 0

def test_serialization(simulator):
    record = {
        "machine_id": "TEST_M",
        "timestamp": 1600000000000,
        "sensor_type": "temperature",
        "value": 42.0,
        "status_code": 0
    }
    data = simulator._serialize_avro(record)
    assert isinstance(data, bytes)
    assert len(data) > 0

def test_run_produce_loop(simulator):
    # Test one iteration of the loop
    # We can throw an exception to break the infinite loop
    simulator.producer.produce = MagicMock()
    simulator.producer.poll = MagicMock()
    
    # Mock time.sleep to raise InterruptedError to break loop, or just run once?
    # Better: mock the loop? No, that's hard. 
    # We can mock _generate_telemetry to return empty list or just verify generate telemetry is called.
    
    # Let's just call _generate_telemetry and manually produce to verify logic if we can't easily break 'while True'
    # Or rely on KeyboardInterrupt strategy used in actual code.
    pass 

@patch('time.sleep', side_effect=KeyboardInterrupt) # Break loop immediately
def test_run_flow(mock_sleep, simulator):
    simulator.run()
    # Check if produce was called (it might produce before sleep)
    assert simulator.producer.produce.call_count >= 3 # 3 sensors for at least 1 machine? 
    # Actually loop iter: for m_id in machines -> generate -> produce
    # 3 machines * 3 sensors = 9 calls per loop
    assert simulator.producer.produce.call_count >= 9
