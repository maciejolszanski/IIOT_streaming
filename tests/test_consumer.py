import io
import os
import sys
from datetime import datetime, timezone
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from fastavro import schemaless_writer

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.consumers.telemetry_consumer import AvroTransformer, TelemetryConsumer, TimescaleSink


# --- AvroTransformer Tests ---
def test_transformer_init_success():
    schema_path = os.path.join(os.path.dirname(__file__), "../schemas/telemetry.avsc")
    transformer = AvroTransformer(schema_path)
    assert transformer.schema is not None


def test_transformer_init_fail():
    with pytest.raises(FileNotFoundError):
        AvroTransformer("/non/existent/path")


def test_transformer_deserialize():
    schema_path = os.path.join(os.path.dirname(__file__), "../schemas/telemetry.avsc")
    transformer = AvroTransformer(schema_path)
    record = {"machine_id": "M1", "sensor_type": "T", "value": 1.0, "timestamp": 1000, "status_code": 0}
    out = io.BytesIO()
    schemaless_writer(out, transformer.schema, record)
    res = transformer.deserialize(out.getvalue())
    assert res["machine_id"] == "M1"


def test_transformer_to_sql_tuple():
    schema_path = os.path.join(os.path.dirname(__file__), "../schemas/telemetry.avsc")
    transformer = AvroTransformer(schema_path)
    record = {"machine_id": "M1", "sensor_type": "temp", "value": 12.3, "timestamp": 1678886400000}
    result = transformer.to_sql_tuple(record)
    assert result[1] == "M1"
    assert isinstance(result[0], datetime)
    assert result[0].tzinfo == timezone.utc


# --- TimescaleSink Tests ---
@patch("psycopg2.connect")
def test_sink_connect_and_schema(mock_connect):
    mock_cursor = mock_connect.return_value.cursor.return_value.__enter__.return_value
    TimescaleSink()
    assert mock_connect.called
    assert mock_cursor.execute.called


@patch("psycopg2.connect")
@patch("time.sleep", return_value=None)
def test_sink_connect_retries(mock_sleep, mock_connect):
    mock_connect.side_effect = [Exception("Fail"), MagicMock()]
    TimescaleSink()
    assert mock_connect.call_count == 2


@patch("psycopg2.connect")
@patch("time.sleep", return_value=None)
def test_sink_connect_gives_up(mock_sleep, mock_connect):
    mock_connect.side_effect = Exception("Permanent Fail")
    with pytest.raises(Exception, match="Permanent Fail"):
        TimescaleSink()


@patch("psycopg2.connect")
def test_sink_close(mock_connect):
    sink = TimescaleSink()
    sink.close()
    sink.conn.close.assert_called_once()
    sink.conn = None
    sink.close()  # Should not raise


class MockError(Exception):
    def __init__(self, pgcode):
        self.pgcode = pgcode


@patch("psycopg2.connect")
@patch("src.consumers.telemetry_consumer.psycopg2.Error", MockError)
def test_sink_ensure_schema_hypertable_exists(mock_connect):
    mock_cursor = mock_connect.return_value.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = [None, MockError("42101"), None]
    TimescaleSink()
    mock_connect.return_value.rollback.assert_called_once()


@patch("psycopg2.connect")
@patch("src.consumers.telemetry_consumer.psycopg2.Error", MockError)
def test_sink_ensure_schema_other_error(mock_connect):
    mock_cursor = mock_connect.return_value.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = [None, MockError("99999")]
    with pytest.raises(MockError):
        TimescaleSink()


@patch("psycopg2.connect")
@patch("src.consumers.telemetry_consumer.execute_values")
def test_sink_write_batch_success(mock_exec, mock_connect):
    sink = TimescaleSink()
    assert sink.write_batch([("data")]) is True
    sink.conn.commit.assert_called()


@patch("psycopg2.connect")
def test_sink_write_batch_failure(mock_connect):
    sink = TimescaleSink()
    sink.conn.commit.side_effect = Exception("Fail")
    assert sink.write_batch([("data")]) is False
    sink.conn.rollback.assert_called()


# --- TelemetryConsumer Tests ---
@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_setup_kafka_retries(mock_trans, mock_sink, mock_kafka):
    mock_kafka.side_effect = [Exception("Fail"), MagicMock()]
    with patch("time.sleep"):
        TelemetryConsumer()
    assert mock_kafka.call_count == 2


@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_setup_kafka_exhaustion(mock_trans, mock_sink, mock_kafka):
    mock_kafka.side_effect = Exception("Fail")
    with patch("time.sleep"):
        with pytest.raises(Exception, match="Fail"):
            TelemetryConsumer()


@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_process_message_branches(mock_trans_cls, mock_sink_cls, mock_kafka_cls):
    app = TelemetryConsumer()
    msg = MagicMock()
    msg.value.return_value = b"corrupt"
    app.transformer.deserialize.side_effect = Exception("Corrupt")
    app._process_message(msg, 50)

    app.transformer.deserialize.side_effect = None
    app.transformer.to_sql_tuple.return_value = (None, "M1", "T", 1.0, 0)
    app.batch = []
    app._process_message(msg, batch_size=10)
    assert len(app.batch) == 1

    with patch.object(app, "_flush") as mock_flush:
        app._process_message(msg, batch_size=1)
        mock_flush.assert_called_once()


@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_flush_branches(mock_trans, mock_sink, mock_kafka):
    app = TelemetryConsumer()
    app.batch = []
    app._flush()
    cast(MagicMock, app.sink).write_batch.assert_not_called()

    app.batch = [("data")]
    cast(MagicMock, app.sink).write_batch.return_value = True
    app._flush()
    app.consumer.commit.assert_called_once()

    app.batch = [("data")]
    cast(MagicMock, app.sink).write_batch.return_value = False
    app._flush()
    assert len(app.batch) == 1


@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_run_logic(mock_trans, mock_sink, mock_kafka):
    app = TelemetryConsumer()
    msg_valid = MagicMock(error=lambda: None, value=lambda: b"v")
    msg_err = MagicMock()
    msg_err.error.return_value = MagicMock()
    app.consumer.poll.side_effect = [None, msg_valid, msg_err]
    with patch.object(app, "_flush") as mock_flush:
        with patch.object(app, "_process_message") as mock_proc:
            app.run()
            mock_flush.assert_called()
            mock_proc.assert_called()


@patch("src.consumers.telemetry_consumer.Consumer")
@patch("src.consumers.telemetry_consumer.TimescaleSink")
@patch("src.consumers.telemetry_consumer.AvroTransformer")
def test_consumer_run_keyboard_interrupt(mock_trans, mock_sink, mock_kafka):
    app = TelemetryConsumer()
    app.consumer.poll.side_effect = KeyboardInterrupt()
    with patch.object(app, "_flush") as mock_flush:
        app.run()
        mock_flush.assert_called_once()
