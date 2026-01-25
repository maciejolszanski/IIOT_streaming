import io
import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader
from psycopg2.extras import execute_values

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_NAME = os.getenv("TOPIC_NAME", "telemetry_raw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/telemetry.avsc")
GROUP_ID = os.getenv("GROUP_ID", "telemetry-consumer-group")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "iiot_db")
DB_USER = os.getenv("DB_USER", "iiot_user")
DB_PASS = os.getenv("DB_PASS", "iiot_password")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "5"))
RETRY_WAIT_SECONDS = int(os.getenv("RETRY_WAIT_SECONDS", "5"))

# --- Setup Logging ---
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class AvroTransformer:
    """Handles deserialization and mapping of Avro records to SQL tuples."""

    def __init__(self, schema_path):
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Missing schema at {schema_path}")
        with open(schema_path, "r") as f:
            self.schema = parse_schema(json.load(f))

    def deserialize(self, payload):
        bytes_io = io.BytesIO(payload)
        return schemaless_reader(bytes_io, self.schema, None)

    def to_sql_tuple(self, record):
        """Converts raw Avro dict to a tuple suitable for Postgres INSERT."""
        ts_ms = record.get("timestamp", int(time.time() * 1000))
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return (
            dt,
            record.get("machine_id", "Unknown"),
            record.get("sensor_type", "Unknown"),
            record.get("value", 0.0),
            record.get("status_code", 0),
        )


class TimescaleSink:
    """Handles all Database interactions: connection, schema, and batch writing."""

    def __init__(self):
        self.conn = self._connect()
        self._ensure_schema()

    def _connect(self):
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                logger.info(f"Connected to TimescaleDB on attempt {attempt}")
                return conn
            except Exception as e:
                if attempt == RETRY_ATTEMPTS:
                    raise
                logger.warning(f"DB connection attempt {attempt} failed: {e}")
                time.sleep(RETRY_WAIT_SECONDS)

    def _ensure_schema(self):
        """Orchestrates the creation of the schema and optimizations."""
        self._create_table()
        self._configure_timescale()

    def _create_table(self):
        """Creates the standard PostgreSQL table if it doesn't exist."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    event_time TIMESTAMPTZ NOT NULL,
                    machine_id TEXT NOT NULL,
                    sensor_type TEXT NOT NULL,
                    value DOUBLE PRECISION,
                    status_code INTEGER
                );
            """)
            self.conn.commit()
            logger.debug("Base telemetry table ensured.")

    def _configure_timescale(self):
        """Converts table to hypertable and adds time-series indexes."""
        with self.conn.cursor() as cur:
            # Create hypertable using if_not_exists (supported in TimescaleDB 2.0+)
            try:
                cur.execute("SELECT create_hypertable('telemetry', 'event_time', if_not_exists => TRUE);")
                self.conn.commit()
                logger.info("Telemetry table hypertable status ensured.")
            except psycopg2.Error as e:
                logger.error(f"Failed to ensure hypertable: {e}")
                self.conn.rollback()
                raise

            # Add optimized index for time-series queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_telemetry_machine_time
                ON telemetry (machine_id, event_time DESC);
            """)
            self.conn.commit()
            logger.debug("TimescaleDB indexes ensured.")

    def write_batch(self, batch):
        """Writes batch to DB. Returns True on success."""
        query = "INSERT INTO telemetry (event_time, machine_id, sensor_type, value, status_code) VALUES %s"
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, batch)
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"💥 Database write failed: {e}")
            self.conn.rollback()
            return False

    def close(self):
        if self.conn:
            self.conn.close()


class TelemetryConsumer:
    """Orchestrator: Coordinates between KafkaSource, Transformer, and Sink."""

    def __init__(self):
        self.transformer = AvroTransformer(SCHEMA_PATH)
        self.sink = TimescaleSink()
        self.consumer = self._setup_kafka()
        self.batch = []

    def _setup_kafka(self):
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                c = Consumer(conf)
                c.list_topics(timeout=2)
                c.subscribe([TOPIC_NAME])
                return c
            except Exception:
                if attempt == RETRY_ATTEMPTS:
                    raise
                time.sleep(RETRY_WAIT_SECONDS)

    def run(self, batch_size=50):
        logger.info(f"📥 Pipeline started: Kafka -> TimescaleDB (Batch size: {batch_size})")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self._flush()
                    continue
                if msg.error():
                    logger.error(f"Kafka Error: {msg.error()}")
                    break

                self._process_message(msg, batch_size)
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
            self._flush()
        finally:
            self._cleanup()

    def _process_message(self, msg, batch_size):
        try:
            record = self.transformer.deserialize(msg.value())
            self.batch.append(self.transformer.to_sql_tuple(record))
            if len(self.batch) >= batch_size:
                self._flush()
        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    def _flush(self):
        if not self.batch:
            return
        if self.sink.write_batch(self.batch):
            self.consumer.commit(asynchronous=False)
            logger.info(f"📦 Flushed {len(self.batch)} records")
            self.batch = []

    def _cleanup(self):
        self.consumer.close()
        self.sink.close()


if __name__ == "__main__":  # pragma: no cover
    TelemetryConsumer().run()
