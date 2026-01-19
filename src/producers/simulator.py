import io
import json
import logging
import os
import random
import time

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_NAME = os.getenv("TOPIC_NAME", "telemetry_raw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/telemetry.avsc")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MACHINE_IDS = os.getenv("MACHINE_IDS", "M001,M002,M003").split(",")
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "5"))
RETRY_WAIT_SECONDS = int(os.getenv("RETRY_WAIT_SECONDS", "5"))


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class TelemetrySimulator:
    def __init__(self):
        # Load and parse Avro schema
        with open(SCHEMA_PATH, "r") as f:
            self.schema_dict = json.load(f)
            self.schema = parse_schema(self.schema_dict)

        self.producer = self._setup_producer()
        self.machines = [m.strip() for m in MACHINE_IDS]
        self.state = {m: {"temp_base": 65.0, "vibration_base": 0.05, "load": 0.5} for m in self.machines}

    def _setup_producer(self):
        """Initializes the Kafka producer with retry logic."""
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'telemetry-simulator',
            'linger.ms': 100,
            'compression.type': 'snappy'
        }

        for attempt in range(1, RETRY_ATTEMPTS + 1):  # pragma: no cover
            try:
                producer = Producer(producer_conf)
                # Test connectivity
                producer.list_topics(timeout=2)
                logger.info(f"Successfully connected to Kafka on attempt {attempt}")
                return producer
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt} failed: {e}")
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    logger.error("Could not connect to Kafka after multiple attempts. Exiting.")
                    raise

    def _generate_telemetry(self, machine_id):
        # Physics Engine: Temp correlates with load
        load = self.state[machine_id]["load"]
        load = max(0.1, min(1.0, load + random.uniform(-0.05, 0.05)))
        self.state[machine_id]["load"] = load

        temp = self.state[machine_id]["temp_base"] + (load * 30.0) + random.uniform(-2, 2)
        vibe = self.state[machine_id]["vibration_base"] + (load * 0.1) + random.uniform(-0.01, 0.01)

        if random.random() < 0.01: # 1% chance for a spike
            temp += 20.0
            logger.warning(f"🔥 CHAOS: Temperature spike detected for machine {machine_id}!")

        sensors = [
            ("temperature", temp),
            ("vibration", vibe),
            ("pressure", 100.0 + random.uniform(-5, 5))
        ]

        messages = []
        for s_type, s_val in sensors:
            msg = {
                "machine_id": machine_id,
                "timestamp": int(time.time() * 1000),
                "sensor_type": s_type,
                "value": float(s_val),
                "status_code": 0
            }
            messages.append(msg)

        return messages

    def _serialize_avro(self, record):
        bytes_io = io.BytesIO()
        schemaless_writer(bytes_io, self.schema, record)
        return bytes_io.getvalue()

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            logger.debug(f"✅ Delivered {msg.key().decode('utf-8')} to {msg.topic()} [{msg.partition()}]")

    def run(self):
        logger.info(f"🚀 Simulator started. Producing to {TOPIC_NAME}...")
        try:
            while True:
                for m_id in self.machines:
                    batch = self._generate_telemetry(m_id)
                    logger.debug(f"Generated batch of {len(batch)} sensors for {m_id}")
                    for record in batch:
                        avro_bytes = self._serialize_avro(record)
                        logger.debug(f"Producing {record['sensor_type']}={record['value']:.2f} for {m_id}")
                        self.producer.produce(
                            TOPIC_NAME,
                            value=avro_bytes,
                            key=m_id,
                            on_delivery=self.delivery_report
                        )

                # Call poll regularly to serve delivery reports
                self.producer.poll(0)
                time.sleep(1.0) # Send batch every second
        except KeyboardInterrupt:
            logger.info("🛑 Simulator stopped. Flushing remaining messages...")
            self.producer.flush(timeout=10)

if __name__ == "__main__":  # pragma: no cover
    simulator = TelemetrySimulator()
    simulator.run()
