import os
import time
import io
import json
import logging
from fastavro import parse_schema, schemaless_reader
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_NAME = os.getenv("TOPIC_NAME", "telemetry_raw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/telemetry.avsc")
GROUP_ID = os.getenv("GROUP_ID", "telemetry-consumer-group")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "5"))
RETRY_WAIT_SECONDS = int(os.getenv("RETRY_WAIT_SECONDS", "5"))

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class TelemetryConsumer:
    def __init__(self):
        # Load and parse Avro schema
        if not os.path.exists(SCHEMA_PATH):
            logger.error(f"Schema file not found at {SCHEMA_PATH}")
            raise FileNotFoundError(f"Missing schema at {SCHEMA_PATH}")

        with open(SCHEMA_PATH, "r") as f:
            self.schema_dict = json.load(f)
            self.schema = parse_schema(self.schema_dict)

        self.consumer = self._setup_consumer()

    def _setup_consumer(self):
        """Initializes the Kafka consumer with resilient parameters."""
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 45000, # Resilience for network blips
            'max.poll.interval.ms': 300000 # Allow 5 mins for processing
        }
        
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                consumer = Consumer(conf)
                # Test connectivity
                consumer.list_topics(timeout=2)
                logger.info(f"Successfully connected to Kafka on attempt {attempt}")
                consumer.subscribe([TOPIC_NAME])
                return consumer
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt} failed: {e}")
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    logger.error("Could not connect to Kafka after multiple attempts. Exiting.")
                    raise

    def _deserialize_avro(self, payload):
        """Deserializes binary Avro payload using the local schema."""
        bytes_io = io.BytesIO(payload)
        return schemaless_reader(bytes_io, self.schema)

    def run(self):
        logger.info(f"📥 Consumer started. Listening on {TOPIC_NAME}...")
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                try:
                    # Deserialize binary Avro
                    record = self._deserialize_avro(msg.value())
                    
                    # Log with a bit more structure
                    m_id = record.get('machine_id', 'Unknown')
                    s_type = record.get('sensor_type', 'Unknown')
                    val = record.get('value', 0.0)
                    
                    logger.debug(f"Partition: {msg.partition()} | Offset: {msg.offset()}")
                    logger.info(f"✅ [{m_id}] {s_type}: {val:.2f}")
                except Exception as e:
                    logger.error(f"Failed to process message from {msg.topic()}: {e}")

        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped.")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = TelemetryConsumer()
    consumer.run()
