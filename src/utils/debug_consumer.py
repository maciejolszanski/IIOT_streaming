import os
import io
import json
from fastavro import parse_schema, schemaless_reader
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_NAME = os.getenv("TOPIC_NAME", "telemetry_raw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/telemetry.avsc")

class DebugConsumer:
    def __init__(self):
        # Load and parse Avro schema
        with open(SCHEMA_PATH, "r") as f:
            self.schema_dict = json.load(f)
            self.schema = parse_schema(self.schema_dict)

        # Kafka Consumer Configuration
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'debug-consumer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([TOPIC_NAME])

    def _deserialize_avro(self, avro_bytes):
        bytes_io = io.BytesIO(avro_bytes)
        return schemaless_reader(bytes_io, self.schema)

    def run(self, max_messages=10):
        print(f"🔍 Debug Consumer started. Listening on {TOPIC_NAME}...")
        count = 0
        try:
            while count < max_messages:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Error: {msg.error()}")
                        break

                # Deserialize
                try:
                    record = self._deserialize_avro(msg.value())
                    print(f"✅ Received ({msg.key().decode() if msg.key() else 'NoKey'}): {record}")
                    count += 1
                except Exception as e:
                    print(f"❌ Deserialization failed: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = DebugConsumer()
    consumer.run()
