import os
import time
import random
import io
import json
from datetime import datetime
from fastavro import parse_schema, schemaless_writer
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_NAME = os.getenv("TOPIC_NAME", "telemetry_raw")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/telemetry.avsc")

class TelemetrySimulator:
    def __init__(self):
        # Load and parse Avro schema
        with open(SCHEMA_PATH, "r") as f:
            self.schema_dict = json.load(f)
            self.schema = parse_schema(self.schema_dict)

        # Kafka Producer Configuration
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'telemetry-simulator'
        }
        self.producer = Producer(producer_conf)

        # Simulation State
        self.machines = ["M001", "M002", "M003"]
        self.state = {m: {"temp_base": 65.0, "vibration_base": 0.05, "load": 0.5} for m in self.machines}

    def _generate_telemetry(self, machine_id):
        # Physics Engine: Temp correlates with load
        load = self.state[machine_id]["load"]
        # Add some random walk to load
        load = max(0.1, min(1.0, load + random.uniform(-0.05, 0.05)))
        self.state[machine_id]["load"] = load

        # Temperature simulation
        temp = self.state[machine_id]["temp_base"] + (load * 30.0) + random.uniform(-2, 2)
        
        # Vibration simulation
        vibe = self.state[machine_id]["vibration_base"] + (load * 0.1) + random.uniform(-0.01, 0.01)

        # Random Chaos: Spikes
        if random.random() < 0.01: # 1% chance for a spike
            temp += 20.0
            print(f"🔥 CHAOS: Temperature spike detected for machine {machine_id}!")

        # Sensors to emit
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
            print(f"❌ Message delivery failed: {err}")
        else:
            # Note: Binary Avro is not printable, so we don't print the value
            pass

    def run(self):
        print(f"🚀 Simulator started. Producing to {TOPIC_NAME}...")
        try:
            while True:
                for m_id in self.machines:
                    batch = self._generate_telemetry(m_id)
                    for record in batch:
                        avro_bytes = self._serialize_avro(record)
                        self.producer.produce(
                            TOPIC_NAME, 
                            value=avro_bytes, 
                            key=m_id, 
                            on_delivery=self.delivery_report
                        )
                
                self.producer.flush()
                time.sleep(1.0) # Send batch every second
        except KeyboardInterrupt:
            print("🛑 Simulator stopped.")

if __name__ == "__main__":
    simulator = TelemetrySimulator()
    simulator.run()
