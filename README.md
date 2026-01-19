# Industrial IoT Real-Time Monitoring POC

An End-to-End Data Engineering pipeline for monitoring industrial machine health in real-time. Designed to run on resource-constrained hardware (~4GB available RAM).

## 🏗️ Project Structure

```text
IIoT/
├── .agent/              # Agent workflows
├── schemas/             # Avro schema definitions
├── src/
│   ├── consumers/       # Stream processing consumers
│   ├── producers/       # Data simulation producers
│   └── utils/           # Utility scripts (validation, etc.)
├── docker-compose.yml   # Infrastructure (Kafka, TimescaleDB, Grafana)
├── requirements.txt     # Python dependencies
└── specification.md     # Detailed project specification
```

## 🚀 Getting Started

### 1. Prerequisites
- Docker Desktop
- Python 3.13

### 2. Infrastructure Setup
Start the core services (Kafka, TimescaleDB, Grafana):
```powershell
docker-compose up -d
```

### 3. Validation
Install dependencies and verify the environment:
```powershell
pip install -r requirements.txt
python src/utils/validate_infra.py
```

### 4. Running the Simulator
Produce simulated telemetry data to Kafka:
```powershell
# Basic usage
python src/producers/simulator.py

# With debug logging
$env:LOG_LEVEL="DEBUG"; python src/producers/simulator.py

# Simulating specific machines
$env:MACHINE_IDS="CNC-01,CNC-02"; python src/producers/simulator.py
```

## 🛠️ Components

### 🔄 Telemetry Simulator
The simulator (`src/producers/simulator.py`) generates realistic industrial sensor data:
- **Physics Engine**: Correlates load with temperature and vibration.
- **Chaos Injection**: Randomized temperature spikes and sensor drift.
- **Resilience**: Automatic retry logic for Kafka connectivity.
- **Performance**: Uses binary Avro with snappy compression and batching.

#### Configuration (Environment Variables)
| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `LOG_LEVEL` | `INFO` | Logger verbosity (DEBUG, INFO, WARNING, ERROR) |
| `MACHINE_IDS` | `M001,M002,M003` | Comma-separated list of machine identifiers |
| `RETRY_ATTEMPTS` | `5` | Kafka connection retry count |

## 📊 Tech Stack
- **Broker:** Kafka (KRaft mode)
- **Database:** TimescaleDB (PostgreSQL)
- **Visualization:** Grafana
- **Processing:** Python (Bytewax / Confluent-Kafka)
- **Serialization:** Avro
