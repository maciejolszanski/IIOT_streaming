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

## 🛠️ Tech Stack
- **Broker:** Kafka (KRaft mode)
- **Database:** TimescaleDB (PostgreSQL)
- **Visualization:** Grafana
- **Processing:** Python (Bytewax / Confluent-Kafka)
- **Serialization:** Avro
