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

### 4. Testing
Run the unit test suite:
```powershell
pytest
```
Running `pytest` will automatically:
1. Execute all tests in the `tests/` directory.
2. Generate a terminal coverage report.
3. Generate a detailed HTML coverage report in the `htmlcov/` directory.

### 5. Code Quality & CI
The project uses `ruff` for linting/formatting and `mypy` for static type checking.

#### Linting & Formatting
```powershell
ruff check .      # Check for errors
ruff format .     # Format code
```

#### Type Checking
```powershell
mypy src
```

#### Pre-commit Hooks
To ensure high code quality, we use `pre-commit`. Install the hooks locally:
```powershell
pre-commit install
```
Hooks will now run automatically on every `git commit`.

#### Continuous Integration
Every push and Pull Request to `master` triggers a GitHub Actions workflow which:
- Runs `ruff` checks.
- Runs `mypy` type checking.
- Runs `pytest` and uploads coverage data.

### 5. Running the Components
#### Start the Simulator (Producer)
```powershell
python src/producers/simulator.py
```

#### Start the Telemetry Consumer (Sink)
```powershell
python src/consumers/telemetry_consumer.py
```

## 🛠️ Components

### 🔄 Telemetry Simulator
The simulator (`src/producers/simulator.py`) generates realistic industrial sensor data:
- **Physics Engine**: Correlates load with temperature and vibration.
- **Chaos Injection**: Randomized temperature spikes and sensor drift.
- **Resilience**: Automatic retry logic for Kafka connectivity.
- **Performance**: Uses binary Avro with snappy compression and batching.

### 📥 Telemetry Consumer
The consumer (`src/consumers/telemetry_consumer.py`) orchestrates the data flow from Kafka to TimescaleDB:
- **Avro Deserialization**: Efficiently decodes binary payloads using schemas.
- **Batch Processing**: Groups records for optimized high-throughput database inserts.
- **TimescaleDB Integration**: Automatically manages hypertable creation and indexing for time-series performance.
- **Reliability**: Implements manual Kafka commits only after successful database persistence.

#### Configuration (Environment Variables)
| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `DB_HOST` | `localhost` | TimescaleDB host |
| `DB_NAME` | `iiot_db` | Database name |
| `DB_USER` | `iiot_user` | Database user |
| `LOG_LEVEL` | `INFO` | Logger verbosity |
| `BATCH_SIZE` | `50` | Number of records per DB insert |

## 📊 Tech Stack
- **Broker:** Kafka (KRaft mode)
- **Database:** TimescaleDB (PostgreSQL plugin)
- **Visualization:** Grafana
- **Processing:** Python (Confluent-Kafka, Psycopg2, FastAvro)
- **Serialization:** Avro
