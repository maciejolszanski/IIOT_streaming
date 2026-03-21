# Industrial IoT Real-Time Monitoring POC

An End-to-End Data Engineering pipeline for monitoring industrial machine health in real-time. Designed to run on resource-constrained hardware (~4GB available RAM).

## 🏗️ Project Structure

```text
IIoT/
├── .agent/              # Agent workflows
├── grafana/             # Grafana provisioning (dashboards, datasources)
├── schemas/             # Avro schema definitions
├── src/
│   ├── consumers/       # Stream processing consumers
│   ├── producers/       # Data simulation producers
│   └── utils/           # Utility scripts (validation, etc.)
├── docker-compose.yml   # Infrastructure (Kafka, TimescaleDB, Grafana)
├── pyproject.toml       # Python project configuration & dependencies
├── uv.lock              # Dependency lockfile
└── specification.md     # Detailed project specification
```

## 🚀 Getting Started

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (must be running)
- [uv](https://docs.astral.sh/uv/) (Dependency manager)
- Python 3.13+

### 2. Infrastructure & Application Setup
Start the entire monitoring stack (Kafka, TimescaleDB, Grafana, Producer, and Consumer) using Docker Compose:
```bash
docker-compose up -d --build
```
This command builds the custom images for the producer and consumer and starts them along with the infrastructure.

Alternatively, you can start only the core infrastructure first:
```bash
docker-compose up -d kafka timescaledb grafana
```


### 3. Validation
Install dependencies and verify the environment:
```powershell
uv sync --all-groups
uv run python src/utils/validate_infra.py
```

### 4. Testing
Run the unit test suite:
```powershell
uv run pytest
```
Running `uv run pytest` will automatically:
1. Execute all tests in the `tests/` directory.
2. Generate a terminal coverage report.
3. Generate a detailed HTML coverage report in the `htmlcov/` directory.

### 5. Code Quality & CI
The project uses `ruff` for linting/formatting.

#### Linting & Formatting
```powershell
uv run ruff check .      # Check for errors
uv run ruff format .     # Format code
```


#### Pre-commit Hooks
To ensure high code quality, we use `pre-commit`. Install the hooks locally:
```powershell
uv run pre-commit install
```
Hooks will now run automatically on every `git commit`.

#### Continuous Integration
Every push and Pull Request to `master` triggers a GitHub Actions workflow which:
- Sets up `uv`.
- Runs `ruff` checks via `uv run`.
- Runs `pytest` via `uv run` and uploads coverage data.

### 6. Running the Components
#### A. Using Docker (Recommended)
If you used `docker-compose up -d`, both components are already running. You can view their logs with:
```bash
docker-compose logs -f producer
docker-compose logs -f consumer
```

#### B. Using Local Python
If you prefer running them locally:
##### Start the Simulator (Producer)
```powershell
uv run python src/producers/simulator.py
```

##### Start the Telemetry Consumer (Sink)
```powershell
uv run python src/consumers/telemetry_consumer.py
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

### 📊 Monitoring Dashboard
The project includes a pre-configured Grafana dashboard for real-time monitoring:
- **Access**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`)
- **Provisioning**: Automatically connects to TimescaleDB and loads the "Industrial IoT Monitor" dashboard.
- **Features**: Real-time time-series plots for sensor telemetry and health gauges for machine status.

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
