# Project Specification: Industrial IoT Real-Time Monitoring POC

## 1. Project Overview & Objectives
**Goal:** Build a robust, scalable End-to-End Data Engineering pipeline to monitor industrial machine health in real-time.
**Core Challenge:** Simulate a realistic industrial environment ("Chaos Engineering") with network latency, sensor drift, and critical failures, and process this data with low latency on constrained hardware.

**Key Technologies:** Python, Kafka, Bytewax (or Faust), PostgreSQL (TimescaleDB), Grafana, Docker.

---

## 2. Architecture Layers

### 🏗️ Layer I: Infrastructure & Environment (The Foundation)
*Objective: Orchestrate a stable environment within strict hardware limits (8GB RAM).*

* **Container Orchestration:** Docker & Docker Compose.
* **Message Broker:** Apache Kafka (Kraft mode).
* **Processing Engine:** Lightweight Python-based Steam Processing (e.g., **Bytewax** or custom **confluent-kafka** consumers).
* **Storage:** PostgreSQL (TimescaleDB) & local storage/MinIO for data archival.
* **Observability:** Grafana.

* **Hardware Constraints Strategy ("The 8GB Limit"):**
    * **Lean Stack:** Removed Spark and Kafka Connect to save ~3GB of RAM.
    * **Direct Sinks:** Processing engine writes directly to the Database and Cold Storage.
    * **No Schema Registry:** Use static Avro schema files within the application code to avoid running a dedicated registry container.

### 🤖 Layer II: Data Modeling & Simulation (The Source)
*Objective: Generate realistic, high-frequency industrial telemetry.*

* **Data Format:** **Avro** (Binary, efficient, schema-enforced).
* **Simulation Logic (Python):**
    * **Physics Engine:** Simulate temperature rise based on "engine load".
    * **Chaos Engineering:** Random injection of:
        * *Spikes:* Sudden value jumps.
        * *Drift:* Gradual sensor de-calibration.
        * *Packet Loss:* Missing data points.
* **Attributes:** `machine_id`, `timestamp` (precision: ms), `sensor_type` (vibration, temp, pressure), `value`, `status_code`.

### 📡 Layer III: Ingestion Layer (Transport)
*Objective: Reliable data transport and decoupling.*

* **Producer (Custom Python):**
    * Serializes data to Avro using local schemas.
    * Produces to Kafka topics partitioned by `machine_id` to guarantee ordering.

### 🧹 Layer IV: Processing & Storage (The "Brain")
*Objective: Transform raw streams into actionable insights with low memory footprint.*

* **Stream Processing (Python):**
    * **Logic:** Read from Kafka, deserialize Avro.
    * **Watermarking/Windowing:** Handle late data and calculate rolling averages (1m, 5m).
    * **Alerting:** Detect consecutive threshold breaches.
* **Storage Strategy:**
    * **Serving Layer:** Direct writes to **PostgreSQL (TimescaleDB)** using async drivers (e.g., `asyncpg`).
    * **Cold Storage:** Monthly/Daily archival of raw events to Parquet/local files.

### 📊 Layer V: Presentation & Consumption (Value)
*Objective: Visualize health and monitor system performance.*

* **Dashboard (Grafana):**
    * Real-time gauges for Machine health.
    * Time-series graphs comparing raw vs. processed signals.
    * Alert history tracking.

---

## 3. Implementation Roadmap (Sprints)

### 🏃 Sprint 1: The "Walking Skeleton" (MVP)
*Goal: End-to-End connectivity with direct ingestion.*
1.  Setup `docker-compose` (Kafka, Postgres/Timescale, Grafana).
2.  Develop Python Producer (Simulator) sending Avro events to Kafka.
3.  Develop a simple Python Consumer to move data from Kafka to Postgres.
4.  Visualize raw data in Grafana.

### 🧠 Sprint 2: Stream Processing & Analytics
*Goal: Implement stateful processing.*
1.  Integrate a stream processing library (e.g., Bytewax).
2.  Implement rolling averages and windowed aggregations.
3.  Write processed telemetry to a separate "clean" Kafka topic or directly to db.

### 🛡️ Sprint 3: Reliability & Chaos
*Goal: Resilience testing.*
1.  Enable TimescaleDB hypertables and retention policies.
2.  Trigger "Chaos" events in the simulator.
3.  Verify system stability under simulated network lag and high load.

### 🚀 Sprint 4: Final Polish
1.  Grafana dashboard refinement.
2.  Performance tuning (memory limits, batch sizes).
3.  Detailed documentation.

---

## 4. Tech Stack Summary
| Component | Technology | Role |
| :--- | :--- | :--- |
| **Language** | Python 3.13 | Simulation, Processing |
| **Broker** | Kafka | Message Bus |
| **Processing** | Bytewax / Faust | Windowing & Aggregation |
| **Schema** | Avro (Static) | Data Serialization |
| **Database** | Postgres (TimescaleDB) | Serving Layer |
| **Viz** | Grafana | Dashboards |