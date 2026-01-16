# Project Specification: Industrial IoT Real-Time Monitoring POC

## 1. Project Overview & Objectives
**Goal:** Build a robust, scalable End-to-End Data Engineering pipeline to monitor industrial machine health in real-time.
**Core Challenge:** Simulate a realistic industrial environment ("Chaos Engineering") with network latency, sensor drift, and critical failures, and process this data with low latency on constrained hardware.

**Key Technologies:** Python, Kafka (w/ Kafka Connect & Schema Registry), Spark Streaming, PostgreSQL (TimescaleDB), Grafana, Docker.

---

## 2. Architecture Layers

### 🏗️ Layer I: Infrastructure & Environment (The Foundation)
*Objective: Orchestrate a stable environment within strict hardware limits (8GB RAM).*

* **Container Orchestration:** Docker & Docker Compose.
* **Service Mesh:**
    * **Message Broker:** Apache Kafka (Kraft mode for lower footprint) or **Redpanda** (C++ alternative) if RAM usage becomes critical.
    * **Schema Governance:** Schema Registry (Apuric or Confluent) to enforce data contracts (Avro/Protobuf).
    * **Processing Engine:** Apache Spark (Structured Streaming).
    * **Storage:** MinIO (S3 compatible) & PostgreSQL.
    * **Observability:** Grafana & Prometheus (optional for pipeline metrics).
* **Hardware Constraints Strategy ("The 8GB Limit"):**
    * Strict `mem_limit` on all containers.
    * **Contingency Plan:** If Kafka/Spark combination causes OOM (Out Of Memory), swap Kafka for Redpanda and/or Spark for a lightweight Python consumer (Faust) for the specific processing logic.

### 🤖 Layer II: Data Modeling & Simulation (The Source)
*Objective: Generate realistic, high-frequency industrial telemetry.*

* **Data Format:** **Avro** (preferred) or Protobuf.
    * *Why:* Binary formats are more efficient than JSON and enforce schema evolution handling.
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
    * Integrates with the Simulator.
    * Serializes data to Avro.
    * Handles "At Least Once" delivery semantics.
* **Ingestion Platform:** Apache Kafka.
    * Topic partitioning strategy based on `machine_id` to guarantee ordering.
* **Connectivity (The "No-Code" Approach):**
    * **Kafka Connect:** utilized for standard data movements to avoid writing boilerplate consumer code.
        * *Source:* (Optional) If ingesting from static files.
        * *Sink:* Moving data to "Cold Storage" and "Serving Layer".

### 🧹 Layer IV: Processing & Storage (The "Brain")
*Objective: Transform raw streams into actionable insights.*

* **Hot Path (Real-time):**
    * **Apache Spark Structured Streaming:**
        * Read from Kafka.
        * **Watermarking:** Handle late-arriving data (due to simulated network lag).
        * **Aggregations:** Calculate 1-minute and 5-minute Rolling Averages (sliding windows).
        * **Alerting Logic:** Detect if `temp > threshold` for `n` consecutive events.
* **Cold Path (Archival):**
    * **Kafka Connect S3 Sink:** Automatically dumps raw Avro topics to MinIO (Parquet format) for future historical analysis.
* **Serving Storage:**
    * **PostgreSQL with TimescaleDB Extension:**
        * Optimized for time-series insertion and querying.
        * Automatic partitioning (hypertables).
    * **Kafka Connect JDBC Sink:** Automatically syncs processed topics from Spark to Postgres.

### 📊 Layer V: Presentation & Consumption (Value)
*Objective: Visualize health and monitor system performance.*

* **Business Dashboard (Grafana):**
    * Real-time gauges for Machine Temperature/Pressure.
    * Time-series graphs showing "Raw" vs "Rolling Average".
    * Alert history table.
* **Pipeline Health Dashboard (Ops):**
    * **Kafka Consumer Lag:** Monitor if the processing layer is keeping up with the ingestion rate.
    * Container Health (CPU/RAM usage).

---

## 3. Implementation Roadmap (Sprints)

### 🏃 Sprint 1: The "Walking Skeleton" (MVP)
*Goal: End-to-End connectivity without complex processing.*
1.  Setup `docker-compose` (Kafka, Schema Registry, Postgres/Timescale, Grafana).
2.  Develop basic Python Producer (Simulator) sending Avro events.
3.  **Configure Kafka Connect (JDBC Sink)** to move data directly from Kafka to Postgres.
4.  Visualize raw data in Grafana.
* *Success Criteria:* Simulator starts -> Data flows through Kafka -> Data appears in SQL table -> Graph updates. No custom Consumer code written.

### 🧠 Sprint 2: Stream Processing (The "Meat")
*Goal: Implement business logic.*
1.  Deploy Apache Spark container.
2.  Implement Spark Structured Streaming job:
    * Read Avro.
    * Calculate Rolling Averages.
    * Write results to a new Kafka topic (`processed_telemetry`).
3.  Re-route Kafka Connect to dump the `processed_telemetry` topic to Postgres.

### 🛡️ Sprint 3: Reliability & Storage Optimization
*Goal: Make it production-ready.*
1.  Enable **TimescaleDB** features (hypertables) in Postgres.
2.  Configure **Kafka Connect S3 Sink** for MinIO archival.
3.  Implement "Chaos" scenarios in the simulator (drifts/spikes) and verify Spark's watermarking handling.

### 🚀 Sprint 4: Monitoring & Final Polish
*Goal: Observability.*
1.  Add Pipeline Monitoring (Kafka Lag) in Grafana.
2.  Final code cleanup and documentation.
3.  Presentation preparation.

---

## 4. Tech Stack Summary
| Component | Technology | Role |
| :--- | :--- | :--- |
| **Language** | Python 3.9+ | Simulation, Scripting |
| **Broker** | Kafka (or Redpanda) | Message Bus |
| **Ingestion** | Kafka Connect | ELT/Data Movement |
| **Processing** | Spark Streaming | Windowing, Aggregation |
| **Schema** | Avro + Registry | Data Governance |
| **Database** | Postgres (TimescaleDB) | Serving Layer |
| **Storage** | MinIO | Data Lake (Cold Store) |
| **Viz** | Grafana | Dashboards |