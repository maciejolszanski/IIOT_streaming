---
description: Validate the IIoT infrastructure (Kafka, Postgres, Grafana)
---
1. Ensure Docker Desktop is running.
2. Run the docker-compose services:
   // turbo
   `docker-compose up -d`
3. Wait for services to be healthy (check with `docker ps`).
4. Install validation dependencies:
   // turbo
   `pip install -r requirements.txt`
5. Run the validation script:
   // turbo
   `python src/utils/validate_infra.py`
