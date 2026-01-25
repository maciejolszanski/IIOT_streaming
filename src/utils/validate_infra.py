import subprocess
from typing import Any

import psycopg2
import requests
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def validate_docker_health(container_name):
    """Checks if a Docker container is reporting a 'healthy' status."""
    print(f"Checking Docker health for {container_name}...")
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{json .State.Health.Status}}", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"⚠️  Could not check health for {container_name} (container not found or no healthcheck defined).")
            return False

        status = result.stdout.strip().replace('"', "")
        if status == "healthy":
            print(f"✅ {container_name} is healthy.")
            return True
        else:
            print(f"❌ {container_name} is {status}!")
            return False
    except FileNotFoundError:
        print("❌ Docker command not found. Is Docker installed?")
        return False


def validate_kafka():
    """Validates Kafka connectivity by listing topics."""
    print("Checking Kafka (localhost:9094)...")
    conf: dict[str, Any] = {"bootstrap.servers": "localhost:9094"}
    try:
        admin_client = AdminClient(conf)
        admin_client.list_topics(timeout=10)
        print("✅ Kafka is up and reachable.")
        return True
    except Exception as e:
        print(f"❌ Kafka validation failed: {e}")
        return False


def validate_postgres():
    """Validates TimescaleDB connectivity."""
    import os
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    
    print(f"Checking TimescaleDB ({db_host}:5432)...")
    try:
        conn = psycopg2.connect(
            dbname=db_name, user=db_user, password=db_pass, host=db_host, port="5432"
        )
        conn.close()
        print("✅ TimescaleDB is up and reachable.")
        return True
    except Exception as e:
        print(f"❌ TimescaleDB validation failed: {e}")
        return False


def validate_grafana():
    """Validates Grafana availability via health API."""
    print("Checking Grafana (localhost:3000)...")
    try:
        response = requests.get("http://localhost:3000/api/health")
        if response.status_code == 200:
            print("✅ Grafana is up and reachable.")
            return True
        else:
            print(f"❌ Grafana returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Grafana validation failed: {e}")
        return False


if __name__ == "__main__":  # pragma: no cover
    print("--- Infrastructure Validation ---")

    # 1. Docker Level Checks
    print("\n[Docker Container Health]")
    services = ["kafka", "timescaledb", "grafana"]
    docker_health_results = [validate_docker_health(s) for s in services]
    container_health_ok = all(docker_health_results)

    # 2. Application Level Checks
    print("\n[Application Connectivity]")
    kafka_conn = validate_kafka()
    pg_conn = validate_postgres()
    grafana_conn = validate_grafana()
    app_conn_ok = all([kafka_conn, pg_conn, grafana_conn])

    if container_health_ok and app_conn_ok:
        print("\n🚀 All infrastructure services are validated and healthy!")
    else:
        print("\n⚠️ Some services failed validation. Please check the logs above.")
