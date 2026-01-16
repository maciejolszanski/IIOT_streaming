import socket
import time
import requests
import psycopg2
from confluent_kafka.admin import AdminClient

def check_port(host, port, timeout=5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout):
        return False

def validate_kafka():
    print("Checking Kafka (localhost:9094)...")
    conf = {'bootstrap.servers': 'localhost:9094'}
    try:
        admin_client = AdminClient(conf)
        metadata = admin_client.list_topics(timeout=10)
        print("✅ Kafka is up and reachable.")
        return True
    except Exception as e:
        print(f"❌ Kafka validation failed: {e}")
        return False

def validate_postgres():
    print("Checking TimescaleDB (localhost:5432)...")
    try:
        conn = psycopg2.connect(
            dbname="iiot_db",
            user="iiot_user",
            password="iiot_password",
            host="localhost",
            port="5432"
        )
        conn.close()
        print("✅ TimescaleDB is up and reachable.")
        return True
    except Exception as e:
        print(f"❌ TimescaleDB validation failed: {e}")
        return False

def validate_grafana():
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

import subprocess
import json

def validate_docker_health(container_name):
    print(f"Checking Docker health for {container_name}...")
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{json .State.Health.Status}}", container_name],
            capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
             # Fallback for containers without explicit healthchecks or if container not found
            print(f"⚠️  Could not check health for {container_name} (might not be running or no healthcheck defined).")
            return False
            
        status = result.stdout.strip().replace('"', '')
        if status == "healthy":
            print(f"✅ {container_name} is healthy.")
            return True
        else:
            print(f"❌ {container_name} is {status}!")
            return False
    except FileNotFoundError:
        print("❌ Docker command not found. Is Docker installed?")
        return False

if __name__ == "__main__":
    print("--- Infrastructure Validation ---")
    
    # 1. Docker Level Checks
    print("\n[Docker Container Health]")
    kafka_health = validate_docker_health("kafka")
    pg_health = validate_docker_health("timescaledb")
    grafana_health = validate_docker_health("grafana")
    
    container_health_ok = kafka_health and pg_health and grafana_health

    # 2. Application Level Checks
    print("\n[Application Connectivity]")
    kafka_conn = validate_kafka()
    pg_conn = validate_postgres()
    grafana_conn = validate_grafana()
    
    app_conn_ok = kafka_conn and pg_conn and grafana_conn

    if container_health_ok and app_conn_ok:
        print("\n🚀 All infrastructure services are validated and healthy!")
    else:
        print("\n⚠️ Some services failed validation. Please check the logs.")
