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

if __name__ == "__main__":
    print("--- Infrastructure Validation ---")
    kafka_ok = validate_kafka()
    pg_ok = validate_postgres()
    grafana_ok = validate_grafana()

    if kafka_ok and pg_ok and grafana_ok:
        print("\n🚀 All infrastructure services are validated!")
    else:
        print("\n⚠️ Some services failed validation. Ensure docker-compose is running.")
