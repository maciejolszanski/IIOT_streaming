<<<<<<< HEAD
import os
import sys
from unittest.mock import MagicMock, patch
=======
import pytest
from unittest.mock import MagicMock, patch
import os
import sys
>>>>>>> origin/master

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

<<<<<<< HEAD
from src.utils.validate_infra import validate_docker_health, validate_grafana, validate_kafka, validate_postgres

=======
from src.utils.validate_infra import validate_docker_health, validate_kafka, validate_postgres, validate_grafana
>>>>>>> origin/master

def test_validate_docker_health_success():
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout='"healthy"')
        assert validate_docker_health("test") is True

def test_validate_docker_health_unhealthy():
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout='"starting"')
        assert validate_docker_health("test") is False

def test_validate_docker_health_error():
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=1)
        assert validate_docker_health("test") is False

def test_validate_docker_health_no_docker():
    with patch('subprocess.run', side_effect=FileNotFoundError):
        assert validate_docker_health("test") is False

def test_validate_kafka_success():
<<<<<<< HEAD
    with patch('src.utils.validate_infra.AdminClient'):
=======
    with patch('src.utils.validate_infra.AdminClient') as mock_admin:
>>>>>>> origin/master
        assert validate_kafka() is True

def test_validate_kafka_failure():
    with patch('src.utils.validate_infra.AdminClient', side_effect=Exception("Fail")):
        assert validate_kafka() is False

def test_validate_postgres_success():
<<<<<<< HEAD
    with patch('psycopg2.connect'):
=======
    with patch('psycopg2.connect') as mock_conn:
>>>>>>> origin/master
        assert validate_postgres() is True

def test_validate_postgres_failure():
    with patch('psycopg2.connect', side_effect=Exception("Fail")):
        assert validate_postgres() is False

def test_validate_grafana_success():
    with patch('requests.get') as mock_get:
        mock_get.return_value = MagicMock(status_code=200)
        assert validate_grafana() is True

def test_validate_grafana_failure_code():
    with patch('requests.get') as mock_get:
        mock_get.return_value = MagicMock(status_code=500)
        assert validate_grafana() is False

def test_validate_grafana_exception():
    with patch('requests.get', side_effect=Exception("Fail")):
        assert validate_grafana() is False
