import os
import pytest

def pytest_configure(config):
    # Ensure DB env vars exist early for config import
    os.environ.setdefault('DB_NAME', 'testdb')
    os.environ.setdefault('DB_USER', 'testuser')
    os.environ.setdefault('DB_PASSWORD', 'testpass')
    os.environ.setdefault('DB_HOST', 'localhost')
    os.environ.setdefault('DB_PORT', '5432')

@pytest.fixture(autouse=True)
def set_env_db_vars(monkeypatch):
    # Allow tests to override DB env vars if needed
    monkeypatch.setenv('DB_NAME', os.environ['DB_NAME'])
    monkeypatch.setenv('DB_USER', os.environ['DB_USER'])
    monkeypatch.setenv('DB_PASSWORD', os.environ['DB_PASSWORD'])
    monkeypatch.setenv('DB_HOST', os.environ['DB_HOST'])
    monkeypatch.setenv('DB_PORT', os.environ['DB_PORT'])
    yield