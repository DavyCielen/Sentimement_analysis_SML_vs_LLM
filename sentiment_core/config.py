"""
Configuration and logging setup for sentiment_core.
"""
import os
import logging

# Configure logging for all runners
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# Database connection parameters (must be provided via environment variables)
db_params = {
    'dbname': os.environ['DB_NAME'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'host': os.environ['DB_HOST'],
    'port': os.environ.get('DB_PORT', '5432'),
}

# Batch size for processing (optional, defaults to 5)
try:
    batch_size = int(os.getenv('BATCH_SIZE', '5'))
except ValueError:
    batch_size = 5