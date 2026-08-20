# Import libraries
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

CSV_PATH = os.environ['CSV_PATH']
KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_RETRIES = os.environ['KAFKA_RETRIES']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
KAFKA_GROUP_ID = os.environ['KAFKA_GROUP_ID']
STREAM_INTERVAL_SECONDS = os.environ['STREAM_INTERVAL_SECONDS']

DATABASE_URL = os.environ['DATABASE_URL']