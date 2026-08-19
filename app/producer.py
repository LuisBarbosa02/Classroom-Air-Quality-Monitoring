# Import libraries
import logging
from .config import CSV_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_RETRIES, KAFKA_TOPIC, STREAM_INTERVAL_SECONDS
import csv
from confluent_kafka import Producer
import json
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | producer | %(message)s"
)
logger = logging.getLogger("producer")

# Reading data rows
def load_rows(csv_path=CSV_PATH):
    with open(csv_path, mode='r') as file:
        reader = csv.DictReader(file)

        for row in reader:
            yield row

# Kafka producer
def build_producer():
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "retries": int(KAFKA_RETRIES),
            "delivery.timeout.ms": 120000,
            "request.timeout.ms": 30000,
            "client.id": "classroom-air-quality-producer"
        }
    )

# Delivery notifications
def delivery_notification(err, msg):
    if err is not None:
        logger.error(
            "Kafka delivery failed for topic=%s partition=%s offset=%s: %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            err
        )
    else:
        logger.info(
            "Published timestamp=%s to topic=%s partition=%d offset=%d",
            msg.key().decode('utf-8'),
            msg.topic(),
            msg.partition(),
            msg.offset()
        )

# Send data to Apache Kafka
def publish_row(producer, row):
    # Transform data row into JSON
    data = json.dumps(row)
    
    # Giving the row a unique identifier
    key = row['timestamp'].encode('utf-8')
    
    while True:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key,
                value=data,
                on_delivery=delivery_notification
            )
            producer.poll(0)
            return
        except BufferError:
            logger.warning("Producer queue if full, waiting before retrying.")
            producer.poll(1.0)
        except KafkaException as exc:
            logger.warning("Kafka failed publishing, retrying: %s", exc)
            time.sleep(2)

# Main program
def main():
    # Display initial streaming conditions
    logger.info("Starting producer")
    logger.info("Dataset: %s", CSV_PATH)
    logger.info("Kafka: %s | topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    logger.info("Stream interval: %.2f seconds", float(STREAM_INTERVAL_SECONDS))
    
    # Load producer
    producer = build_producer()

    try:
        # Load data
        rows = load_rows()

        # Publish rows to Apache Kafka
        for i, row in enumerate(rows):
            # Publish row
            publish_row(producer, row)

            # Insert interval between the data entries
            time.sleep(float(STREAM_INTERVAL_SECONDS))

        logger.info("Dataset exhausted, flushing pending messages.")
        remaining = producer.flush(120)
        if remaining:
            raise RuntimeError(f"{remaining} message(s) were not delivered.")

        logger.info("Producer successfully completed.")

    except KeyboardInterrupt:
        logger.info('Producer interrupted by user.')

    finally:
        producer.flush(120)

# Run program
if __name__ == "__main__":
    main()