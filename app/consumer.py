# Import libraries
import logging
import json
import time

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from confluent_kafka import Consumer, KafkaException

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC
)

from .database import SessionLocal, access_table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | consumer | %(message)s"
)
logger = logging.getLogger("consumer")

# Insert into PostgreSQL
INSERT_SQL = text(
    """
    INSERT INTO classroom_air_quality (
    event_timestamp,
    school_period,
    student_count_estimated,
    co2_ppm,
    pm25_ugm3,
    temperature_c,
    humidity_pct,
    robot_x_pos,
    robot_y_pos,
    ventilation_decision,
    air_quality_label,
    raw_data
    )
    VALUES (
    :event_timestamp,
    :school_period,
    :student_count_estimated,
    :co2_ppm,
    :pm25_ugm3,
    :temperature_c,
    :humidity_pct,
    :robot_x_pos,
    :robot_y_pos,
    :ventilation_decision,
    :air_quality_label,
    CAST(:raw_data as JSONB)
    )
    ON CONFLICT (event_timestamp) DO NOTHING
    """
)

# Kafka consumer
def build_consumer():
    # Configuring consumer
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }

    return Consumer(consumer_config)

# Store Kafka message into PostgreSQL
def store_message(data):
    # Load session
    session = SessionLocal()

    try:
        session.execute(
            INSERT_SQL,
            {
                "event_timestamp": data['timestamp'],
                "school_period": data['school_period'],
                "student_count_estimated": int(data['student_count_estimated']),
                "co2_ppm": float(data['co2_ppm']),
                "pm25_ugm3": float(data['pm25_ugm3']),
                "temperature_c": float(data['temperature_c']),
                "humidity_pct": float(data['humidity_pct']),
                "robot_x_pos": float(data['robot_x_pos']),
                "robot_y_pos": float(data['robot_y_pos']),
                "ventilation_decision": data['ventilation_decision'],
                "air_quality_label": data['air_quality_label'],
                "raw_data": json.dumps(data)
            }
        )

        # Committing PostgreSQL operation
        session.commit()

    except SQLAlchemyError:
        # Roll back if the operation fails
        session.rollback()
        raise

    finally:
        # Close SQLAlchemy session
        session.close()

# Process Kafka message
def process_message(message, consumer):
    # Check if an error occurred
    if message.error():
        raise KafkaException(message.error())

    # Convert message data type to a Python dictionary
    data = json.loads(message.value().decode("utf-8"))

    logger.info("Received timestamp=%s | partition=%d | offset=%d", data['timestamp'], message.partition(), message.offset())

    try:
        # Store message into PostgreSQL
        store_message(data)

        # Committing Kafka offset
        consumer.commit(message=message)
        logger.info("Successfully stored and committed timestamp=%s", data['timestamp'])

    except SQLAlchemyError as exc:
        logger.error("PostgreSQL operation failed for timestamp=%s: %s", data['timestamp'], exc)
        raise

# Main program
def main():
    # Access PostgreSQL table
    access_table()
    
    # Display initial conditions
    logger.info("Starting Kafka consumer.")
    logger.info("Kafka: %s | topic: %s | group: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID)

    # Build consumer
    consumer = build_consumer()

    # Subscribe consumer to topic
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            try:
                # Wait for Kafka message
                message = consumer.poll(1.0)

            except KafkaException as exc:
                logger.error("Kafka polling error: %s", exc)
                time.sleep(2)
                continue

            # No message arrived
            if message is None:
                continue

            try:
                # Process message
                process_message(message, consumer)

            except SQLAlchemyError:
                logger.warning("PostgreSQL operation failed, so Kafka offset was not committed.")
                time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")

    finally:
        consumer.close()
        logger.info("Consumer stopped.")

# Run
if __name__ == "__main__":
    main()