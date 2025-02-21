#####################################
# Import Modules
#####################################
# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger


#####################################
# Define Message Generator
#####################################

def generate_messages():
    """
    Generate a stream of JSON messages.
    """
    TOPICS = {
    "Horror": "Horror",
    "Action": "Action",
    "RPG": "RPG",
    "Roguelike": "Roguelike",
    "Platformer": "Platformer",
    "Sports": "Sports",
    "Strategy": "Strategy",
    "FPS": "FPS",
    "MMO": "MMO",
    "Mobile": "Mobile",
}
    
    while True:
        score = random.randint(50, 99)  # Generate a random score between 50 and 99
        topic = random.choice(list(TOPICS.keys()))  # Choose a random topic
        message_text = f"I just played a {topic} game! I'd give it a score of {score}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Get the category based on the topic
        category = TOPICS.get(topic, "Other")  # Default to 'Other' if no match

        # Find category based on keywords (could still use this if relevant)
        keyword_mentioned = next(
            (word for word in topic if word in topic), "other"
        )

        # Create JSON message with category from mapping
        json_message = {
            "message": message_text,
            "timestamp": timestamp,
            "category": category,  # Use category from mapping
            "Review Score": score,
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
        }

        yield json_message

#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()