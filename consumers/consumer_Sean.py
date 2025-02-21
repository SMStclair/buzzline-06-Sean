import json
import os
import pathlib
import sys
import matplotlib.pyplot as plt
import numpy as np

from kafka import KafkaConsumer
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Function to process a single message
def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    try:
        processed_message = {
            "category": message.get("category"),
            "review_score": int(message.get("Review Score", 0)),  # Add the review score
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

# Function to update and calculate the average review scores
def update_genre_scores(genre_scores, message):
    """
    Update the genre_scores dictionary with the new review score for the given genre.
    Calculate average scores after all messages have been processed.
    """
    genre = message.get("category")
    score = message.get("review_score")

    if genre not in genre_scores:
        genre_scores[genre] = {"total_score": 0, "count": 0}
    
    genre_scores[genre]["total_score"] += score
    genre_scores[genre]["count"] += 1

# Function to plot the bar chart of average review scores
def plot_average_scores(genre_scores):
    """
    Plot a bar chart showing the average review scores for each genre.
    """
    genres = list(genre_scores.keys())
    average_scores = [data["total_score"] / data["count"] for data in genre_scores.values()]

    plt.clf()  # Clear the current figure
    plt.bar(genres, average_scores, color=plt.cm.plasma(np.linspace(0, 1, len(genres))))
    plt.xlabel('Genres')
    plt.ylabel('Average Review Score')
    plt.title('Average Review Scores by Genre')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.draw()  # Redraw the plot
    plt.pause(0.1)  # Pause to update the plot (without blocking the loop)

# Consume Messages from Kafka Topic
def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    genre_scores = {}  # Dictionary to store total score and count for each genre

    # Enable interactive mode for matplotlib
    plt.ion()  # Turn on interactive mode
    plt.figure()  # Create a new figure

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                update_genre_scores(genre_scores, processed_message)
                plot_average_scores(genre_scores)  # Update the plot after each message

            plt.pause(0.1)  # Pause to allow the plot to update

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

    finally:
        plt.ioff()  # Turn off interactive mode to stop updating the plot

# Define Main Function
def main():
    """
    Main function to run the consumer process.

    Reads configuration and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 4. Begin consuming messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()
