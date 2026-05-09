import json
import random
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
TOPIC = "clickstream-events"
MAX_RETRIES = 3
RETRY_DELAY = 5

def load_product_catalog():
    try:
        catalog_path = "/opt/airflow/data/product_catalog.csv"
        
        logger.info(f"Loading product catalog from {catalog_path}")
        
        if not os.path.exists(catalog_path):
            logger.error(f"Product catalog not found at {catalog_path}")
            raise FileNotFoundError(f"Product catalog not found at {catalog_path}")
        
        catalog = pd.read_csv(catalog_path)
        logger.info(f"Loaded {len(catalog)} products from catalog")
        
        product_to_category = dict(
            zip(
                catalog["product_id"].astype(str).str.strip().str.upper(),
                catalog["category"].astype(str).str.strip()
            )
        )
        
        return product_to_category, list(catalog["product_id"].astype(str).str.strip().str.upper())
        
    except Exception as e:
        logger.error(f"Error loading product catalog: {e}", exc_info=True)
        raise

def create_kafka_producer(retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            logger.info(f"Attempting to connect to Kafka broker: {KAFKA_BROKER} (Attempt {attempt + 1}/{retries})")
            
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            
            logger.info("Successfully connected to Kafka broker")
            return producer
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka (Attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logger.critical("Failed to connect to Kafka after all retries")
                raise

def generate_event(users, products, product_to_category):
    try:
        user_id = random.choice(users)
        product_id = random.choice(products)
        
        heavy_view_products = ["P0001", "P0002", "P0003"]
        if random.random() < 0.4 and heavy_view_products:
            product_id = random.choice(heavy_view_products)
        
        category = product_to_category.get(product_id, "UNKNOWN")
        
        event_type = random.choices(
            ["view", "add_to_cart", "purchase"],
            weights=[0.8, 0.15, 0.05],
            k=1
        )[0]
        
        event = {
            "user_id": user_id,
            "product_id": product_id,
            "category": category,
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error generating event: {e}")
        return None

def on_send_success(record_metadata):
    logger.debug(f"Event sent to partition {record_metadata.partition} at offset {record_metadata.offset}")

def on_send_error(exc):
    logger.error(f"Failed to send event: {exc}", exc_info=True)

def main():
    try:
        logger.info("=" * 60)
        logger.info("Clickstream Event Producer Starting")
        logger.info("=" * 60)
        
        product_to_category, products = load_product_catalog()
        users = [f"U{str(i).zfill(4)}" for i in range(1, 201)]
        
        logger.info(f"Generated {len(users)} users and {len(products)} products")
        
        producer = create_kafka_producer()
        
        logger.info(f"Starting to produce events to topic '{TOPIC}'...")
        logger.info("Press Ctrl+C to stop the producer\n")
        
        event_count = 0
        error_count = 0
        
        while True:
            try:
                event = generate_event(users, products, product_to_category)
                
                if event:
                    future = producer.send(TOPIC, event)
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    
                    event_count += 1
                    
                    if event_count % 100 == 0:
                        logger.info(f"Produced {event_count} events (Errors: {error_count})")
                        logger.debug(f"Latest event: {event}")
                    
                    time.sleep(0.1)
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                error_count += 1
                time.sleep(1)
        
    except KeyboardInterrupt:
        logger.info("\nProducer interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error in producer: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'producer' in locals():
            logger.info("Flushing remaining messages...")
            producer.flush()
            producer.close()
            logger.info("Producer closed successfully")

if __name__ == "__main__":
    main()
