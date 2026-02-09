# ================================================================================
# SCRIPT: Tracking ordered products
# ================================================================================
# Description:
#   - Consume messages from Kafka topic 'mysql.coffee_shop.order_details'
#   - Cache each product item into Redis under the key 'ordered_products:{order_id}'
#   - Call check_and_trigger() to verify if all products for an order are present
#   - If complete and order info exists, send a consolidated message to topic
#     'order_ready_for_checking'
# ================================================================================

import os
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, BASE_DIR)
os.chdir(BASE_DIR)

import redis
import multiprocessing
from loguru import logger

from src.utils.kafka_handler import KafkaHandler
from src.utils.log_handler import *

logger = get_logger(service_name="order_details_tracker", branch="consumer")

# Redis Configuration
redis_dynamic = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

def check_and_trigger(order_id, producer):
    if redis_dynamic.get(f"order_status:{order_id}") == "checking":
        return
    
    order_info = redis_dynamic.hgetall(f"order_info:{order_id}")
    if not order_info:
        return
    
    num_products = int(order_info.get("num_products", 0))
    current_products = redis_dynamic.smembers(f"ordered_products:{order_id}")
    if len(current_products) == num_products:
        producer.send("order_ready_for_checking", {
            "order_id": order_id,
            "customer_id": order_info["customer_id"],
            "payment_method_id": order_info["payment_method_id"],
            "product_ids": list(current_products),
        })
    
        redis_dynamic.set(f"order_status:{order_id}", "checking")
        redis_dynamic.delete(f"order_info:{order_id}")
        redis_dynamic.delete(f"ordered_products:{order_id}")

        logger.info(
            "Order ready for recommendation checking",
            extra={
                "event": "ORDER_READY_FOR_CHECKING",
                "order_id": order_id,
                "meta": {"num_products": num_products}
            }
        )
    

def process_message(message, producer):
    order_detail_payload = message.value.get("payload")["after"]
    order_id = order_detail_payload["order_id"]
    product_id = order_detail_payload["product_id"]

    if redis_dynamic.get(f"order_status:{order_id}") == "checking":
        return
    
    # Add product_id to the Redis set for this order (to track all ordered products)
    redis_dynamic.sadd(f"ordered_products:{order_id}", product_id)
    redis_dynamic.expire(f"ordered_products:{order_id}", 120)

    check_and_trigger(order_id, producer)


def ordered_products_worker(worker_id: int):
    bootstrap_servers = ["localhost:19092", "localhost:19093", "localhost:19094"]
    kafka_client = KafkaHandler(bootstrap_servers)
    producer = kafka_client.get_producer()
    consumer = kafka_client.get_consumer(
        topic="mysql.coffee_shop.order_details",
        group_id="ordered_products_tracker"
    )

    try:
        while True:
            message_pack = consumer.poll(timeout_ms=1000)
            for _, messages in message_pack.items():
                for message in messages:
                    process_message(message, producer)
    except KeyboardInterrupt:
        print("Received shutdown signal. Exiting gracefully...")
    except Exception as e:
        logger.error(
            "Failed to process order detail message",
            extra={
                "event": "ORDER_DETAIL_PROCESS_FAILED",
                "meta": {"error": str(e)}
            }
        )
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print("Kafka producer closed.")
        print("Kafka consumer closed.")


def main():
    num_workers = 4
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=ordered_products_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    
if __name__ == "__main__":
    main()
