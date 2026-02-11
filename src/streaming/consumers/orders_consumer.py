# =======================================================================
# SCRIPT: Tracking order info
# =======================================================================
# Description:
#   - Consume messages from Kafka topic 'mysql.coffee_shop.orders'
#   - Cache order details (customer_id, payment_method_id, num_products) in Redis
#   - Call check_and_trigger() to verify if all products have arrived
#   - If complete and products exist, send a consolidated message to topic
#     'order_ready_for_checking'
# =======================================================================

import os
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)
os.chdir(BASE_DIR)

import redis
import multiprocessing
from loguru import logger

from src.utils.kafka_handler import KafkaHandler
from src.utils.log_handler import *

logger = setup_logger(service_name="order_tracker", branch="consumer")

# Redis configuration
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
    
        logger.success(
            "Order ready for recommendation checking",
            extra={
                "event": "ORDER_READY_FOR_CHECKING",
                "order_id": order_id,
                "meta": {"num_products": num_products}
            }
        )


def process_message(message, producer):
    order_payload = message.value.get("payload")["after"]
    order_id = order_payload["id"]
    customer_id = order_payload["customer_id"]
    payment_method_id = order_payload["payment_method_id"]
    num_products = order_payload["num_products"]

    if redis_dynamic.get(f"order_status:{order_id}") == "checking":
        return    
    
    redis_dynamic.hset(f"order_info:{order_id}", mapping={
        "customer_id": customer_id,
        "payment_method_id": payment_method_id,
        "num_products": num_products
    })

    redis_dynamic.expire(f"order_info:{order_id}", 120)
    check_and_trigger(order_id, producer)
    

def order_info_worker(worker_id: int):
    bootstrap_servers = ["localhost:19092", "localhost:19093", "localhost:19094"]
    kafka_client = KafkaHandler(bootstrap_servers)
    producer = kafka_client.get_producer()
    consumer = kafka_client.get_consumer(
        topic="mysql.coffee_shop.orders",
        group_id="order_info_tracker"
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
            "Failed to process order message",
            extra={
                "event": "ORDER_PROCESS_FAILED",
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
    num_workers = 3
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=order_info_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

        
if __name__ == "__main__":
    main()