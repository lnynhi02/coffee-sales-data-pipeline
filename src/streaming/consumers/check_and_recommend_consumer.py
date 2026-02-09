# =======================================================================
# SCRIPT: Check Order Eligibility and Recommend Product
# =======================================================================
# Purpose:
#   - Consume messages from Kafka topic 'order_ready_for_checking'
#   - Check eligibility for diamond-tier customers using ACB bank transfers
#   - Recommend random product with discount offers if conditions are met
# =======================================================================

import os
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)
os.chdir(BASE_DIR)

import random
import redis
import multiprocessing
from loguru import logger
from kafka import KafkaProducer

from src.utils.kafka_handler import KafkaHandler
from src.utils.log_handler import *

logger = get_logger(service_name="check_and_recommender", branch="consumer")

redis_static = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
redis_dynamic = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

def is_order_eligible(customer_id: int, payment_method_id: int) -> bool:
    """
    Check if an order is eligible for product recommendation based on:
        - Payment method is bank transfer (ACB)
        - Customer belongs to diamond tier
    Returns:
        Tuple of (eligibility boolean, customer_id if eligible else None)
    """

    if not customer_id or not payment_method_id:
        return False
    
    if not (
        redis_static.sismember("bank_acb_payment", payment_method_id) and
        redis_static.sismember("diamond_customers", customer_id)
    ):
        return False

    return True


def product_recommendation(exclude_ids: list[str]) -> dict:
    """
    Randomly select a product from Redis static cache excluding products in exclude_ids.

    Returns:
        A dictionary with product details or None if no suitable product found.
    """

    keys = redis_static.keys("product:*")
    product_candidates = []

    for key in keys:
        product_id = key.split(":")[1]
        if product_id in exclude_ids:
            continue

        product = redis_static.hgetall(f"product:{product_id}")
        product["product_id"] = product_id
        product_candidates.append(product)

    return random.choice(product_candidates) if product_candidates else None


def process_message(message, producer: KafkaProducer):
    order_payload = message.value
    order_id = order_payload["order_id"]
    customer_id = order_payload["customer_id"]
    payment_method_id = order_payload["payment_method_id"]
    ordered_product_ids = order_payload["product_ids"]
    
    if redis_dynamic.get(f"order_status:{order_id}") == "completed":
        return

    #  --- Start eligibility check for recommendation ---
    # If order does not meet criteria, mark as completed and skip recommendation
    if not is_order_eligible(customer_id, payment_method_id):
        redis_dynamic.set(f"order_status:{order_id}", "completed", ex=100)
        logger.info(
            "Order marked completed without recommendation",
            extra={
                "event": "ORDER_SKIPPED",
                "order_id": order_id,
                "meta": {"recommended": False, "reason": "not eligible"}
            }
        )
        return

    # --- Order passed eligibility checks (diamond tier + ACB bank transfer) ---
    # Proceed to find a recommendation
    exclude_product_ids = [pid for pid in ordered_product_ids]
    suggested_product = product_recommendation(exclude_ids=exclude_product_ids)
    if not suggested_product:
        redis_dynamic.set(f"order_status:{order_id}", "completed")
        return

    # --- Prepare suggestion details with random discount ----
    product_id = suggested_product["product_id"]
    product_name = suggested_product["name"]
    unit_price = int(suggested_product["unit_price"])

    discount_factor = round(random.uniform(0.5, 0.8), 2)
    quantity = 1
    subtotal = int(unit_price * quantity * discount_factor)
    discount_percent = round((1 - discount_factor) * 100)

    logger.info(
        "Recommend product",
        extra={
            "event": "RECOMMEND_PRODUCT",
            "order_id": order_id,
            "meta": {
                "product_id": product_id,
                "product_name": product_name,
                "discount_percent": discount_percent
            }
        }
    )

    producer.send("order_suggestion", {
        "order_id": order_id,
        "product_id": product_id,
        "product_name": product_name,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_percent,
        "subtotal": subtotal,
        "is_suggestion": True
    })

    producer.send("order_suggestion_accepted", {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "discount_percent": discount_percent,
        "subtotal": subtotal,
        "is_suggestion": True
    })
    
    # --- Mark order as completed ---
    redis_dynamic.set(f"order_status:{order_id}", "completed", ex=100)
    logger.info(
        "Order marked completed with recommendation",
        extra={
            "event": "ORDER_RECOMMENDED",
            "order_id": order_id,
            "meta": {"recommended": True, "product_id": product_id, "discount_percent": discount_percent}
        }
    )


def recommendation_engine_worker(worker_id: int):
    bootstrap_server = ["localhost:19092", "localhost:19093", "localhost:19094"]
    kafka_client = KafkaHandler(bootstrap_server)
    producer = kafka_client.get_producer()
    consumer = kafka_client.get_consumer(
        topic="order_ready_for_checking",
        group_id="recommendation_engine"
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
            "Unhandled error in recommendation engine",
            extra={
                "event": "PROCESS_MESSAGE_FAILED",
                "meta": {"error": str(e)}
            }
        )
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("Kafka consumer closed.")
        print("Kafka producer closed.")


def main() -> None:
    num_workers = 2
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=recommendation_engine_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
