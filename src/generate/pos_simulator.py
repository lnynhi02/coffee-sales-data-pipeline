import os
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(BASE_DIR)
os.chdir(BASE_DIR)

import time
import random
import threading
from faker import Faker
from datetime import datetime

from src.utils.db_helper import *
from src.utils.log_handler import *

logger = get_logger(service_name="order_generator", branch="generator")
fake = Faker()

class POSSimulator:
    def __init__(self, num_threads=1):
        self.num_threads = num_threads
        self.stop_event = threading.Event()

    def _get_conn_cursor(self):
        cfg = get_mysql_config()
        return get_conn_cursor(
            cfg["host"],
            cfg["user"],
            cfg["password"],
            cfg["database"]
        )
        
    def _upsert_customer_and_points(self, cursor, customer_id, order_total):
        name = fake.name()
        phone = fake.random_number(digits=9, fix_len=True)
        tier = "regular"

        cursor.execute("""
            INSERT IGNORE INTO customers (id, name, phone_number, tier, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (customer_id, name, phone, tier))

        cursor.execute("""
            INSERT INTO customer_points (customer_id, total_points, total_spent, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                total_points = customer_points.total_points + VALUES(total_points),
                total_spent  = customer_points.total_spent  + VALUES(total_spent),
                updated_at   = NOW()
        """, (customer_id, 1, order_total))

        # Update membership tier
        cursor.execute("""
            SELECT total_points, total_spent
            FROM customer_points
            WHERE customer_id = %s
        """, (customer_id,))
        row = cursor.fetchone()

        total_points = row["total_points"]
        total_spent  = row["total_spent"]

        new_tier = "regular"
        if total_points >= 100 or total_spent >= 7_000_000:
            new_tier = "diamond"
        elif total_points >= 40 or total_spent >= 3_000_000:
            new_tier = "gold"
        elif total_points >= 15 or total_spent >= 1_000_000:
            new_tier = "silver"
        elif total_points >= 5 or total_spent >= 300_000:
            new_tier = "bronze"

        cursor.execute("""
            UPDATE customers 
            SET tier = %s, updated_at = NOW() 
            WHERE id = %s
        """, (new_tier, customer_id))

    def _create_order(self, cursor, order_id, timestamp, customer_id, store_id, payment_method_id, num_products):
        cursor.execute("""
            INSERT INTO orders (id, timestamp, customer_id, store_id, payment_method_id, num_products)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (order_id, timestamp, customer_id, store_id, payment_method_id, num_products))
        # cursor.execute("""
        #     INSERT INTO stg_orders (order_id, timestamp, customer_id, store_id, payment_method_id, num_products)
        #     VALUES (%s, %s, %s, %s, %s, %s)
        # """, (order_id, timestamp, customer_id, store_id, payment_method_id, num_products))

    def _prepare_order_products(self, products):
        num_items = random.choices([1, 2, 3], weights=[0.75, 0.15, 0.1])[0]
        selected = random.sample(products, num_items)
        return [(p["id"], random.choice([1, 2]), p["unit_price"]) for p in selected]
    
    def _generate_orders(self, thread_id):
        with self._get_conn_cursor() as (conn, cursor):
            cursor.execute("SELECT id, name, unit_price FROM products")
            products = cursor.fetchall()

            while not self.stop_event.is_set():
                order_id = fake.uuid4()
                timestamp = datetime.now()
                customer_id = random.randint(1, 2000000)
                store_id = random.randint(1, 1000)
                payment_method_id = random.randint(1, 12)

                ordered_items = self._prepare_order_products(products)
                total_amount = sum(quantity * unit_price for (_, quantity, unit_price) in ordered_items)

                try:
                    self._upsert_customer_and_points(cursor, customer_id, total_amount)
                    self._create_order(cursor, order_id, timestamp, customer_id, store_id, payment_method_id, len(ordered_items))

                    for product_id, quantity, subtotal in ordered_items:
                        cursor.execute("""
                            INSERT INTO order_details (order_id, product_id, quantity, subtotal, is_suggestion)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (order_id, product_id, quantity, subtotal, False))
                        # cursor.execute("""
                        #     INSERT INTO stg_order_details (order_id, product_id, quantity, subtotal, is_suggestion)
                        #     VALUES (%s, %s, %s, %s, %s)
                        # """, (order_id, product_id, quantity, subtotal, False))

                    conn.commit()
                    logger.info(
                        "New order created",
                        extra={"event": "ORDER_CREATED", "order_id": order_id}
                    )
                except Exception as e:
                    logger.error(
                        "Failed to create order",
                        extra={
                            "event": "ORDER_CREATE_FAILED",
                            "order_id": order_id,
                            "meta": {
                                "error": str(e)
                            }
                        }
                    )
                    conn.rollback()
                time.sleep(0.01)
    
    def run(self):
        threads = []
        for i in range(self.num_threads):
            t = threading.Thread(target=self._generate_orders, args=(i,))
            t.start()
            threads.append(t)

        try:
            while any(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping all threads...")
            self.stop_event.set()
            for t in threads:
                t.join()


if __name__ == "__main__":
    pos = POSSimulator(num_threads=3)
    pos.run()