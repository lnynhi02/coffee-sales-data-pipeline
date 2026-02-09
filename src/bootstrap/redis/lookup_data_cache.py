# ===================================================================
# SCRIPT: Cache Reference Data to Redis
# ===================================================================
# Purpose:
#   Load and cache diamond-tier customer IDs, ACB bank payment method IDs,
#   and product details from MySQL into Redis for fast access during
#   real-time product recommendation processing.
# ===================================================================

import sys
from pathlib import Path

import redis
import mysql.connector

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.common_helper import get_mysql_config

# Redis connection
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def cache_diamond_customers(cursor):
    try:
        cursor.execute("SELECT id FROM customers WHERE tier = 'diamond'")
        rows = cursor.fetchall()
        for row in rows:
            r.sadd("diamond_customers", row["id"])

        print(f"Cached {len(rows)} diamond customer IDs to Redis")
    except Exception as e:
        print(f"Failed to cache diamond customers: {e}")


def cache_payment_method(cursor):
    try:
        cursor.execute("SELECT id FROM payment_method WHERE bank LIKE '%ACB%'")
        row = cursor.fetchone()
        if row:
            r.sadd("bank_acb_payment", row["id"])
            print("Cached ACB payment method ID to Redis")
        else:
            print("No ACB payment method found in database")
    except Exception as e:
        print(f"Failed to cache payment method: {e}")


def cache_product(cursor):
    try:
        cursor.execute("SELECT id, name, unit_price FROM products")
        rows = cursor.fetchall()
        for row in rows:
            r.hset(
                f"product:{row['id']}",
                mapping={"name": row["name"], "unit_price": row["unit_price"]},
            )
        print(f"Cached {len(rows)} products to Redis")
    except Exception as e:
        print("Failed to cache products:", e)


def main():
    mysql_config = get_mysql_config()

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)

        cache_diamond_customers(cursor)
        cache_product(cursor)
        cache_payment_method(cursor)

    except Exception as e:
        print("Failed to connect to MySQL or execute caching tasks:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
