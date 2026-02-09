# ==================================================================================
# SCRIPT: Load CSV Files into MySQL Source Tables
# ==================================================================================
# Purpose:
#   - Load structured data from CSV files into MySQL source tables.
#   - These tables serve as dimension or lookup tables for the data pipeline.
#
# Source:
#   - CSV files located in the 'data/' directory:
#
# Target MySQL Tables: diamond_customers, stores, product_category, products, payment_method
#
# Notes:
#   - Make sure the MySQL server is running and accessible.
#   - Ensure table schemas in MySQL match the structure of the CSV files.
#   - Enable 'local_infile' on both server and client sides.
# ==================================================================================

import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

import mysql.connector
from mysql.connector import errorcode
from src.utils.db_helper import get_mysql_config

DATA_DIR = BASE_DIR / "data" / "seeds"

def connect_database(user, password, host, database):
    """Connect to MySQL database."""
    try:
        conn = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            allow_local_infile=True
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username and password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
        sys.exit(1)
    else:
        print("Connect to database successfully!")
        return conn
    
def load_data_to_table(cursor, table_name, csv_path):
    """Load CSV files to tables."""
    csv_path_str = csv_path.replace("\\", "/")

    load_query = f"""
    LOAD DATA LOCAL INFILE '{csv_path_str}'
    INTO TABLE `{table_name}`
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS
    """
    # SET updated_at = CURRENT_TIMESTAMP;

    try:
        cursor.execute(load_query)
    except mysql.connector.Error as err:
        print(f"Error loading data to {table_name}: {err.msg}")
    else:
        print(f"Data loaded into {table_name} from {csv_path}")

def main():
    # Configuration to connect to MySQL database
    mysql_config = get_mysql_config()

    conn = connect_database(**mysql_config)
    cursor = conn.cursor()

    # Load CSV files to each table
    tables_and_files = {
        'stores': str(DATA_DIR / 'stores.csv'),
        'payment_method': str(DATA_DIR / 'payment_method.csv'),
        'product_category': str(DATA_DIR / 'product_category.csv'),
        'products': str(DATA_DIR / 'products.csv'),
        'customers': str(DATA_DIR / 'customers.csv'),
        'customer_points': str(DATA_DIR / 'customer_points.csv'),
    }
    for table, csv_path in tables_and_files.items():
        load_data_to_table(cursor, table, csv_path)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()