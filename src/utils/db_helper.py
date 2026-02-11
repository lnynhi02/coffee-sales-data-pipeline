import os
from pathlib import Path
from dotenv import load_dotenv
from contextlib import contextmanager

import mysql.connector

BASE_DIR = Path(__file__).resolve().parent.parent.parent
dotenv_path = BASE_DIR / ".env"
load_dotenv(str(dotenv_path))

def get_mysql_config():
    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "host": os.getenv("MYSQL_HOST"),
        "database": os.getenv("MYSQL_DATABASE"),
    }

@contextmanager
def get_conn_cursor(host, user, password, database):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor(buffered=True, dictionary=True)
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()

def execute_query(query, cursor):
    cursor.execute(query)