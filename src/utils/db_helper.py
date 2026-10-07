import os
import mysql.connector

from pathlib import Path
from dotenv import load_dotenv
from contextlib import contextmanager

from src.utils.paths import PROJECT_ROOT

dotenv_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path)

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