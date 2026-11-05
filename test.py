import psycopg2
import time
import string
import random
from concurrent.futures import ThreadPoolExecutor

TOTAL_ROWS = 400_000
BATCH_SIZE = 50
THREADS = 10
ROWS_PER_THREAD = TOTAL_ROWS // THREADS
SYNC_COMMIT = False


def random_text(n=100):
    return ''.join(random.choices(string.ascii_letters, k=n))


def worker(thread_id):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="myfirstdb",
        user="postgres",
        password="1508",
    )
    conn.autocommit = False
    cur = conn.cursor()

    if not SYNC_COMMIT:
        cur.execute("SET LOCAL synchronous_commit = off;")

    inserted = 0
    start = time.time()

    while inserted < ROWS_PER_THREAD:
        rows = [(random_text(),) for _ in range(BATCH_SIZE)]

        cur.executemany(
            "INSERT INTO insert_test (payload) VALUES (%s)",
            rows
        )
        conn.commit()

        inserted += BATCH_SIZE

        if inserted % 10_000 == 0:
            elapsed = time.time() - start
            print(
                f"[Thread {thread_id}] "
                f"Inserted {inserted} rows in {elapsed:.2f}s"
            )

    cur.close()
    conn.close()
    return inserted


def main():
    start = time.time()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [
            executor.submit(worker, i)
            for i in range(THREADS)
        ]
        total = sum(f.result() for f in futures)

    elapsed = time.time() - start
    print(f"\nDONE: {total} rows in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
