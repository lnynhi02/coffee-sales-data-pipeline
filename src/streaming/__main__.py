import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(BASE_DIR)

from multiprocessing import Process
from src.streaming.consumers import check_and_recommend_consumer, order_details_consumer, orders_consumer

def main():
    processes = [
        Process(target=orders_consumer.main, daemon=False,),
        Process(target=order_details_consumer.main, daemon=False,),
        Process(target=check_and_recommend_consumer.main, daemon=False,),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()