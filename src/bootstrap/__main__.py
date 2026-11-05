import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.bootstrap.mysql import create_table, load_reference_data
from src.bootstrap.redis import lookup_data_cache

def main():
    print("Starting platform bootstrap")

    print("==========================================")
    create_table.main()

    print("==========================================")
    load_reference_data.main()

    print("==========================================")
    lookup_data_cache.main()

    print("Bootstrap completed successfully")

if __name__ == "__main__":
    main()