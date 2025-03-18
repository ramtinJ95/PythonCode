import logging
from src.database_manager import reset_database, database_setup
from src.load_csv import load_csv_files
from src.get_currencies_and_rates import get_currencies_and_rates


def main():
    reset_choice = input("Do you want to reset the database? (Y/N): ").strip().upper()

    if reset_choice == "Y":
        reset_database()
        database_setup()

    get_currencies_and_rates()
    load_csv_files()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
