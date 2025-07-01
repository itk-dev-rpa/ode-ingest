from sqlalchemy import create_engine
import ode_ingest as ode
import config
from table_columns import table_date_columns, table_used_columns, table_keys
from csv_cleaner import DateColumn


tables = [
    "BO-aftale-haendelse",
    "BO-aftale",
    "Bilag-aaben",
    "Bilag-master",
    "Bilag-lukket",
    "FP-aftale",
    "Forretningspartner",
    "Indbetalinger",
    "Opsaetning-Aftalekontotype",
    "Opsaetning-Rykkerniveau",
    "RIM-aftale-rater",
    "RIM-aftale-renter",
    "RIM-aftale",
    "Rykker",
    "UU-aftale-haefter",
    "UU-aftale",
]


def create_table(name):
    columns = set()
    for table_dict in [table_used_columns, table_date_columns, table_keys]:
        if name in table_dict and table_dict[name]:
            columns.update(table_dict[name])
    ode.create_table(name, columns)


def insert_total_data(table, from_file = 0, max_files = None):
    files = ode.find_files(config.FILE_DIRECTORY, [f"{table}_Total"])
    print(f"Found {len(files)} files")
    i = from_file
    date_column = table_date_columns.get(table)
    if date_column:
        date_column = DateColumn(date_column, '20231130', '20231231')
    for file_path in files[from_file:max_files]:
        print(f"Insert data from {file_path} at index {i}")

        i += 1
        print(f"Inserting data from file {i}/{len(files)}")
        ode.insert_data(file_path, table, create_engine(config.CONNECTION_STRING), date_column)
        # try:
        # except Exception:
        #     print("Insert failed. Retrying.")
        #     insert_total_data(table, i - 1)


if __name__ == "__main__":
    for table in tables:
        print(f"Create table for {table}")
        create_table(table)
        insert_total_data(table)
