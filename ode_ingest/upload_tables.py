from os import path
import shutil
from sqlalchemy import create_engine
import ode_ingest as ode
import config
from table_columns import table_date_columns, table_used_columns, table_keys
from csv_cleaner import DateColumn
import file_sorting as sort


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
    date_column = None  # table_date_columns.get(table)
    engine = create_engine(config.CONNECTION_STRING.replace("{DB_NAME}", config.DB_NAME), fast_executemany=True)
    if date_column:
        date_column = DateColumn(date_column, '20231130', '20231231')
    for file_path in files[from_file:max_files]:
        print(f"Insert data from {file_path} at index {i}")

        i += 1
        print(f"Inserting data from file {i}/{len(files)}")
        df = ode.create_dataframe_from_file(file_path, table, date_column)
        ode.insert_data(df, table, engine)
        # try:
        # except Exception:
        #     print("Insert failed. Retrying.")
        #     insert_total_data(table, i - 1)


def insert_delta_data(delta_table, from_file = 0):
    ''' This function will need to insert data from delta files that have not yet been processed - either deleting files as they are added
    or setting a flag somewhere, making sure we keep a record of the last date we updated.
    The second part is to make sure we upload the files in the correct order.
    Actually updating or inserting shouldn't be a problem, but we'll see.
    '''
    files = ode.find_files(config.FILE_DIRECTORY, [f"{delta_table}_Delta"])
    files = sort.sort_files(files)
    i = from_file  # Should this be based on file name?
    engine = create_engine(config.CONNECTION_STRING.replace("{DB_NAME}", config.DB_NAME), fast_executemany=True)

    for file_path in files[from_file:]:
        i += 1
        print(f"Inserting data from file {i}/{len(files)}")
        df = ode.create_dataframe_from_file(file_path, delta_table)
        ode.merge_table_from_dataframe(df, delta_table, engine)
        directory, filename = path.split(file_path)
        shutil.move(file_path, path.join(directory, "processed_delta_files", filename))


if __name__ == "__main__":
    for table in tables:
        print(f"Create table for {table}")
        create_table(table)
        # insert_total_data(table)
        insert_delta_data(table)
