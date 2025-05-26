import ode_ingest as ode
import config


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
    columns = ode.unique_columns(config.FILE_DIRECTORY, [f"{name}_"])
    ode.create_table(name, columns)


def insert_total_data(table, from_file = 0, max_files = None):
    files = ode.find_files(config.FILE_DIRECTORY, [f"{table}_Total"])
    print(f"Found {len(files)} files")
    i = from_file
    for file_path in files[from_file:max_files]:
        print(f"Insert data from {file_path} at index {i}")
        i += 1
        ode.insert_data(file_path, table, ode.get_connection())


if __name__ == "__main__":
    for table in tables:
        print(f"Create table for {table}")
        create_table(table)
        insert_total_data(table, from_file=152)
