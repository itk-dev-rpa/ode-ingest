import ode_ingest.ode_ingest as ode
from ode_ingest import config


def create_table():
    name = "BO-aftale-haendelse"
    columns = ode.unique_columns(config.FILE_DIRECTORY, [name])
    ode.create_table(name, columns)


def insert_total_data():
    files = ode.find_files(config.FILE_DIRECTORY, ["BO-aftale-haendelse_Total"])
    for file_path in files:
        ode.insert_data(file_path, "BO-aftale-haendelse", ode.get_connection())


if __name__ == "__main__":
    insert_total_data()
