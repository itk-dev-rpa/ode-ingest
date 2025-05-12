'''

'''
import os
import os.path
import pandas as pd
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, PrimaryKeyConstraint, insert


table_keys = {
    "BO-aftale-haendelse": ["Eksterne reference nøgle"],
    "BO-aftale": ["Aftalenummer", "Bilagsnummer", "Position"],
    "Bilag-aaben": None,
    "Bilag-lukket": ["Dato-ID", "Identifikation", "Intervalnummer", "Recordnummer"],
    "Bilag-master": ["Bilagsnummer", "Gentagelsesposition", "Position", "Delposition"],
    "FP-aftale": None,
    "Forretningspartner": ["Klient", "Forretningspartner", "Identifikationsart", "Identifikationsnr.", "Forretningspartner-GUID"],
    "Indbetalinger": ["Art_kilde", "Betalingsidentifikator", "Løbenummer"],
    "Opsaetning-Aftalekontotype": ["Klient", "Sprognøgle", "Aftalekontotype"],
    "Opsaetning-Rykkerniveau": ["Sprognøgle", "Rykkeprocedure", "Rykkeniveau"],
    "RIM-aftale-rater": None,
    "RIM-aftale-renter": ["Aftalenummer", "Intr-postnr"],
    "RIM-aftale": None,
    "Rykker": ["Dato-ID", "Identifikation", "Forretningspartner", "Aftalekonto", "Rykkertæller", "Bilagsnummer", "Gentagelsesposition", "Position", "Delposition"],
    "UU-aftale-haefter": ["Klient", "Aftalenummer", "Forretningspartner"],
    "UU-aftale": None,
}

table_date_columns = {
    "BO-aftale-haendelse": None,
    "BO-aftale": None,
    "Bilag-aaben": None,
    "Bilag-lukket": "Dato-ID",
    "Bilag-master": None,
    "FP-aftale": None,
    "Forretningspartner": None,
    "Indbetalinger": None,
    "Opsaetning-Aftalekontotype": None,
    "Opsaetning-Rykkerniveau": None,
    "RIM-aftale-rater": None,
    "RIM-aftale-renter": None,
    "RIM-aftale": None,
    "Rykker": "Dato-ID",
    "UU-aftale-haefter": None,
    "UU-aftale": None,
}

api_table_keys = {
    "BO-aftale-haendelse": ["Klient", "Aftalenummer"],
    "BO-aftale": ["Aftalenummer", "Bilagsnummer", "Position"],
    "Bilag-aaben": None,
    "Bilag-lukket": ["Dato-ID", "Identifikation", "Intervalnummer", "Recordnummer"],
    "Bilag-master": ["Bilagsnummer", "Gentagelsesposition", "Position", "Delposition"],
    "FP-aftale": None,
    "Forretningspartner": ["Klient", "Forretningspartner", "Identifikationsart", "Identifikationsnr.", "Forretningspartner-GUID"],
    "Indbetalinger": ["Art_kilde", "Betalingsidentifikator", "Løbenummer"],
    "Opsaetning-Aftalekontotype": ["Klient", "Sprognøgle", "Aftalekontotype"],
    "Opsaetning-Rykkerniveau": ["Sprognøgle", "Rykkeprocedure", "Rykkeniveau"],
    "RIM-aftale-rater": None,
    "RIM-aftale-renter": ["Aftalenummer"],
    "RIM-aftale": None,
    "Rykker": ["Dato-ID", "Identifikation", "Forretningspartner", "Aftalekonto", "Rykkertæller", "Bilagsnummer", "Gentagelsesposition", "Position", "Delposition"],
    "UU-aftale-haefter": ["Klient", "Aftalenummer", "Forretningspartner"],
    "UU-aftale": None,
}

# Rework


def get_filenames(file, postfix):
    with open(file, 'r', encoding='utf-8') as f:
        return [f"{line.strip()}{postfix}" for line in f]


def add_data_to_tables(tables):
    directory = '\\\\adm.aarhuskommune.dk\\AAK\\Faelles\\MKB\\BackofficeDebitor_Rapporter'
    engine = get_connection()
    for table in tables:
        files = find_files(directory, [table])
        for file_path in files:
            insert_data(file_path, table.split("_")[0], engine)


def get_connection():
    connection_string = 'mssql+pyodbc://@SRVSQLHOTEL05/BackDataLake-Test?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    return create_engine(connection_string)


def find_files(directory, partial_names):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            for partial_name in partial_names:
                if partial_name in filename:
                    files.append(os.path.join(root, filename))
    return files


@dataclass
class DateColumn:
    column: str
    start_date: str
    end_date: str


def dataframe_from_csv(file_path: str, date_column: DateColumn | None = None):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, sep=';', thousands='.', decimal=',')
            df.columns = df.columns.str.replace(' ', '_')

            if date_column:
                df[date_column.column] = pd.to_datetime(df[date_column.column])
                mask = (df[date_column.column] > date_column.start_date and df[date_column.column] < date_column.end_date)
                df = df.loc[mask]
                if df.count == 0:
                    return None
            return df
        except UnicodeDecodeError:
            continue


def insert_data(file_path, table_name, engine):
    existing_data = pd.read_sql_table(table_name, engine, schema="ode")
    print(existing_data.columns)
    if table_date_columns[table_name]:
        date_column = DateColumn(column=table_date_columns[table_name], start_date="", end_date="")
    else:
        date_column = None

    df = dataframe_from_csv(file_path, date_column)
    if not df:
        return

    df = df[df.columns.intersection(existing_data.columns)]
    df = df.astype(str)
    if (table_keys[table_name] is not None):
        df.set_index(table_keys[table_name], inplace = True)

    df.drop(df.columns[df.columns.str.contains('^Unnamed', case=False)], axis=1, inplace=True)
    print("Uploading to SQL")
    df.to_sql(table_name, engine, if_exists="append", schema="ode")


def insert_data_columns(file_path, table_name, engine, columns):
    existing_data = pd.read_sql_table(table_name, engine)
    print(existing_data.columns)

    df = dataframe_from_csv(file_path)
    print(df.columns)

    df.drop(df.columns[df.columns.str.contains('^Unnamed', case=False)], axis=1, inplace=True)
    df.drop(columns=[col for col in df if col not in columns], axis=1, inplace=True)
    df.to_sql(table_name, engine, if_exists='append', index=False)


def upsert_data(df, table_name, engine, keys):
    # Læs eksisterende data fra tabellen
    existing_data = pd.read_sql_table(table_name, engine)

    # Find rækker der skal opdateres
    update_data = df[df[keys].apply(tuple, axis=1).isin(existing_data[keys].apply(tuple, axis=1))]
    new_data = df[~df[keys].apply(tuple, axis=1).isin(existing_data[keys].apply(tuple, axis=1))]

    # Opdater eksisterende rækker
    for _, row in update_data.iterrows():
        set_clause = ', '.join([f"{col} = '{row[col]}'" for col in df.columns if col not in keys])
        where_clause = ' AND '.join([f"{key} = '{row[key]}'" for key in keys])
        engine.execute(
            f"""
            UPDATE {table_name}
             SET {set_clause}
            WHERE {where_clause}
            """
        )

    # Indsæt nye rækker
    new_data.to_sql(table_name, engine, if_exists='append', index=False)


# ! Rework


def get_column_names(file_path):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                elements = file.readline().strip().replace("\"", "").replace(" ", "_").split(';')
                print(f"Removing some columns: {len([element for element in elements if not element])}")
                return [element for element in elements if element]  # This may create issues with mapping to columns- needs investigation
        except UnicodeDecodeError:
            continue


def unique_columns(directory, partial_names):
    all_columns = set()
    files = find_files(directory, partial_names)
    for file_path in files:
        columns = get_column_names(file_path)
        for element in all_columns:
            if element not in columns:
                print(f"{element} was not in {os.path.basename(file_path)}!")
        for column in columns:
            if files.index(file_path) != 0 and column not in all_columns:
                print(f"Adding column: {column} to {file_path}")
            all_columns.add(column)

    return all_columns


def per_partial(directory, partial_names):
    """Print all unique columns for each partial name - a sanity check on the data.

    Args:
        directory: Directory of files.
        partial_names: Set of data names to look for.
    """
    for name in partial_names:
        columns = unique_columns(directory, [name])
        print(f"{name}:\n{columns}")


def get_headlines():
    # Læs delvise filnavne fra filenames.txt
    with open('file_names.txt', 'r', encoding='utf-8') as f:
        partial_names = [line.strip() for line in f]

    # Find unikke kolonnenavne
    per_partial('\\\\adm.aarhuskommune.dk\\AAK\\Faelles\\MKB\\BackofficeDebitor_Rapporter', partial_names)

    print("done")


def generate_tables_from_filenames():
    with open('file_names.txt', 'r', encoding='utf-8') as f:
        partial_names = [line.strip() for line in f]
    directory = '\\\\adm.aarhuskommune.dk\\AAK\\Faelles\\MKB\\BackofficeDebitor_Rapporter'
    for name in partial_names:
        columns = unique_columns(directory, [name])
        print(f"Creating table {name[:-1]} with columns {columns}")
        create_table(name[:-1], columns)


def add_total_data_to_table():
    table_data = {}
    directory = '\\\\adm.aarhuskommune.dk\\AAK\\Faelles\\MKB\\BackofficeDebitor_Rapporter'
    with open('partial_filenames.txt', 'r', encoding='utf-8') as f:
        partial_names = [f"{line.strip()}Total" for line in f]
    for table in partial_names:
        files = find_files(directory, [table])
        table_rows = []
        for file_path in files:
            print(f"adding {file_path}")
            table_rows.append(read_data_from_csv(file_path))
        table_data[table] = table_rows
    return table_data


def add_data_to_sql():
    data = add_total_data_to_table()

    directory = '\\\\adm.aarhuskommune.dk\\AAK\\Faelles\\MKB\\BackofficeDebitor_Rapporter'
    # TODO: Add column for date on changes
    engine = get_connection()
    metadata = MetaData(schema='ode')
    with engine.connect() as connection:
        for table_name, table_data in data.items():
            columns = unique_columns(directory, [table_name])
            table = Table(table_name.split("_")[0], metadata, autoload_with=engine)
            for value_set in table_data:
                column_data = tuple(zip(*value_set))
                stmt = insert(table).values({col: val for col, val in zip(columns, column_data)})
                connection.execute(stmt)
                connection.commit()


def add_delta_data_to_table():
    with open('file_names.txt', 'r', encoding='utf-8') as f:
        partial_names = [f"{line.strip()}Delta" for line in f]
    print(partial_names)


def read_data_from_csv(file_path):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                rows = file.readlines()
                rows.pop(0)
                return [line.strip().replace("\"", "").split(';') for line in rows]
        except UnicodeDecodeError:
            continue


def create_table(table_name, columns):
    engine = get_connection()

    metadata = MetaData(schema='ode')
    primary_keys = table_keys[table_name]

    columns_list = [Column(col, String(255)) for col in columns]

    if (primary_keys):
        primary_key_constraint = PrimaryKeyConstraint(*primary_keys)
        columns_list.append(primary_key_constraint)
    else:
        columns_list.append(Column("index", String(255)))

    Table(table_name, metadata, *columns_list)

    metadata.create_all(engine)


def enter_data(table_name, columns, values):
    engine = get_connection()
    metadata = MetaData(schema='ode')
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.connect() as connection:
        stmt = insert(table).values({col: val for col, val in zip(columns, values)})
        connection.execute(stmt)
        connection.commit()
        print(f"Data indsat i tabel '{table_name}'")
