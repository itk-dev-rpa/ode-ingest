'''
ODE files are exported from Opus Debitor and with these functions, read and imported to an SQL database.
From the documentation we have a set of keys for indexes (adjusted from documentation) and
some definitions of columns for date stamps.
'''
import os
import os.path
import pandas as pd
from dataclasses import dataclass

from sqlalchemy import create_engine, Engine


table_keys = {
    "BO-aftale-haendelse": ["Ekstern_reference_nøgle"],
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
    "RIM-aftale-renter": ["Aftalenummer", "Intr-posnr"],
    "RIM-aftale": None,
    "Rykker": ["Dato-ID", "Identifikation", "Forretningspartner", "Aftalekonto", "Rykkertæller", "Bilagsnummer", "Gentagelsesposition", "Position", "Delposition"],
    "UU-aftale-haefter": ["Klient", "Aftalenummer", "Forretningspartner"],
    "UU-aftale": None,
}

table_date_columns = {
    "BO-aftale-haendelse": ["Afskrivnings_dato", "Betalingsdato"],
    "BO-aftale": None,
    "Bilag-aaben": "Bogføringsdato",
    "Bilag-lukket": "Bogføringsdato",
    "Bilag-master": "Registreringsdato",
    "FP-aftale": "Oprettet_den",
    "Forretningspartner": None,
    "Indbetalinger": "Oprettet_den",
    "Opsaetning-Aftalekontotype": None,
    "Opsaetning-Rykkerniveau": None,
    "RIM-aftale-rater": "Oprettet_den",
    "RIM-aftale-renter": "Oprettet_den",
    "RIM-aftale": "Oprettet_den",
    "Rykker": "Udstedelsesdato",
    "UU-aftale-haefter": None,
    "UU-aftale": None,
}

table_date_formats = {
    "Bilag-master": "%d.%m.%Y"
}


@dataclass
class DateColumn:
    column: str
    start_date: str
    end_date: str
    date_format: str


def get_connection():
    connection_string = 'mssql+pyodbc://@SRVSQLHOTEL05/BackDataLake-Test?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    return create_engine(connection_string)


def find_files(directory: str, partial_names: list[str]):
    """Return files containing any of a list of partial names.

    Args:
        directory: Directory of files to look for.
        partial_names: List of partial filenames, eg "BO-aaben", "Bilag-master_Total

    Returns:
        List of files from directory matching list of names.
    """
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            for partial_name in partial_names:
                if partial_name in filename:
                    files.append(os.path.join(root, filename))
    return files


def dataframe_from_csv(file_path: str, date_column: DateColumn | None = None):
    """Read a CSV and create a pandas dataframe.

    Args:
        file_path: Path of file to convert.
        date_column: Which column, if any, contains a date stamp, and which dates to restrict dataframe to. Defaults to None.

    Returns:
        Dataframe of CSV content, within the provided dates.
    """
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, sep=';', thousands='.', decimal=',')
            df.columns = df.columns.str.replace(' ', '_')

            if date_column:
                start_date = pd.to_datetime(date_column.start_date)
                end_date = pd.to_datetime(date_column.end_date)
                if type(date_column.column) is list:
                    mask = pd.Series(False, index=df.index)
                    for col in date_column.column:
                        df[col] = pd.to_datetime(df[col], format=date_column.date_format, errors='coerce')
                        mask |= (df[col] >= start_date) & (df[col] <= end_date)
                else:
                    df[date_column.column] = pd.to_datetime(df[date_column.column], format=date_column.date_format, errors='coerce')
                    mask = (df[date_column.column] >= start_date) & (df[date_column.column] <= end_date)
                df = df.loc[mask]
                if df.count == 0:
                    return None
            return df
        except UnicodeDecodeError:
            continue


def insert_data(file_path: str, table_name: str, engine: Engine):
    """Add data to SQL.

    Args:
        file_path: File to add data from.
        table_name: SQL table to add data to.
        engine: SQL Engine to use.
    """
    existing_data = pd.read_sql_table(table_name, engine, schema="ode")
    if table_date_columns[table_name]:
        date_format = '%Y%m%d'
        if table_name in table_date_formats:
            date_format = table_date_formats[table_name]
        date_column = DateColumn(column=table_date_columns[table_name], start_date="20231130", end_date="20231231", date_format=date_format)
    else:
        date_column = None
    df = dataframe_from_csv(file_path, date_column)
    if df.empty:
        return

    df = df[df.columns.intersection(existing_data.columns)]
    df = df.astype(str)
    if (table_keys[table_name] is not None):
        df.set_index(table_keys[table_name], inplace = True)

    df.drop(df.columns[df.columns.str.contains('^Unnamed', case=False)], axis=1, inplace=True)
    print("Uploading to SQL")
    df.to_sql(table_name, engine, if_exists="append", schema="ode")


def get_column_names(file_path: str) -> list[str]:
    """Find column names from a file.

    Args:
        file_path: File to lookup.

    Returns:
        List of column names.
    """
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                elements = file.readline().strip().replace("\"", "").replace(" ", "_").split(';')
                return [element for element in elements if element]
        except UnicodeDecodeError:
            continue


def unique_columns(directory: str, partial_names: str) -> list[str]:
    """For all files matching the partial name, find columns and print if any files are missing other columns.

    Args:
        directory: Directory to look for files.
        partial_names: Partial name of file to look for (eg. "BO-udtraek_").

    Returns:
        Return all columns found across files matching the partial name.
    """
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
