'''
ODE files are exported from Opus Debitor and with these functions, read and imported to an SQL database.
From the documentation we have a set of keys for indexes (adjusted from documentation) and
some definitions of columns for date stamps.
'''
import os
import os.path
from pathlib import Path
from typing import Optional

import pandas as pd

from sqlalchemy import create_engine, Engine, Table, Column, String, MetaData, PrimaryKeyConstraint
import config
from csv_cleaner import CSVCleaner, DateColumn
from table_columns import table_keys, table_used_columns


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
    """Read a CSV and create a pandas dataframe. THIS IS LEGACY AND SHOULD BE REMOVED

    Args:
        file_path: Path of file to convert.
        date_column: Which column, if any, contains a date stamp, and which dates to restrict dataframe to. Defaults to None.

    Returns:
        Dataframe of CSV content, within the provided dates.
    """
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, sep=';', thousands='.', decimal=',', dtype='str')
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


def insert_data(file_path: str,
                table_name: str,
                engine: Engine,
                date_filter: Optional[DateColumn] = None):
    """Add data to SQL.

    Args:
        file_path: File to add data from.
        table_name: SQL table to add data to.
        engine: SQL Engine to use.
        date_filter: Dictionary med kolonnenavn, start- og slutdato.
    """

    csv_file = Path(file_path)

    if not csv_file.exists():
        return

    cleaner = CSVCleaner()
    analysis = cleaner.analyze_csv(csv_file)
    date_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'date']
    int_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'integer']
    float_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'float']

    df = cleaner.read_csv_with_types(
            csv_file,
            date_columns=date_cols,
            integer_columns=int_cols,
            float_columns=float_cols,
            date_filter=date_filter  # Valgfrit
        )

    # df = dataframe_from_csv(csv_file)

    df = df[df.columns.intersection(set(table_used_columns[table_name] + table_keys[table_name]))]
    if table_keys[table_name] is not None:
        df.set_index(table_keys[table_name], inplace = True)

    df.drop(df.columns[df.columns.str.contains('^Unnamed', case=False)], axis=1, inplace=True)
    print("Uploading to SQL")
    df.to_sql(table_name, engine, if_exists="append", schema="ode")


def create_table(table_name: str, columns: list[str]):
    """Create table in the SQL database with the table name and columns.

    Args:
        table_name: Table name for the table.
        columns: Columns for the table.
    """
    engine = create_engine(config.CONNECTION_STRING)

    metadata = MetaData(schema='ode')
    primary_keys = table_keys[table_name]

    columns_list = [Column(col, String(255)) for col in columns]

    if primary_keys:
        primary_key_constraint = PrimaryKeyConstraint(*primary_keys)
        columns_list.append(primary_key_constraint)
    else:
        columns_list.append(Column("index", String(255)))

    Table(table_name, metadata, *columns_list)

    metadata.create_all(engine)


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
