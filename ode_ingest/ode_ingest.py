'''
ODE files are exported from Opus Debitor and with these functions, read and imported to an SQL database.
From the documentation we have a set of keys for indexes (adjusted from documentation) and
some definitions of columns for date stamps.
'''
import os
import os.path
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine, text, Engine, Table, Column, String, MetaData, PrimaryKeyConstraint
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
    for filename in Path(directory).iterdir():
        for partial_name in partial_names:
            if partial_name in filename.name:
                files.append(str(filename))
    return files


def insert_data(df: pd.DataFrame,
                table_name: str,
                engine: Engine):
    """Add data to SQL.

    Args:
        df: Dataframe to add data from.
        table_name: SQL table to add data to.
        engine: SQL Engine to use.
    """
    print("Uploading to SQL")
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, schema=config.DB_SCHEMA)

    with engine.begin() as conn:
        conn.execute(table.insert(), df.to_dict('records'))
    # df.to_sql(name=table_name, con=engine, if_exists="append", method='multi', chunksize=1000, schema=config.DB_SCHEMA)


def create_dataframe_from_file(file_path: str, table_name: str, date_filter: Optional[DateColumn] = None) -> pd.DataFrame:
    csv_file = Path(file_path)

    if not csv_file.exists():
        return None

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

    # Make sure we only use the required columns
    columns = set()
    for table_dict in [table_used_columns, table_keys]:
        if table_name in table_dict and table_dict[table_name]:
            columns.update(table_dict[table_name])
    df = df[df.columns.intersection(columns)]

    if table_keys[table_name] is None:
        df.reset_index(allow_duplicates=True)

    drop_columns = df.columns[df.columns.str.contains('^Unnamed', case=False)]
    df.drop(drop_columns, axis=1, inplace=True)
    return df


def update_table_from_dataframe(df: pd.DataFrame, table_name: str, engine: Engine):
    """Brug sqalchemy til at opdatere tabellen med nye værdier."""
    key_columns = table_keys[table_name]
    if not key_columns:
        print("No keys found, inserting whole table.")
        # There are no keys, just insert the whole table.
        return insert_data(df, table_name, engine)
    else:
        df.set_index(key_columns, inplace=True)

    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        inserted = 0
        updated = 0
        new_records = []
        index_data = df.to_dict('index')
        for record in index_data:  # Here we get the index keys as keys for a dictionary of columns as keys with values
            index_values = [record] if isinstance(record, str) else list(record)
            index_keys = dict(zip(key_columns, index_values))

            where_conditions = []
            where_params = {}
            # Set where clause from record as values for keys
            for index in index_keys:
                where_conditions.append(f"[{index}] = :key_{index.replace(".", "")}")
                where_params[f"key_{index}".replace(".", "")] = index_keys[index]
            where_clause = " AND ".join(where_conditions)

            # Byg SET-klausul for update (alle kolonner undtagen keys)
            set_conditions = []
            update_params = {}
            for col in index_data[record]:  # Access specific dictionary of this index
                set_conditions.append(f"[{col}] = :update_{col.replace(".", "")}")
                update_params[f"update_{col}".replace(".", "")] = index_data[record][col]

            set_clause = ", ".join(set_conditions)

            # Prøv at opdatere først
            update_table = f"[{config.DB_NAME}].[{config.DB_SCHEMA}].[{table_name}]"   # Fix for some table names containing "-" when using sqlalchemy
            update_sql = f"UPDATE {update_table} SET {set_clause} WHERE {where_clause}"
            all_params = {**where_params, **update_params}

            results = session.execute(text(update_sql), all_params)
            if results.rowcount == 0:
                inserted += 1
                values = {**index_keys, **index_data[record]}
                new_records.append(values)
            else:
                updated += 1

        print(f"Updated {updated} rows, inserting {inserted} new rows from delta.")

        new_records_df = pd.DataFrame(new_records)
        if table_keys[table_name] is not None:
            new_records_df.set_index(table_keys[table_name], inplace = True)
        insert_data(new_records_df, table_name, engine)

        session.commit()

    except Exception as e:
        print(e)
        session.rollback()
        raise
    finally:
        session.close()


def merge_table_from_dataframe(df: pd.DataFrame, table_name: str, engine: Engine):
    """Brug SQL Server MERGE til at opdatere/indsætte data i bulk."""
    key_columns = table_keys[table_name]
    if not key_columns:
        print("No keys found, inserting whole table.")
        return insert_data(df, table_name, engine)

    # Opret temp table navn
    temp_table = f"#temp_{table_name}_{int(time.time())}"

    try:
        # 1. Upload dataframe til temp table
        print(f"Uploading {len(df)} rows to temp table...")
        # Brug almindelig engine connection

        metadata = MetaData(schema='ode')
        Table(temp_table, metadata, *[Column(col, String(255)) for col in list(df.columns)])
        metadata.create_all(engine)

        insert_data(df, temp_table, engine)

        # 2. Byg MERGE statement
        target_table = f"[{config.DB_NAME}].[{config.DB_SCHEMA}].[{table_name}]"
        temp_table = f"[{config.DB_NAME}].[{config.DB_SCHEMA}].[{temp_table}]"

        # ON clause - match på primary keys
        on_conditions = []
        for key in key_columns:
            on_conditions.append(f"target.[{key}] = source.[{key}]")
        on_clause = " AND ".join(on_conditions)

        # SET clause - alle kolonner undtagen keys
        data_columns = [col for col in df.columns if col not in key_columns]
        set_conditions = []
        for col in data_columns:
            set_conditions.append(f"[{col}] = source.[{col}]")
        set_clause = ", ".join(set_conditions)

        # INSERT clause - alle kolonner
        all_columns = df.columns.tolist()
        insert_columns = ", ".join([f"[{col}]" for col in all_columns])
        insert_values = ", ".join([f"source.[{col}]" for col in all_columns])

        merge_sql = f"""
        MERGE {target_table} AS target
        USING {temp_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        OUTPUT $action, inserted.*;
        """

        # 3. Kør MERGE og få resultater
        print("Running MERGE operation...")
        with engine.connect() as conn:
            result = conn.execute(text(merge_sql))

            # Tæl resultater fra OUTPUT
            updated = 0
            inserted = 0
            try:
                for row in result:
                    if row[0] == 'UPDATE':
                        updated += 1
                    elif row[0] == 'INSERT':
                        inserted += 1
            except Exception:
                # Hvis OUTPUT ikke virker, kør uden
                pass

            print(f"MERGE completed: {updated} updated, {inserted} inserted")
            conn.commit()

    except Exception as e:
        print(f"MERGE failed: {e}")
        raise
    finally:
        # Ryd op temp table
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
        except Exception:
            pass  # Temp tables ryddes automatisk op
        # Ryd op temp table
        try:
            with engine.raw_connection() as raw_conn:
                cursor = raw_conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                raw_conn.commit()
        except Exception:
            pass  # Temp tables ryddes automatisk op


def create_table(table_name: str, columns: list[str]):
    """Create table in the SQL database with the table name and columns.

    Args:
        table_name: Table name for the table.
        columns: Columns for the table.
    """
    engine = create_engine(config.CONNECTION_STRING.replace("{DB_NAME}", config.DB_NAME))

    metadata = MetaData(schema='ode')
    primary_keys = table_keys[table_name] if table_name in table_keys else None

    columns = set()
    for table_dict in [table_used_columns, table_keys]:
        if table_name in table_dict and table_dict[table_name]:
            columns.update(table_dict[table_name])

    columns_list = [Column(col, String(255)) for col in columns]

    if primary_keys:
        primary_key_constraint = PrimaryKeyConstraint(*primary_keys)
        columns_list.append(primary_key_constraint)
    # elif 'index' not in columns:
    #     primary_key_constraint = PrimaryKeyConstraint('index')
    #     columns_list.append(Column('index', String(255)))
    #     columns_list.append(primary_key_constraint)

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
