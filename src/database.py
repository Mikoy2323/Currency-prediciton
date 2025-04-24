import sqlite3
import src.currency_scrapper as curr_scrap
import pandas as pd
from pathlib import Path


def check_table_exists(table_name):
    """
    Checks if a table with the given name exists in the SQLite database.

    Args:
        table_name (str): Name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name=?
    """, (table_name,))
    result = cursor.fetchone() is not None
    conn.close()
    return result


def check_num_of_records(table_name):
    """
    Returns the number of records in the specified table.

    Args:
        table_name (str): Table name to count records in.

    Returns:
        int: Number of records.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    result = cursor.fetchone()
    conn.close()
    return result[0]


def check_table_empty(table_name):
    """
    Checks if a table is empty.

    Args:
        table_name (str): Table name.

    Returns:
        bool: True if the table is empty, False otherwise.
    """
    return check_num_of_records(table_name) == 0


def return_values(table_name, col="*", cond=""):
    """
    Returns data from a given table with optional column selection and condition.

    Args:
        table_name (str): Table name.
        col (str): Columns to retrieve (default "*").
        cond (str): SQL condition (default "").

    Returns:
        list: Retrieved rows, flattened if single column is selected.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    cursor.execute(f'SELECT {col} FROM {table_name} {cond}')
    result = cursor.fetchall()
    if col != "*" and "," not in col:
        result = [res[0] for res in result]
    conn.close()
    return result if result is not None else []


def drop_table(table_name):
    """
    Drops a table from the database if it exists.

    Args:
        table_name (str): Table name.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    conn.close()


def create_table(table_name, cols, col_types):
    """
    Creates a new table with specified columns and types.

    Args:
        table_name (str): Table name.
        cols (list): Column names.
        col_types (list): Corresponding column types.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    query = ", ".join([f"{col} {col_type}" for col, col_type in zip(cols, col_types)])
    cursor.execute(f'CREATE TABLE {table_name}({query})')
    conn.commit()
    conn.close()


def table_creator():
    """
    Creates required database tables (Currencies, ExchangeRates, Predictions) if they don't exist.
    """
    if not check_table_exists("Currencies"):
        create_table("Currencies",
                     ["currency_id", "name", "code", "rmape", "table_val"],
                     ["INTEGER", "TEXT", "TEXT", "FLOAT", "TEXT"])

    if not check_table_exists("ExchangeRates"):
        create_table("ExchangeRates",
                     ["rate_id", "currency_id", "date", "value"],
                     ["INTEGER", "INTEGER", "DATE", "FLOAT"])

    if not check_table_exists("Predictions"):
        create_table("Predictions",
                     ["prediction_id", "currency_id", "date", "value"],
                     ["INTEGER", "INTEGER", "DATE", "FLOAT"])


def currencies_insert():
    """
    Inserts current currencies from the NBP API into the Currencies table (if empty),
    along with RMAPE scores loaded from a CSV file.
    """
    if check_table_empty("Currencies"):
        conn = sqlite3.connect('currencies.db')
        cursor = conn.cursor()
        query = '''INSERT INTO Currencies("currency_id", "name", "code", "rmape", "table_val") VALUES (?, ?, ?, ?, ?)'''

        currencies_df = curr_scrap.current_currencies_values()
        rmape = pd.read_csv('model_rmape.csv', index_col=0)

        data = list(zip(range(len(currencies_df)),
                        currencies_df["name"],
                        currencies_df["code"],
                        rmape["rmape"],
                        currencies_df["table"]))
        cursor.executemany(query, data)
        conn.commit()
        conn.close()


def exchange_rates_insert(initial=False):
    """
    Inserts exchange rates into the ExchangeRates table.
    If `initial` is True, fetches historical data; otherwise, fetches current data.
    Skips records already present in the database.

    Args:
        initial (bool): Whether to fetch historical data (True) or current data (False).

    Returns:
        pd.DataFrame: DataFrame of inserted records.
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()
    values_in_db = return_values("ExchangeRates", "currency_id")

    query = '''INSERT INTO ExchangeRates("rate_id", "currency_id", "date", "value") VALUES (?, ?, ?, ?)'''
    df = curr_scrap.historical_currencies_values() if initial else curr_scrap.current_currencies_values()

    data = []
    for idx in range(len(df)):
        name_val = df.loc[idx, "name"]
        cursor.execute(f'SELECT currency_id FROM "Currencies" WHERE name="{name_val}"')
        result = cursor.fetchone()
        if result is not None: 
            if result[0] not in values_in_db:
                data.insert(idx, (idx, result[0], df.loc[idx, "date"], df.loc[idx, "value"]))
    cursor.executemany(query, data)
    conn.commit()
    conn.close()
    return df


def predictions_insert(predictions_df, tomorrow):
    """
    Inserts model predictions into the Predictions table for a given date.

    Args:
        predictions_df (pd.DataFrame): DataFrame with prediction results.
        tomorrow (str): Date for which predictions are made (in YYYY-MM-DD format).
    """
    conn = sqlite3.connect('currencies.db')
    cursor = conn.cursor()

    query = '''INSERT INTO Predictions("prediction_id", "currency_id", "date", "value") VALUES (?, ?, ?, ?)'''
    data = list(zip(range(len(predictions_df)),
                    predictions_df["currency_id"],
                    [tomorrow] * len(predictions_df),
                    predictions_df["prediction"]))
    cursor.executemany(query, data)
    conn.commit()
    conn.close()


def database_init_pipeline():
    """
    Initializes the entire database pipeline:
    - Drops all existing tables
    - Creates fresh schema
    - Inserts current currency data and historical exchange rates
    """
    drop_table("Predictions")
    drop_table("ExchangeRates")
    drop_table("Currencies")
    table_creator()
    currencies_insert()
    exchange_rates_insert(initial=True)
