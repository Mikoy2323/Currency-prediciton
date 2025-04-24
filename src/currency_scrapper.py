import requests
import pandas as pd
from datetime import datetime, timedelta


def currency_json_to_df(json_data):
    """
    Converts JSON response from the NBP API into a structured pandas DataFrame.

    Args:
        json_data (dict): JSON data from NBP API containing currency rates.

    Returns:
        pd.DataFrame: DataFrame with columns [name, code, value, date] representing currency information.
    """
    currencies_df = pd.DataFrame(columns=["name", "code", "value"])
    for currency in json_data["rates"]:
        n = len(currencies_df)
        currencies_df.loc[n, "name"] = currency['currency']
        currencies_df.loc[n, "code"] = currency['code']
        currencies_df.loc[n, "value"] = currency['mid']
    currencies_df["date"] = json_data["effectiveDate"]
    return currencies_df


def current_currencies_values():
    """
    Retrieves the latest currency exchange rates from the NBP API (tables 'a' and 'b').

    Returns:
        pd.DataFrame: Combined DataFrame with current exchange rates and source table label.
    """
    currency_df = None
    for table in ["a", "b"]:
        response = requests.get(f'http://api.nbp.pl/api/exchangerates/tables/{table}?format=json')
        currency_df_temp = currency_json_to_df(response.json()[0])
        currency_df_temp["table"] = table
        currency_df = pd.concat([currency_df, currency_df_temp])
    return currency_df.reset_index(drop=True)


def historical_currencies_values():
    """
    Downloads historical currency exchange rates from the NBP API (tables 'a' and 'b'),
    starting from 2023-01-01 up to yesterday.

    Returns:
        pd.DataFrame: Combined DataFrame with historical exchange rates and their respective table labels.
    """
    yesterday = datetime.today() - timedelta(days=1)
    date_range = pd.date_range(start="2023-01-01", end=yesterday.strftime('%Y-%m-%d'))
    historical_df = pd.DataFrame(columns=["name", "code", "value", "table"])
    for table in ["a", "b"]:
        for day in date_range:
            response = requests.get(f'http://api.nbp.pl/api/exchangerates/tables/{table}/{day.date()}/?format=json')
            if response.status_code == 404:
                continue
            df_day = currency_json_to_df(response.json()[0])
            df_day["table"] = table
            historical_df = pd.concat([historical_df, df_day])
    return historical_df.reset_index(drop=True)


