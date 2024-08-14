"""
This module contains functions for cleaning the financial data fetched from the SEC API.

Current issues in cleaning the SEC API data arise from the nature of accounting standards:
- Data can be found in either 'us-gaap' or 'ifrs-full' objects.
- If entity follows 'ifrs-full' reporting standards, the monetary unit of the entity must be found in the object itself.

(Note: The placeholder functions below work only for companies reporting under 'us-gaap' standards.)

This module is currently in progress until a clear and concise method to handle the different data routes is found.
"""

import pandas as pd


# Placeholder functions for data cleaning
def prep_data(json_data: dict) -> dict:
    """Parse json_data get 'us-gaap' or 'ifrs-full' object"""
    try:
        data = json_data.get("facts", {})
        if "us-gaap" in data:
            data_object = data.get("us-gaap")
        elif "ifrs-full" in data:
            data_object = data.get("ifrs-full")
        else:
            print(f"Error on this data object: {data}")
            data_object = {}
        return data_object
    except KeyError as e:
        print(f"Could not parse: {e}")


def get_df(data: dict, account: str) -> pd.DataFrame:
    """Clean company json data from fetch_file() and return a pandas DataFrame."""
    try:
        acc_data = data[account]["units"]["USD"]
        df = pd.DataFrame.from_dict(acc_data)
        df = df[df["fp"] == "FY"]
        df["year"] = pd.to_datetime(df["end"]).dt.year
        df.drop_duplicates(subset=["year"], keep="last", inplace=True)
        df = df[["year", "val"]]
        df.rename(columns={"val": account}, inplace=True)
        return df
    except KeyError as e:
        print(f"df could not be processed for: {e}")
        return pd.DataFrame({})


def get_assets_df(prepped_json: dict) -> pd.DataFrame:
    assets_df = get_df(prepped_json, "Assets")
    return assets_df


def get_liabilities_df(prepped_json: dict) -> pd.DataFrame:
    liabilities_df = get_df(prepped_json, "Liabilities")
    return liabilities_df


def get_equity_df(prepped_json: dict) -> pd.DataFrame:
    equity_df = get_df(prepped_json, "StockholdersEquity")
    if equity_df == pd.DataFrame({}):
        equity_df = get_df(
            prepped_json,
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        )
    return equity_df


def merge_final_df(
    assets_df: pd.DataFrame, equity_df: pd.DataFrame, liabilities_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge assets, equity, and liabilities DataFrames."""
    merged_df = pd.merge(assets_df, equity_df, on="year", how="outer")
    if liabilities_df == pd.DataFrame({}):
        merged_df["Liabilities"] = merged_df["Assets"] - merged_df["Equity"]
    else:
        merged_df = pd.merge(merged_df, liabilities_df, on="year", how="outer")

    return merged_df


def clean_company_data_using_dataframes(data_json: dict) -> dict:
    """
    Clean company json data using pandas DataFrames and return a dictionary of the three
    fundamental accounting categories 'Assets', 'Liabilities', and 'Equity'.

    :param data_json: dict: Financial data for the company.
    :return dict: Cleaned financial data.
    """
    final_df = merge_final_df(
        get_assets_df(data_json),
        get_equity_df(data_json),
        get_liabilities_df(data_json),
    )
    return final_df.to_dict()


def clean_company_data(data_json: dict) -> dict:
    """
    Placeholder function to clean company data without leaving dictionary form.
    Actual implementation is in progress.
    """
    # Implement logic
    return data_json
