import logging
import os
import random
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

user_agent = os.getenv("EMAIL_ADDRESS")


def format_values(num: int) -> str:
    """
    To make data more readable

    Examples:
    >>> format_values(1_230_000_000_000)
    '1.23T'
    >>> format_values(4_560_000_000)
    '4.56B'
    >>> format_values(7_890_000)
    '7.89M'
    >>> format_values(123)
    '123'
    >>> format_values(-7_890_000_000)
    '-7.89B'
    """
    format_tuples = [(1e12, "T"), (1e9, "B"), (1e6, "M")]
    for threshold, suffix in format_tuples:
        if abs(num) >= threshold:
            return f"{num / threshold:.2f}{suffix}"
    return str(num)


def _format_values(merged_df: pd.DataFrame) -> pd.DataFrame:
    return merged_df.map(format_values)


def fetch_cik(company_name: str = "") -> str:
    """
    GET CIK id for the specified company name. If no company name is passed,
    the function will return a CIK id for a random company.

    :param company_name: str, user-specified company ticker symbol, e.g., 'AMZN' for Amazon.
    :return: str, CIK id of the specified or random company. Must be a width of 10 characters.
    """
    headers = {"User-Agent": user_agent}
    get_url = "https://www.sec.gov/files/company_tickers.json"

    try:
        tickers_data = requests.get(get_url, headers=headers)
        tickers_json = tickers_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return ""

    company_name = company_name.upper()
    if company_name:
        for obj in tickers_json.values():
            if obj["ticker"] == company_name:
                return f'{obj["cik_str"]:010}'
        print(f"Company with ticker {company_name} not found.")
        return ""
    else:
        random_obj = random.choice(list(tickers_json.values()))
        return f'{random_obj["cik_str"]:010}'


# Fix this function
def fetch_sec_api(cik_str: str) -> dict:
    """
    send GET request to the EDGAR database where SEC filings are stored.
    Returns the response data as a JSON object if the request is successful.
    Raises an exception if the request fails.
    """
    try:
        headers = {"User-Agent": user_agent}
        get_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        sec_data = requests.get(get_url, headers=headers)
        sec_data.raise_for_status()
        return sec_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return {}


def clean_company_data(json_file: dict, account_list: list[str]) -> list[pd.DataFrame]:
    """
    clean company JSON data and return a list of DataFrames
    :param json_file: dict: financial data for company
    :param account_list: list of account that user would like to add to df e.g 'Assets', 'Liabilities', etc.
    :return: list: list of dataframes for unique accounts
    """
    company_dfs = []
    for account in account_list:
        try:
            acc_data = json_file["facts"]["us-gaap"][account]["units"]["USD"]
            df = pd.DataFrame.from_dict(acc_data)
            df = df[df["fp"] == "FY"]
            df["year"] = pd.to_datetime(df["end"]).dt.year
            df = df.drop_duplicates(subset=["year"], keep="last")
            df = df[["year", "val"]]
            df = df.rename(columns={"val": account})
            company_dfs.append(df)
        except KeyError as e:
            print(f"df could not be processed for: {e}")
            company_dfs.append(pd.DataFrame({}))
    return company_dfs


def merge_final_df(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """merge list of dfs and return df"""
    cleaned_df_list = [df for df in df_list if not df.empty]

    merged_df = cleaned_df_list[0]
    for cdf in cleaned_df_list[1:]:
        merged_df = pd.merge(merged_df, cdf, on="year", how="outer")

    return merged_df


def add_extra_columns(cleaned_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'valuation', 'ac/l', and 'cf/l' columns to the DataFrame where:
    - EARNINGS_MULTIPLIER is an arbitray multiple used to estimate company value based on future earnings.
    - 'valuation': (YEARS_TO_RECOVER_RETURN * CashFlows) + Cash - LongTermDebt
    - 'ac/l': Ratio of AssetsCurrent to Liabilities.
    - 'cf/l': Ratio of CashFlows to Liabilities.
    """
    # Add 'valuation' column
    EARNINGS_MULTIPLIER = 20
    cleaned_df["valuation"] = (
        (EARNINGS_MULTIPLIER * cleaned_df["CashFlows"])
        + cleaned_df["Cash"]
        - cleaned_df["LongTermDebt"]
    )

    # Add 'ac/l' column
    cleaned_df["ac/l"] = round(
        cleaned_df["AssetsCurrent"] / cleaned_df["Liabilities"], 2
    )

    # Add 'cf/l' column
    cleaned_df["cf/l"] = round(cleaned_df["CashFlows"] / cleaned_df["Liabilities"], 2)

    return cleaned_df


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def elapsed(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"{func.__name__}() runtime: {elapsed_time:.3f}s")
        return result

    return wrapper


@elapsed
def process_financial_data(ticker: str = "META") -> pd.DataFrame:
    specified_accounts = [
        "NetCashProvidedByUsedInOperatingActivities",
        "CashAndCashEquivalentsAtCarryingValue",
        "Liabilities",
        "AssetsCurrent",
        "Revenues",
        "LongTermDebt",
    ]
    accounts_to_drop = ["Revenues", "AssetsCurrent", "Liabilities"]
    accounts_to_rename = {
        "NetCashProvidedByUsedInOperatingActivities": "CashFlows",
        "CashAndCashEquivalentsAtCarryingValue": "Cash",
    }

    comp_cik = fetch_cik(ticker)
    company_data = fetch_sec_api(comp_cik)
    clean_df_list = clean_company_data(company_data, specified_accounts)
    result = merge_final_df(clean_df_list)
    result = result.rename(columns=accounts_to_rename)
    result = add_extra_columns(result)
    result = result.drop(columns=accounts_to_drop)
    result_df = _format_values(result)

    return result_df


if __name__ == "__main__":
    financials_df = process_financial_data(ticker="META")
    print(financials_df)
