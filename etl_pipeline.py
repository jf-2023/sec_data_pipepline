import cProfile
import pstats
import random
import time
from pstats import SortKey

import pandas as pd
import requests


def format_values(num):
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
    return num


def _format_values(merged_df):
    return merged_df.map(format_values)


def fetch_cik(company_name=None):
    """
    GET CIK id for the specified company name. If no company name is passed,
    the function will return a CIK id for a random company.

    :param company_name: str, user-specified company ticker symbol, e.g., 'AMZN' for Amazon.
    :return: str, CIK id of the specified or random company. Must be a width of 10 characters.
    """
    headers = {"User-Agent": "YourEmail@example.com"}
    get_url = "https://www.sec.gov/files/company_tickers.json"

    try:
        tickers_data = requests.get(get_url, headers=headers)
        tickers_json = tickers_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

    company_name = str(company_name.upper())
    if company_name:
        for obj in tickers_json.values():
            if obj["ticker"] == company_name:
                return f'{obj["cik_str"]:010}'
        print(f"Company with ticker {company_name} not found.")
        return None
    else:
        random_obj = random.choice(list(tickers_json.values()))
        return f'{random_obj["cik_str"]:010}'


def fetch_sec_api(cik_str):
    """
    send GET request to the EDGAR database where SEC filings are stored.
    returns json
    """
    try:
        headers = {"User-Agent": "YourEmail@example.com"}
        get_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        sec_data = requests.get(get_url, headers=headers)
        return sec_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return


def clean_company_data(json_file, account_list):
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


def merge_final_df(df_list):
    """merge list of dfs and return df"""
    cleaned_df_list = [df for df in df_list if not df.empty]

    merged_df = cleaned_df_list[0]
    for cdf in cleaned_df_list[1:]:
        merged_df = pd.merge(merged_df, cdf, on="year", how="outer")

    return merged_df


def drop_columns(cleaned_df, drop_list):
    """Drop specified columns in drop_list from cleaned_df"""
    for col in drop_list:
        try:
            cleaned_df = cleaned_df.drop(columns=[col])
        except KeyError as e:
            print(f"Cannot drop: {e}")
    return cleaned_df


def add_extra_columns(cleaned_df):
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


def main():
    start_time = time.perf_counter()

    tick = "META"
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

    comp_cik = fetch_cik(tick)
    company_data = fetch_sec_api(comp_cik)
    clean_df_list = clean_company_data(company_data, specified_accounts)
    result = merge_final_df(clean_df_list)
    result = result.rename(columns=accounts_to_rename)
    result = add_extra_columns(result)
    result = drop_columns(result, accounts_to_drop)
    result_df = _format_values(result)

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(result_df)

    end_time = time.perf_counter()
    print(f"\nCode Runtime: {end_time - start_time: .2f}s\n")


if __name__ == "__main__":
    with cProfile.Profile() as profile:
        main()

    p = pstats.Stats(profile)
    p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats("main.py", 6)
