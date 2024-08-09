import os
from dotenv import load_dotenv
import requests
import random
import time
import pandas as pd
import cProfile
import pstats
from pstats import SortKey

load_dotenv()

user_agent = os.getenv('EMAIL_ADDRESS')

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns


def format_values(num):
    """To make data more readable(i.e. 1230000000 => 1.23B)"""
    if abs(num) >= 1e12:
        return "{:.2f}T".format(num / 1e12)
    elif abs(num) >= 1e9:
        return "{:.2f}B".format(num / 1e9)
    elif abs(num) >= 1e6:
        return "{:.2f}M".format(num / 1e6)
    else:
        return num


def convert_df_to_str_data(merged_df):
    final_df = merged_df.map(format_values)
    return final_df


def fetch_cik(company_name: str = "") -> str:
    """
    get CIK id for the specified company name. If no company name is passed,
    the function will return a CIK id for a random company.

    :param company_name: user-specified company ticker symbol, e.g., 'AMZN' for Amazon.
    :return: CIK id of the specified or random company, right zero padded to 10 characters.
    """
    headers = {'User-Agent': user_agent}
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


def fetch_sec_api(cik_str):
    """
    send GET request to the EDGAR database where SEC filings are stored.
    returns json
    """
    try:
        headers = {'User-Agent': 'YourEmail@example.com'}
        get_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        sec_data = requests.get(get_url, headers=headers)
        return sec_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return


def clean_company_data(json_file, account_list):
    """
        clean company json data and return cleaned df
        :param json_file: dict: financial data for company
        :param account_list: list of account that user would like to add to df e.g 'Assets', 'Liabilities', etc.
        :return: list: list of dataframes for unique accounts
    """
    company_dfs = []
    for account in account_list:
        try:
            acc_data = json_file['facts']['us-gaap'][account]['units']['USD']
            df = pd.DataFrame.from_dict(acc_data)
            df = df[df['fp'] == "FY"]
            df['year'] = pd.to_datetime(df['end']).dt.year
            df.drop_duplicates(subset=['year'], keep="last", inplace=True)
            df = df[['year', 'val']]
            df.rename(columns={'val': account}, inplace=True)
            company_dfs.append(df)
        except KeyError as e:
            print(f"df could not be processed for: {e}")
            company_dfs.append({})
    return company_dfs


def merge_final_df(df_list):
    """ merge list of dfs and return df """
    cleaned_df_list = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]

    merged_df = cleaned_df_list[0]
    for cdf in cleaned_df_list[1:]:
        merged_df = pd.merge(merged_df, cdf, on='year', how='outer')

    return merged_df


def drop_columns(cleaned_df, drop_list):
    """ Drop specified columns in drop_list from cleaned_df """
    for col in drop_list:
        try:
            cleaned_df.drop(columns=[col], inplace=True)
        except KeyError as e:
            print(f"Cannot drop: {e}")
    return cleaned_df


def rename_columns(cleaned_df, rename_dict):
    """
    Rename specified columns in cleaned_df
    :param cleaned_df:
    :param rename_dict: key is old name, value is new name e.g. {'original_name': 'rename_value'}
    """
    try:
        cleaned_df.rename(columns=rename_dict, inplace=True)
    except KeyError as e:
        print(f"Cannot rename: {e}")
    return cleaned_df


def add_valuation1_col(cleaned_df):
    """ Add valuation column to final df """
    cleaned_df["valuation"] = (20 * cleaned_df["CashFlows"]) + cleaned_df["Cash"] - cleaned_df["LongTermDebt"]
    valuation_df = cleaned_df
    return valuation_df


def add_current_assets_to_liabilities_ratio(cleaned_df):
    """ Add ratio of AssetsCurrent/Liabilities column named ac/l to final df """
    cleaned_df["ac/l"] = round(cleaned_df["AssetsCurrent"] / cleaned_df["Liabilities"], 2)
    return cleaned_df


def add_cf_to_liabilities_ratio(cleaned_df):
    """ Add ratio of CashFlows/Liabilities column named cf/l to final df """
    cleaned_df["cf/l"] = round(cleaned_df["CashFlows"] / cleaned_df["Liabilities"], 2)
    return cleaned_df


def main():
    start_time = time.perf_counter()

    tick = ""
    specified_accounts = [
        'NetCashProvidedByUsedInOperatingActivities',
        'CashAndCashEquivalentsAtCarryingValue',
        'Liabilities',
        'AssetsCurrent',
        'Revenues',
        'LongTermDebt'
    ]
    accounts_to_drop = [
        "Revenues",
        "AssetsCurrent",
        "Liabilities"
    ]
    accounts_to_rename = {
            'NetCashProvidedByUsedInOperatingActivities': 'CashFlows',
            'CashAndCashEquivalentsAtCarryingValue': 'Cash'
        }

    comp_cik = fetch_cik(tick)
    company_data = fetch_sec_api(comp_cik)
    clean_df_list = clean_company_data(company_data, specified_accounts)
    result = merge_final_df(clean_df_list)
    result = rename_columns(result, accounts_to_rename)
    result = add_valuation1_col(result)
    result = add_cf_to_liabilities_ratio(result)
    result = add_current_assets_to_liabilities_ratio(result)
    result = drop_columns(result, accounts_to_drop)
    result_df = convert_df_to_str_data(result)
    print(result_df)

    end_time = time.perf_counter()
    print(f"\nCode Runtime: {end_time - start_time: .2f}s\n")


if __name__ == "__main__":
    with cProfile.Profile() as profile:
        main()

    p = pstats.Stats(profile)
    p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats('main.py', 6)
