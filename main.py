import json
import random
import time
import os
import pandas as pd

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns

"""
beartype:


"""


# @beartype
# def format_values(num) -> int:
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


def get_random_ticker():
    """
    Return json data from a random file in depository
    :return: dict
    """
    file_path = "C:\\Users\\cornf\\Documents\\stockTickers.json"
    with open(file_path, "r") as file:
        tickers_list = json.load(file)
    ticker_amount = len(tickers_list)
    random_num = random.randint(0, ticker_amount)
    rand_tick = tickers_list[random_num]
    return rand_tick


def get_cik_of_ticker(company):
    """
    Return json data from a random file in depository
    :return: dict
    """
    file_path = "C:\\Users\\cornf\\Documents\\stockTickers.json"
    with open(file_path, "r") as file:
        tickers_list = json.load(file)
    for ticker in tickers_list:
        if ticker['ticker'] == company:
            cik_str = f"{ticker['cik_str']:010}"
            return cik_str


def get_company_data(ticker):
    """
    Searches directory and opens file for requested ticker data.

    :param ticker: str: The ticker of the company.
    :return: dict or None: The company data if found, otherwise None.
    """
    directory_path = 'C:/Users/cornf/Documents/companyFacts/'
    specific_file = f"CIK{ticker}.json"
    file_path = os.path.join(directory_path, specific_file)

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        print(f"File {specific_file} not found")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file {specific_file}")


def clean_company_data(json_file):
    """
    clean company json data and return cleaned df
    :param json_file: dict: financial data for company
    :return: list: list of dataframes for unique accounts
    """
    account_lists = [
        'NetCashProvidedByUsedInOperatingActivities',
        'CashAndCashEquivalentsAtCarryingValue',
        'Liabilities',
        'AssetsCurrent',
        'Revenues',
        'LongTermDebt'
    ]
    company_dfs = []
    # 1. do parallelization
    # 2. keep in dictionary
    for account in account_lists:
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
    """
    merge list of dfs and return df
    :param df_list: list: list of dataframes for unique accounts for the company
    :return: df: formatted df of company financials
    """
    cleaned_df_list = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]

    merged_df = cleaned_df_list[0]
    # Merge each DataFrame in the list on the 'year' column
    for cdf in cleaned_df_list[1:]:
        merged_df = pd.merge(merged_df, cdf, on='year', how='outer')

    merged_df.rename(columns={
        'NetCashProvidedByUsedInOperatingActivities': 'CashFlows',
        'CashAndCashEquivalentsAtCarryingValue': 'Cash'
    }, inplace=True)
    return merged_df

    # Filter out any empty dictionaries or non-DataFrame objects from the list


"""
    clean_df_list = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]

    if not clean_df_list:
        return pd.DataFrame()  # Return an empty DataFrame if no valid DataFrames found

    combined_df = pd.concat(clean_df_list, ignore_index=True)
    combined_df = combined_df.applymap(format_values)
    return combined_df

"""


def add_valuation1_col(cleaned_df):
    """
    Add valuation column to final df
    :param cleaned_df:
    :return: df: clean df with new valuation column
    """
    cleaned_df["valuation"] = cleaned_df["CashFlows"] + cleaned_df["Cash"]
    valuation_df = cleaned_df
    return valuation_df


def add_current_assets_to_liabilities_ratio(cleaned_df):
    """
    Add ratio of AssetsCurrent/Liabilities column named ac/l to final df
    :param cleaned_df:
    :return: df: return cleaned_df with new column
    """
    cleaned_df["ac/l"] = round(cleaned_df["AssetsCurrent"] / cleaned_df["Liabilities"], 2)
    return cleaned_df


def add_cf_to_liabilities_ratio(cleaned_df):
    """
    Add ratio of CashFlows/Liabilities column named cf/l to final df
    :param cleaned_df:
    :return: df: return cleaned_df with new column
    """
    cleaned_df["cf/l"] = round(cleaned_df["CashFlows"] / cleaned_df["Liabilities"], 2)
    return cleaned_df


def drop_unused_columns(cleaned_df):
    """
    Drop unused columns from cleaned_df
    :param cleaned_df:
    :return: cleaned_df
    """
    drop_list = ["Revenues", "AssetsCurrent", "Liabilities"]
    for col in drop_list:
        try:
            cleaned_df.drop(columns=[col], inplace=True)
        except KeyError as e:
            print(f"Cannot drop: {e}")
    return cleaned_df


start_time = time.perf_counter()

# print(get_random_ticker())
tick = "MSFT".upper()
comp_cik = get_cik_of_ticker(tick)
company_data = get_company_data(comp_cik)
clean_df_list = clean_company_data(company_data)
result = merge_final_df(clean_df_list)
complete_df = add_valuation1_col(result)
complete_df = add_cf_to_liabilities_ratio(complete_df)
complete_df = add_current_assets_to_liabilities_ratio(complete_df)
complete_df = drop_unused_columns(complete_df)
result_df = convert_df_to_str_data(complete_df)
print(result_df)

end_time = time.perf_counter()
print(f"Runtime: {end_time - start_time: .5f}s")

"""
For LEVI I looked at: 
    - make new file system and put current code on there and start using git
    - 'add column for enteprise value for each year'
        - [(3 year ave cash flow) + (3 year ave cash)] - (3 year ave Liabilities) 

    - 'Get Market Cap'
    - 'Compare valuation to marketcap'

    git init
"""
