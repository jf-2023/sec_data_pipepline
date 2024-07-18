import requests
import random
import time
import pandas as pd

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


def fetch_cik(company_name=None):
    """
    GET CIK id for the specified company name. If no company name is passed,
    the function will return a CIK id for a random company.

    :param company_name: str, user-specified company ticker symbol, e.g., 'AMZN' for Amazon.
    :return: str, CIK id of the specified or random company. Must be a width of 10 characters.
    """
    headers = {'User-Agent': 'mr.muffin235@gmail.com'}
    # headers = {'User-Agent': 'YourEmail@example.com'}
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
        headers = {'User-Agent': 'mr.muffin235@gmail.com'}
        # headers = {'User-Agent': 'YourEmail@example.com'}
        get_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        sec_data = requests.get(get_url, headers=headers)
        return sec_data.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return


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

tick = "MSFT".upper()
comp_cik = fetch_cik(tick)
company_data = fetch_sec_api(comp_cik)
clean_df_list = clean_company_data(company_data)

result = merge_final_df(clean_df_list)
complete_df = add_valuation1_col(result)
complete_df = add_cf_to_liabilities_ratio(complete_df)
complete_df = add_current_assets_to_liabilities_ratio(complete_df)
complete_df = drop_unused_columns(complete_df)
result_df = convert_df_to_str_data(complete_df)
print(result_df)

end_time = time.perf_counter()
print(f"\nCode Runtime: {end_time - start_time: .2f}s")
