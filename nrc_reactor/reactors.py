from data_download import request_to_table
from sql_functions import retreive_reactor_query,retrieve_all_query,retrieve_outages_query
import pandas as pd
import sqlite3
from datetime import datetime

con = sqlite3.connect('./reactors.db')
cursor = con.cursor()

def get_all_reactors(site_has_power:int = None) -> pd.DataFrame:
    """queries database for all reactors. site_has_power is optional, must be 0 or 100, filters reactors by power value
    returns a pandas dataframe object to explore data"""

    query = retrieve_all_query(site_has_power)
    reactors_df = pd.read_sql(query, con)
    print(f'{len(reactors_df)} rows returned from query')
    print(f'Sample Results:\n{reactors_df.head()}')

    return reactors_df

def get_reactor_by_id(unit:str = None, fuzzy_search= False, latest_outage=False) -> pd.DataFrame:
    """queries database for specific reactor. set fuzzy_search=True if only part of the name is known.
    returns a pandas dataframe object to explore data"""
    
    query = retreive_reactor_query(unit, fuzzy_search, latest_outage)
    reactor_df = pd.read_sql(query, con)

    if latest_outage:
        reactor_df['report_datetime'] = reactor_df['report_date'].apply(lambda row: datetime.strptime(row, '%Y-%m-%d'))
        reactor_min_df = reactor_df[reactor_df['report_datetime'] == reactor_df['report_datetime'].min()]
        print(f"Most Recent Outage for {reactor_min_df['unit'].values[0]}: {reactor_min_df['report_date'].values[0]} (YYYY-MM-DD)")
        reactor_df= reactor_df.drop(columns=['report_datetime'])

    print(f'{len(reactor_df)} rows returned from query')
    print(f'Sample Results:\n{reactor_df.head()}')

    return reactor_df

def get_reactors_by_date_range(start_date_string:str, end_date_string:str) -> pd.DataFrame:
    """queries database for specific reactor. date value can be separated by '-' or '/'. 
    datestring should be in month-day-year. returns a pandas dataframe object to explore data"""

    query = retrieve_outages_query(start_date_string, end_date_string)
    reactors_date_df = pd.read_sql(query, con)
    print(f'{len(reactors_date_df)} rows returned from query')
    print(f'Sample Results:\n{reactors_date_df.head()}')

    return reactors_date_df

if __name__ == '__main__':
    request_to_table()
    get_all_reactors(site_has_power=0)
    get_reactor_by_id('Diablo', fuzzy_search=True)
    get_reactors_by_date_range('10-20-2023','10/30/2023')
    get_reactor_by_id('Beaver Valley 1', latest_outage=True)
    get_reactor_by_id('Braidwood', True, True)
