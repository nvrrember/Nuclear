import sqlite3
from datetime import datetime

def retrieve_all_query(state_filter:int = None) -> str:
    """query to return all reactors, with an optional filter for reactor state"""
    valid = {None,0, 100}
    if state_filter not in valid:
        raise ValueError("power filter value must be one of the following values: %r." % valid)

    all_query = 'select * from nrc_reactors'

    if state_filter is not None:
        all_query += f' where power = {state_filter}'

    return all_query

def retreive_reactor_query(unit_id:str, fuzzy_search=False, latest = False) -> str:
    """retrieves the details about a specific reactor. fuzzy searching available for reactors if only part of unit name is known.
    use latest = True to get the latest outage for a reactor"""

    query_selector = lambda fuzzy_val: f"unit like '%{unit_id}%'" if fuzzy_val else f"unit = '{unit_id}'"

    reactor_query = f"select * from nrc_reactors where {query_selector(fuzzy_search)}"
    
    if latest:
        reactor_query = f"select * from nrc_reactors where {query_selector(fuzzy_search)} and power = 0"

    return reactor_query

def retrieve_outages_query(start_date, end_date) -> str:
    """returns all reactors with an outage in the given date range"""

    delimiter = lambda x: '-' if '-' in x else '/'

    start_date = ['-'.join([year, month.zfill(2), day.zfill(2)]) for (month,day,year) in [start_date.split(delimiter(start_date))]][0]
    end_date = ['-'.join([year, month.zfill(2), day.zfill(2)]) for (month,day,year) in [end_date.split(delimiter(end_date))]][0]

    outage_query = f'select * from nrc_reactors where date(report_date) between "{start_date}" and "{end_date}"'

    return outage_query