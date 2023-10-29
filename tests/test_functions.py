from nrc_reactor import sql_functions as sql

def test_retreive_all():
    assert sql.retrieve_all_query(0) == 'select * from nrc_reactors where power = 0'

def test_retrieve_one():
    assert sql.retreive_reactor_query('Nate',True) == "select * from nrc_reactors where unit like '%Nate%'"

def test_retrieve_range():
    assert sql.retrieve_outages_query('1-20-2023','8/9/2023') == 'select * from nrc_reactors where date(report_date) between "2023-01-20" and "2023-08-09"'