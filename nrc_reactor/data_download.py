import requests
import sqlite3

data_download_url = 'https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt'

def request_to_table():
    """send get request to url to extract data for reactors for last 365 days, and insert into reactor_db"""

    con = sqlite3.connect('./reactors.db')
    cursor = con.cursor()

    #create table
    cursor.execute('CREATE TABLE if not exists nrc_reactors(report_date text, unit text, power integer)')

    #truncate table for new results if populated
    if len(cursor.execute("select * from nrc_reactors limit 5").fetchall()) > 1:
        print('Truncating table nrc_reactors')
        cursor.execute('delete from nrc_reactors')

    data_response = requests.get(data_download_url)
    data_text = data_response.text

    #format string for date and get list from split
    formatted_data_list = data_text.replace('\r','').replace(' 12:00:00 AM','').split('\n')
    formatted_data_list = [row.split('|') for row in formatted_data_list[1:] if row != ' ']
    finalized_data_list = [['-'.join([year, month.zfill(2), day.zfill(2)]),row[1],row[2]] for row in formatted_data_list for (month,day,year) in [row[0].split('/')]]
    print(f'{len(finalized_data_list)} rows of reactor data received from url')

    cursor.executemany('INSERT INTO nrc_reactors VALUES(?, ?, ?)', finalized_data_list)
    con.commit()

    print(f'{len(cursor.execute("select * from nrc_reactors").fetchall())} rows in database table nrc_reactors')

if __name__ == '__main__':
    request_to_table()