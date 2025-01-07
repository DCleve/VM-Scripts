import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import datetime as dt
import time
import os
import numpy as np
import pytz

login = os.getlogin()
separator = '\\'
startTime = time.time()

gc=gspread.service_account()

import snowflake.connector
from snowflake.connector import connect
snowflake_pull = connect(user='Dave', password='Quantum314!', account='fva14998.us-east-1')

##Import Timestudy Data
file_ts = gc.open_by_key('1ZCBTdfSlfJRr0iRmDErxiRMUfs8nJuzpDTfzg5aZBxc').worksheet('Parse')
file_ts_df = pd.DataFrame.from_dict(file_ts.get_all_records())
file_ts_df.dropna(subset=['Timestamp'], inplace=True)

file_ts_df = file_ts_df.loc[(file_ts_df['Exclude'] == "FALSE") & (file_ts_df['In test and sorted'] == "") & (file_ts_df['In test and unsorted'] == "")]

file_ts_df = file_ts_df[['Run', 'Full Cabinet', 'Cards', 'Density', 'Time Elapsed']]

##Write data to sheet
file_ts_tab = gc.open_by_key('1PFWsUGDnQzmp-VAswxwEG9f6q2Mx_ZRZYhPxh_yKbyI').worksheet('RunGenData')
file_ts_tab.batch_clear(['A1:E'])

pull_tab = gc.open_by_key('1PFWsUGDnQzmp-VAswxwEG9f6q2Mx_ZRZYhPxh_yKbyI').worksheet('SnowflakeData')
pull_tab.batch_clear(['A1:E'])

time.sleep(30)

gd.set_with_dataframe(file_ts_tab, file_ts_df, row=1, col=1)

##Import pulling data
sql_pull = ("""
select
    ppbp.location_name as cabinet
    , ppbp.location_pulled_at_et::date as pulled_date
    , ppbp.number_of_cards as total_cards_pulled_per_location
    , ppbp.number_of_skus as total_skus_per_location
    , ppbp.pull_duration_seconds as total_pull_time_seconds


from analytics.core.paperless_pulling_by_pull as ppbp

where
    ppbp.location_pulled_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_pull)

pull_df = cursor.fetch_pandas_all()
pull_df.drop(pull_df.filter(like='Unnamed'), axis=1, inplace=True)
pull_df.dropna(subset = ['CABINET'], inplace=True)

##Aggragate numbers
pull_df["combined"] = pull_df['CABINET'].astype(str) + pull_df['PULLED_DATE'].astype(str)

cards_per_location = pull_df.groupby('combined')['TOTAL_CARDS_PULLED_PER_LOCATION'].sum()
pull_df = pd.merge(pull_df, cards_per_location, how='right', on='combined')

skus_per_location = pull_df.groupby('combined')['TOTAL_SKUS_PER_LOCATION'].sum()
pull_df = pd.merge(pull_df, skus_per_location, how='right', on='combined')

time_per_location = pull_df.groupby('combined')['TOTAL_PULL_TIME_SECONDS'].sum()
pull_df = pd.merge(pull_df, time_per_location, how='right', on='combined')

pull_df.drop_duplicates(subset='combined', inplace=True)


pull_df.rename(columns={'TOTAL_CARDS_PULLED_PER_LOCATION_y':'TOTAL_CARDS_PULLED_PER_LOCATION', 'TOTAL_SKUS_PER_LOCATION_y':'TOTAL_SKUS_PER_LOCATION', 'TOTAL_PULL_TIME_SECONDS_y':'TOTAL_PULL_TIME_SECONDS'}, inplace=True)

pull_df.drop(['TOTAL_CARDS_PULLED_PER_LOCATION_x', 'TOTAL_SKUS_PER_LOCATION_x', 'TOTAL_PULL_TIME_SECONDS_x', 'combined'], axis=1,  inplace=True)

##Write data to sheet
gd.set_with_dataframe(pull_tab, pull_df, row=1, col=1)





















