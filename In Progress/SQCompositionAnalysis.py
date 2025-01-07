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

###Import data
sq_sql = ("""
select
    shippingqueue.shippingqueuenumber as sq_number
    , shippingqueue.ordercount as order_count
    , shippingqueue.productcount as card_count
    , shippingqueue.updatedat as updated_at
    , shippingqueue.createdat as created_at

from
hvr_tcgstore_production.tcgd.shippingqueue

where
    shippingqueue.updatedat::date >= dateadd(dd, -50, getdate())::date

""")

cursor = snowflake_pull.cursor()
cursor.execute(sq_sql)

sq_df = cursor.fetch_pandas_all()
sq_df.drop(sq_df.filter(like='Unnamed'), axis=1, inplace=True)
sq_df.dropna(subset = ['SQ_NUMBER'], inplace=True)

##Import Shift Data
shift = gc.open_by_key('1Xq6I5LWxUvqRQ3kw8aBHFyPYmTJlajIfeMbWzzmggi4').worksheet('FilteredData')
shiftdata_df = pd.DataFrame.from_dict(shift.get_all_records())
shiftdata_df.dropna(subset=["Date"], inplace=True)
shiftdata_df.drop(shiftdata_df.filter(like='Unnamed'), axis=1, inplace=True)
shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Role', 'Regular Hours']]
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date'])
shiftdata_df['Date'] = shiftdata_df['Date'].dt.date

shiftdata_df = shiftdata_df.loc[(shiftdata_df['Regular Hours'] != '-') & (shiftdata_df['Regular Hours'].astype('float64') > 0)]
shiftdata_df['Primary Email'] = shiftdata_df['Primary Email'].str.lower()

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Length','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)']]
staffing_df.rename(columns={'Email':'Puncher'}, inplace=True)

nameChanges = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('NameChanges')
nameChanges_df = pd.DataFrame.from_dict(nameChanges.get_all_records())
nameChanges_df.dropna(subset=['Current Preferred Name'], inplace=True)
nameChanges_df.rename(columns={'Current Preferred Name':'Preferred Name'}, inplace=True)

slimNameChanges_df = nameChanges_df.copy()
slimNameChanges_df.rename(columns={'Former Email':'Primary Email'}, inplace=True)

nameChanges_df = pd.merge(nameChanges_df, staffing_df, how='left', on='Preferred Name')
nameChanges_df.drop(['Former Preferred Name', 'Current Email', 'Puncher'], axis=1, inplace=True)
nameChanges_df.rename(columns={'Former Email':'Puncher'}, inplace=True)

staffing_df = pd.concat([staffing_df, nameChanges_df])

staffing_df['Puncher'] = staffing_df['Puncher'].str.lower()

##Combine shift data with staffing data
staffing_df = staffing_df[['Puncher', 'Shift Name', 'Preferred Name', 'Shift Length']]
staffing_df.rename(columns={'Puncher':'Primary Email'}, inplace=True)

shiftdata_df = pd.merge(shiftdata_df, staffing_df, how='left', on='Primary Email')
shiftdata_df = pd.merge(shiftdata_df, slimNameChanges_df, how='left', on='Primary Email')

##Last 32 days
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date']).dt.date
shiftdata_df["now"] = pd.Timestamp.now()
shiftdata_df['now'] = shiftdata_df['now'].dt.date
shiftdata_df = shiftdata_df.loc[shiftdata_df['Date'] > (shiftdata_df['now'] - timedelta(days = 360))]

shiftdata_df = shiftdata_df.loc[shiftdata_df['Shift Length'].astype('float64') > 0]

shiftdata_df = shiftdata_df[['Date', 'Preferred Name_x', 'Shift Name', 'Regular Hours']]

##Write data to sheet
dataTab = gc.open_by_key('1fJtuzORJlYyBhqjQo7Rw5unWwUXisFDcEd_Cyb2JwZI').worksheet('Data')
shiftTab = gc.open_by_key('1fJtuzORJlYyBhqjQo7Rw5unWwUXisFDcEd_Cyb2JwZI').worksheet('Shift')

dataTab.batch_clear(['A1:E'])
shiftTab.clear()

gd.set_with_dataframe(dataTab, sq_df, row=1, col=1)
gd.set_with_dataframe(shiftTab, shiftdata_df)