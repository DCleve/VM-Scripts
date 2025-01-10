import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
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

##Import PVP SQ Data
sql_slot = ("""
select
    shippingqueue.shippingqueuenumber as shippingqueuenumber
    , shippingqueue.ordercount as order_count
    , shippingqueue.productcount as product_count
    , shippingqueue.createdat as created_at

from hvr_tcgstore_production.tcgd.shippingqueue

where
    //shippingqueue.createdat::date >= '2024-11-06'
    shippingqueue.createdat::date >= '2023-01-01'
    and shippingqueue.createdat::date < '2023-02-01'

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_slot)

data_df = cursor.fetch_pandas_all()
data_df.drop(data_df.filter(like='Unnamed'), axis=1, inplace=True)
data_df.dropna(subset = ['SHIPPINGQUEUENUMBER'], inplace=True)

data_df['ORDER_COUNT'] = data_df['ORDER_COUNT'].astype('float64')
data_df['PRODUCT_COUNT'] = data_df['PRODUCT_COUNT'].astype('float64')


data_df['CREATED_AT'] = pd.to_datetime(data_df['CREATED_AT'])
data_df['CREATED_AT'] = data_df['CREATED_AT'].dt.date

##SQ Types
data_df["sq_type"] = data_df['SHIPPINGQUEUENUMBER'].str[-3:]
data_df.loc[data_df['SHIPPINGQUEUENUMBER'].map(len) == 16, 'sq_type'] = data_df['SHIPPINGQUEUENUMBER'].str[-6:]

##Aggragate orders per SQ type
total_count = data_df.groupby('sq_type')['ORDER_COUNT'].sum()
data_df = pd.merge(data_df, total_count, how='right', on='sq_type')
data_df.rename(columns={'ORDER_COUNT_x':'ORDER_COUNT', 'ORDER_COUNT_y':'total_order_count_by_sq_type'}, inplace=True)

total_days = data_df.groupby('sq_type')['CREATED_AT'].nunique()
data_df = pd.merge(data_df, total_days, how='right', on='sq_type')
data_df.rename(columns={'CREATED_AT_x':'CREATED_AT', 'CREATED_AT_y':'unique_generation_days'}, inplace=True)

total_sqs = data_df.groupby('sq_type')['SHIPPINGQUEUENUMBER'].nunique()
data_df = pd.merge(data_df, total_sqs, how='right', on='sq_type')
data_df.rename(columns={'SHIPPINGQUEUENUMBER_x':'SHIPPINGQUEUENUMBER', 'SHIPPINGQUEUENUMBER_y':'total_unique_sqs'}, inplace=True)

##Reduce to unique sq type
data_df.drop_duplicates(subset=['sq_type'], inplace=True)

##Further math
data_df["orders_per_sq"] = data_df['total_order_count_by_sq_type'].astype('float64') / data_df['total_unique_sqs'].astype('float64')

data_df["keep"] = 0

data_df.loc[data_df['unique_generation_days'].astype('float64') > 0, 'keep'] = 1

total_orders = data_df.groupby('keep')['total_order_count_by_sq_type'].sum()
data_df = pd.merge(data_df, total_orders, how='right', on='keep')
data_df.rename(columns={'total_order_count_by_sq_type_x':'total_order_count_by_sq_type', 'total_order_count_by_sq_type_y':'total_orders'}, inplace=True)

data_df["order_proportion"] = data_df['total_order_count_by_sq_type'].astype('float64') / data_df['total_orders'].astype('float64')

data_df["orders_per_day"] = data_df['total_order_count_by_sq_type'].astype('float64') / data_df['unique_generation_days'].astype('float64')

##Create final dataframe
data_df = data_df[['sq_type', 'total_order_count_by_sq_type', 'orders_per_sq', 'order_proportion', 'orders_per_day', 'total_orders',]]

##Write data to sheet
dataDoc = gc.open_by_key('1AHtwA2alIi3prgNbbRHlbJv4hDQKQ2e0wh1K6pfP6Bw')
dataTab = dataDoc.worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, data_df)