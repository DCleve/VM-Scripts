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

##Import PVP SQ Data
data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqslot.csv"]
data_result = separator.join(data_string)
data_df = pd.read_csv(data_result)

data_df['ORDER_COUNT'] = data_df['ORDER_COUNT'].astype('float64')
data_df['card_qty_by_slot'] = data_df['card_qty_by_slot'].astype('float64')
data_df['unique_pcids_by_slot'] = data_df['unique_pcids_by_slot'].astype('float64')

data_df['CREATED_AT'] = pd.to_datetime(data_df['CREATED_AT'])
data_df['CREATED_AT'] = data_df['CREATED_AT'].dt.date

##Only look at last 30 days
data_df["now"] = pd.Timestamp.now()
data_df['now'] = data_df['now'].dt.date

data_df = data_df.loc[(data_df['CREATED_AT'] + timedelta(days = 30)) >= data_df['now']]

##SQ Types
data_df.loc[data_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'SHIPPINGQUEUENUMBER'] = data_df['QUEUE_NUMBER']
data_df["sq_type"] = data_df['SHIPPINGQUEUENUMBER'].str[-3:]
data_df.loc[(data_df['SHIPPINGQUEUENUMBER'].map(len) == 16) & (data_df['SHIPPINGQUEUENUMBER'].str[-3:] != 'POQ'), 'sq_type'] = data_df['SHIPPINGQUEUENUMBER'].str[-6:]

##Aggragate PCIDs and cards
pcid_count = data_df.groupby('SHIPPINGQUEUENUMBER')['unique_pcids_by_slot'].sum()
data_df = pd.merge(data_df, pcid_count, how='right', on='SHIPPINGQUEUENUMBER')
data_df.rename(columns={'unique_pcids_by_slot_x':'UNIQUE_PCIDS', 'unique_pcids_by_slot_y':'pcid_count_by_sq'}, inplace=True)

card_count = data_df.groupby('SHIPPINGQUEUENUMBER')['card_qty_by_slot'].sum()
data_df = pd.merge(data_df, card_count, how='right', on='SHIPPINGQUEUENUMBER')
data_df.rename(columns={'card_qty_by_slot_x':'PRODUCT_COUNT', 'card_qty_by_slot_y':'card_count_by_sq'}, inplace=True)

##Density
data_df["density"] = data_df['card_count_by_sq'] / data_df['pcid_count_by_sq']

##Parse down to single SQs
data_df.drop_duplicates(subset=['SHIPPINGQUEUENUMBER'], inplace=True)

##Remove garbage SQs
data_df = data_df.loc[data_df['density'].astype('float64') < 100]

##Order count math
median_count = data_df.groupby('sq_type')['ORDER_COUNT'].median()
data_df = pd.merge(data_df, median_count, how='right', on='sq_type')
data_df.rename(columns={'ORDER_COUNT_x':'ORDER_COUNT', 'ORDER_COUNT_y':'median_order_count_by_sq_type'}, inplace=True)

total_count = data_df.groupby('sq_type')['ORDER_COUNT'].sum()
data_df = pd.merge(data_df, total_count, how='right', on='sq_type')
data_df.rename(columns={'ORDER_COUNT_x':'ORDER_COUNT', 'ORDER_COUNT_y':'total_order_count_by_sq_type'}, inplace=True)

##Average density
average_density = data_df.groupby('sq_type')['density'].mean()
data_df = pd.merge(data_df, average_density, how='right', on='sq_type')
data_df.rename(columns={'density_x':'density', 'density_y':'average_density'}, inplace=True)

##Parse down to unique SQ types
data_df.drop_duplicates(subset=['sq_type'], inplace=True)

data_df.sort_values(by=['sq_type'], ascending=[True], inplace=True)

##Order percentage of total
data_df["task"] = "shipping"
data_df['total_order_count_by_sq_type'] = data_df['total_order_count_by_sq_type'].astype('float64')

total_order_count = data_df.groupby('task')['total_order_count_by_sq_type'].sum()
data_df = pd.merge(data_df, total_order_count, how='right', on='task')
data_df.rename(columns={'total_order_count_by_sq_type_x':'sq_type_order_count', 'total_order_count_by_sq_type_y':'overall_order_count'}, inplace=True)

data_df['overall_order_count'] = data_df['overall_order_count'].astype('float64')

data_df["percentage_of_total"] = data_df['sq_type_order_count'] / data_df['overall_order_count']

##Parse down dataframe
data_df = data_df[['sq_type', 'median_order_count_by_sq_type', 'sq_type_order_count', 'average_density', 'percentage_of_total']]

##Write to 48 doc
doc48 = gc.open_by_key('1fxU133MtAZZuoeZFOUd4w5-pusmvTM8nfUE4LN3E7Yk')
shippingTab = doc48.worksheet('3. Shipping Production Plan')
shippingTab.batch_clear(['W1:AA35'])
gd.set_with_dataframe(shippingTab, data_df, row=1, col=23)

##Update audit log
csv_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Audit CSVs", "AuditLog.csv"]
result = separator.join(csv_string)
audit_df = pd.read_csv(result)

executionTime = (time.time() - startTime)
dt_string = datetime.now(pytz.timezone('America/New_York')).strftime("%m/%d/%Y %H:%M:%S")
script_name = os.path.basename(__file__).replace('.py', '')

new_audit = {'Timestamp': dt_string, 'Execution Time': executionTime, 'Script': script_name}

new_audit_df = pd.DataFrame(data=new_audit, index=[0])

audit_df = pd.concat([audit_df, new_audit_df])
audit_df.dropna(subset=["Timestamp"], inplace=True)

audit_df.to_csv(result, index=False)