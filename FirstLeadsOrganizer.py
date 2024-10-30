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

dataTab = gc.open_by_key('17IAGbeOqjKxE_8qTAurIx67L2GUyNlNwSktdWuHj_-A').worksheet('Data')

##Import SQ Data
pvp_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "PVP.csv"]
pvp_result = separator.join(pvp_string)
pvp_df = pd.read_csv(pvp_result)

##Fix data
pvp_df['CREATED_AT'] = pd.to_datetime(pvp_df['CREATED_AT']).dt.date

pvp_df["sq_type"] = pvp_df['SHIPPINGQUEUENUMBER'].str[-3:]
pvp_df.loc[pvp_df['SHIPPINGQUEUENUMBER'].map(len) == 16, 'sq_type'] = pvp_df['SHIPPINGQUEUENUMBER'].str[-6:]
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'sq_type'] = "POQ"

pvp_df.loc[pvp_df['sq_type'] == 'POQ', 'SHIPPINGQUEUENUMBER'] = pvp_df['QUEUE_NUMBER']

pvp_df.drop('QUEUE_NUMBER', axis=1, inplace=True)

##Write data to sheet
dataTab.batch_clear(['A1:E'])
gd.set_with_dataframe(dataTab, pvp_df)

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