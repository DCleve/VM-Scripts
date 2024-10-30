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

##Import data
dataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
dataTab_result = separator.join(dataTab_string)
dataTab_df = pd.read_csv(dataTab_result)

parsedDataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "ParsedData.csv"]
parsedDataTab_result = separator.join(parsedDataTab_string)
parsedDataTab_df = pd.read_csv(parsedDataTab_result)

##Parse out data
dataTab_df["Punch"] = dataTab_df['Data'].str.split('|').str[0]
dataTab_df["First Offset"] = dataTab_df['Data'].str.split('|').str[1]
dataTab_df["Puncher"] = dataTab_df['Data'].str.split('|').str[2]
dataTab_df["Units"] = dataTab_df['Data'].str.split('|').str[3]
dataTab_df["Subtask"] = dataTab_df['Data'].str.split('|').str[4]
dataTab_df["Day %"] = dataTab_df['Data'].str.split('|').str[5]
dataTab_df["Earned Hours"] = dataTab_df['Data'].str.split('|').str[6]
dataTab_df["Task"] = dataTab_df['Data'].str.split('|').str[7]
dataTab_df["adjusted_shift_length"] = dataTab_df['Data'].str.split('|').str[8]
dataTab_df["Total Day %"] = dataTab_df['Data'].str.split('|').str[9]
dataTab_df["Total Earned Hours"] = dataTab_df['Data'].str.split('|').str[10]
dataTab_df["hours_worked"] = dataTab_df['Data'].str.split('|').str[11]
dataTab_df["Shift Name"] = dataTab_df['Data'].str.split('|').str[12]

##Only include gen tasks
dataTab_df = dataTab_df.loc[dataTab_df['Task'] == 'General Tasks']
dataTab_df.drop('Data', axis=1, inplace=True)

##Write data to sheet
dataTab = gc.open_by_key('1HcKF-vUqoWUDX02IOZdeDioawz5Aw6dxwEtGHAAAFmM').worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, dataTab_df)

parsedDataTab = gc.open_by_key('1HcKF-vUqoWUDX02IOZdeDioawz5Aw6dxwEtGHAAAFmM').worksheet('ParsedData')
parsedDataTab.clear()
gd.set_with_dataframe(parsedDataTab, parsedDataTab_df)

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