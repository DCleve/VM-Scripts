import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import datetime as dt
import time
import os
import numpy as np
import pytz
import datetime as dt

login = os.getlogin()
separator = '\\'
startTime = time.time()

gc=gspread.service_account()

##Import nuway data
dataTab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Data')
importTab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Import')

##Get current archive data
dataTab_df = pd.DataFrame.from_dict(dataTab.get_all_records())
dataTab_df["Punch"] = dataTab_df['Data'].str.split('|').str[0]
dataTab_df = dataTab_df.loc[dataTab_df['Punch'] != '']
dataTab_df['Punch'] = pd.to_datetime(dataTab_df['Punch']).dt.date

##Create older than 90 day frame
dataToArchive_df = dataTab_df.copy()

dataToArchive_df = dataToArchive_df.loc[(dataToArchive_df['Punch'] + timedelta(days = 90)) < dt.date.today()]

dataToArchive_df = dataToArchive_df[['Data']]

##Write old data to full archive
if len(dataToArchive_df) > 0:

    fullArchiveTab = gc.open_by_key('1ZSv8UD6I4lXI5Icav1FrWjdTURmgWHbSLMVflE1pC1k').worksheet('Archive')
    fullArchive_df = pd.DataFrame.from_dict(fullArchiveTab.get_all_records())

    archive_data = dataToArchive_df.values.tolist()

    fullArchiveTab.append_rows(archive_data)

##Search for punches older than 90 days in current archive and delete them
date_test = dt.date.today() - timedelta(days = 90)

for i in range(len(dataTab_df), 0, -1):
    if dataTab_df['Punch'][i - 1] < date_test:
        dataTab.delete_rows(i + 1, i + 1)

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