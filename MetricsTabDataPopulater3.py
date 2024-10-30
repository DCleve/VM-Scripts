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

##Import nuway data
punch_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
punch_result = separator.join(punch_string)
punch_df = pd.read_csv(punch_result)

punch_df["Punch"] = punch_df['Data'].str.split('|').str[0]
punch_df["First_Offset"] = punch_df['Data'].str.split('|').str[1]
punch_df["Team Members"] = punch_df['Data'].str.split('|').str[2]
punch_df["Units"] = punch_df['Data'].str.split('|').str[3]
punch_df["Subtask"] = punch_df['Data'].str.split('|').str[4]
punch_df["Task"] = punch_df['Data'].str.split('|').str[7]
punch_df["Day %"] = punch_df['Data'].str.split('|').str[5]
punch_df["Earned Hours"] = punch_df['Data'].str.split('|').str[6]
punch_df["Total Day %"] = punch_df['Data'].str.split('|').str[9]
punch_df["Total Earned Hours"] = punch_df['Data'].str.split('|').str[10]
punch_df["UKG Hours"] = punch_df['Data'].str.split('|').str[12]

punch_df.drop('Data', axis=1, inplace=True)

punch_df['Data'] = punch_df['Punch'].astype(str) + "|" + punch_df['First_Offset'].astype(str) + "|" + punch_df['Team Members'].astype(str) + "|" + punch_df['Units'].astype(str) + "|" + punch_df['Subtask'].astype(str) + "|" + punch_df['Day %'].astype(str) + "|" + punch_df['Earned Hours'].astype(str) + "|" + punch_df['Task'].astype(str) + "|" + punch_df['Total Day %'].astype(str) + "|" + punch_df['Total Earned Hours'].astype(str) + "|" + punch_df['UKG Hours'].astype(str)

punch_df = punch_df[['Data', 'Team Members', 'First_Offset']]

##Only include the past 90 days
punch_df["now"] = pd.Timestamp.now()
punch_df['now'] = punch_df['now'].dt.date

punch_df['First_Offset'] = pd.to_datetime(punch_df['First_Offset']).dt.date

punch_df = punch_df.loc[punch_df['First_Offset'] >= (punch_df['now'] - timedelta(days = 90))]

##Import google key info
key = gc.open_by_key('17494Hlnq3EPkR8WL3bkb0wSdbP2o025OuVJxSQ3aueE').worksheet('Data')
key_df = pd.DataFrame.from_dict(key.get_all_records())
key_df.dropna(subset=["Team Members"], inplace=True)
key_df.drop(key_df.filter(like='Unnamed'), axis=1, inplace=True)
key_df = key_df[['Team Members', 'Google Key']]

##Join nuway data on key data
punch_df = pd.merge(punch_df, key_df, how='left', on='Team Members')

punch_df = punch_df.loc[punch_df['Google Key'].str.len() == 44]

punch_df.drop_duplicates(subset=['Data'], inplace=True)

punch_df["Date"] = punch_df['Data'].str.split('|').str[1]
punch_df["Sort"] = punch_df['Data'].str.split('|').str[0]
punch_df['Date'] = pd.to_datetime(punch_df['Date'])
punch_df['Sort'] = pd.to_datetime(punch_df['Sort'])
punch_df.sort_values(by=['Sort'], ascending=True, inplace=True)
punch_df['Date'] = pd.to_datetime(punch_df['Date']).dt.date

punch_df['Date'] = punch_df['Date'].astype(str)

punch_df = punch_df[['Data', 'Date', 'Google Key']]

##Prepare data for writing to nuways
unique_archive_df = punch_df.copy()

unique_archive_df = unique_archive_df[['Google Key']]
unique_archive_df.drop_duplicates(subset=['Google Key'], inplace=True)
testLength = len(unique_archive_df);

s = 149;
i = 100;

while i <= s:
    if i < testLength:
        url = unique_archive_df.iloc[i, 0]
        print(url)
        print(i)

        data_df = pd.DataFrame()
        data_df = punch_df.loc[punch_df['Google Key'] == url]

        data_df = data_df[['Data', 'Date']]

        nuway = gc.open_by_key(url)
        metrics_Tab = nuway.worksheet('MetArch')
        metrics_Tab.clear()
        gd.set_with_dataframe(metrics_Tab, data_df)

        int_check = divmod(i + 1, 8)

        if int_check[1] == 0:
            time.sleep(45)

    i+=1

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