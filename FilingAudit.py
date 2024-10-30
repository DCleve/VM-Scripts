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

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffingdata = pd.DataFrame(staffing.get_all_values())
staffingdata.columns = staffingdata.iloc[0]
staffingdata = staffingdata[1:]
staffing_df = pd.DataFrame(staffingdata)
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Length','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role']]
staffing_df.rename(columns={'Email':'Puncher'}, inplace=True)

staffing_df['Puncher']= staffing_df['Puncher'].str.lower()

##Import Nuway Archive Data
nuway_tab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Data')
nuway_df = pd.DataFrame.from_dict(nuway_tab.get_all_records())
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)
nuway_df = nuway_df[['Data']]

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]

nuway_df = nuway_df.loc[nuway_df['Task'] == 'Filing']

nuway_df = nuway_df[['Punch', 'Puncher',  'SQ/POQ', 'Location/Cards']]

##Merge Staffing Data to nuway data
nuway_df['Puncher'] = nuway_df['Puncher'].str.lower()
nuway_df = pd.merge(nuway_df, staffing_df, how='left')

nuway_df.drop('Puncher', axis=1, inplace=True)
nuway_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

nuway_df = nuway_df[['Punch', 'Location/Cards', 'Puncher',  'Shift Name']]

##Write data to sheet
test = gc.open_by_key('10AGK6oHv6TIIwVtH7wv8G7DGs74i4XGjWD_VhDnvaDc')
testTab = test.worksheet('Data')
testTab.clear()
gd.set_with_dataframe(testTab, nuway_df)

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