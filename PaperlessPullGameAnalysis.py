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

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Length','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)']]
staffing_df.rename(columns={'Email':'Puncher'}, inplace=True)

staffing_df['Puncher'] = staffing_df['Puncher'].str.lower()

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

###Import SQ Game Data
sq_acc_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqacc.csv"]
sq_acc_result = separator.join(sq_acc_string)
sq_acc_df = pd.read_csv(sq_acc_result)

##Fix data types
sq_acc_df['ORDER_COUNT'] = sq_acc_df['ORDER_COUNT'].astype('float64')
sq_acc_df['CARD_QUANTITY'] = sq_acc_df['CARD_QUANTITY'].astype('float64')
sq_acc_df['sq_card_quantity'] = sq_acc_df['sq_card_quantity'].astype('float64')

##Import Paperless Data
paperless_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Paperless.csv"]
paperless_result = separator.join(paperless_string)
paperless_df = pd.read_csv(paperless_result)

##Fix data types
paperless_df["CARDS_PULLED"] = paperless_df["CARDS_PULLED"].astype('float64')
paperless_df["DENSITY_PULLED"] = paperless_df["DENSITY_PULLED"].astype('float64')
paperless_df['PUNCHER'] = paperless_df['PUNCHER'].str.lower()
paperless_df.rename(columns={'PUNCHER':'Puncher'}, inplace=True)

##Reduce game data frame
sq_acc_df["combined"] = sq_acc_df['QUEUE_NUMBER'].astype(str) + sq_acc_df['GAME_NAME'].astype(str)

sq_acc_df.drop_duplicates(subset='combined', inplace=True)

sq_acc_df = sq_acc_df[['SHIPPINGQUEUENUMBER', 'ORDER_COUNT', 'GAME_NAME', 'sq_card_quantity']]
sq_acc_df.rename(columns={'SHIPPINGQUEUENUMBER':'SQ'}, inplace=True)

##Merge dataframes
paperless_df = pd.merge(paperless_df, sq_acc_df, how='left', on='SQ')
paperless_df = pd.merge(paperless_df, staffing_df, how='left', on='Puncher')

paperless_df = paperless_df[['SQ', 'PUNCH', 'CARDS_PULLED', 'DENSITY_PULLED', 'PAUSED_TIME_SECONDS', 'pulling_time_hours', 'sq_type', 'PULLING_START', 'ORDER_COUNT', 'GAME_NAME', 'sq_card_quantity', 'Preferred Name', 'Shift Name']]

##Write data to sheet
pp_analysis_data_tab = gc.open_by_key('1LRB-_tLF6kUlIUemkp_o7gakH_pAvy990GWtmbbDa_Y').worksheet('Data')
pp_analysis_data_tab.clear()
gd.set_with_dataframe(pp_analysis_data_tab, paperless_df)

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