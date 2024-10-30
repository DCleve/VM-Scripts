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

workflow = gc.open_by_key('1U38UjtKRdtgvjCvLEgceZtzdCDSjYiZOAIzEbVovZa4')
archivetab = workflow.worksheet('ArchiveData')
staffingtab = workflow.worksheet('Personnel')
sqDataTab = workflow.worksheet('SQData')

##connect to nuway archive
nuway_tab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Data')
nuway_df = pd.DataFrame.from_dict(nuway_tab.get_all_records())
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)

nuway_df = nuway_df[['Data']]

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]

nuway_df.loc[(nuway_df['Task'].str[:3] != 'PVP') & (nuway_df['Task'] != 'SCO PVP'), 'Punch'] = None
nuway_df.dropna(subset=["Punch"], inplace=True)

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Last, First (Formatting)', 'Role', 'Shift Name']]

staffing_df = staffing_df.sort_values(by=['Last, First (Formatting)'], ascending=True)

##Merge Staffing Data
staffing_df['Email'] = staffing_df['Email'].str.lower()
nuway_df['Puncher'] = nuway_df['Puncher'].str.lower()

nuway_df = pd.merge(nuway_df, staffing_df, left_on = "Puncher", right_on = "Email")

nuway_df["Combined"] = nuway_df['Punch'].astype(str) + nuway_df['Preferred Name'].astype(str)
nuway_df = nuway_df.drop_duplicates(subset=['Combined'])

##fix sq numbers
nuway_df["queue_date"] = nuway_df['SQ/POQ'].str.split('-').str[0]
nuway_df["queue_number"] = nuway_df['SQ/POQ'].str.split('-').str[1]
nuway_df['queue_number'] = nuway_df['queue_number'].str.zfill(3)
nuway_df['SQ/POQ'] = nuway_df['queue_date'] + '-' + nuway_df['queue_number']

##parse down data
nuway_df['Punch'] = pd.to_datetime(nuway_df['Punch'])

nuway_df.loc[nuway_df.Punch <= (pd.Timestamp(datetime.now()) - timedelta(days = 9)), 'Punch'] = None
nuway_df.dropna(subset=["Punch"], inplace=True)

###Import PVP SQ Data
pvp_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "PVP.csv"]
pvp_result = separator.join(pvp_string)
pvp_df = pd.read_csv(pvp_result)

##Fix data types
pvp_df['ORDER_COUNT'] = pvp_df['ORDER_COUNT'].astype('float64')
pvp_df['sq_card_quantity'] = pvp_df['sq_card_quantity'].astype('float64')
pvp_df['QUEUE_NUMBER'] = pvp_df['QUEUE_NUMBER'].str.lower()

pvp_df['CREATED_AT'] = pd.to_datetime(pvp_df['CREATED_AT']).dt.date

##Fix SQ Numbers
pvp_df["full_sq_number"] = pvp_df['SHIPPINGQUEUENUMBER']
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'full_sq_number'] = pvp_df['QUEUE_NUMBER']
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'QUEUE_NUMBER'] = pvp_df['SHIPPINGQUEUENUMBER']
pvp_df['SHIPPINGQUEUENUMBER'] = pvp_df['full_sq_number']

##Extract SQ Number and SQ Type
pvp_df["sq_number"] = pvp_df['SHIPPINGQUEUENUMBER'].str.split('-').str[1]

pvp_df["sq_type"] = pvp_df['sq_number'].str[-3:]

pvp_df.loc[pvp_df['sq_number'].map(len) == 9, 'sq_type'] = pvp_df['sq_number'].str[-6:]

pvp_df["sq_number"] = pvp_df['sq_number'].str[:3]

##Last 9 days
pvp_df["now"] = pd.Timestamp.now()
pvp_df['now'] = pvp_df['now'].dt.date
pvp_df = pvp_df.loc[pvp_df['CREATED_AT'] > (pvp_df['now'] - timedelta(days = 9))]

##Create final dataframe
pvp_df = pvp_df[['CREATED_AT', 'QUEUE_NUMBER', 'SHIPPINGQUEUENUMBER', 'sq_number', 'sq_type', 'ORDER_COUNT','sq_card_quantity']]

##Write data to sheett
nuway_df = nuway_df[['Punch',  'Task', 'SQ/POQ', 'Preferred Name', 'Location/Cards']]

cell_list = workflow.worksheet('PVPDashPt2').range('M1')
for cell in cell_list:
    cell.value = 'Off'
workflow.worksheet('PVPDashPt2').update_cells(cell_list, value_input_option='USER_ENTERED')

time.sleep(10)

staffingtab.clear()
gd.set_with_dataframe(staffingtab, staffing_df)

archivetab.clear()
gd.set_with_dataframe(archivetab, nuway_df)

sqDataTab.clear()
gd.set_with_dataframe(sqDataTab, pvp_df)

cell_list = workflow.worksheet('PVPDashPt2').range('M1')
for cell in cell_list:
    cell.value = 'On'
workflow.worksheet('PVPDashPt2').update_cells(cell_list, value_input_option='USER_ENTERED')

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