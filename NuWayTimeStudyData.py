import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import time
import numpy as np
import requests
import os
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

nuway_df = pd.DataFrame(nuway_tab.get_all_values())
nuway_df.columns = nuway_df.iloc[0]
nuway_df = nuway_df[2:]
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)
nuway_df = nuway_df[['Data']]
nuway_df['Data'] = nuway_df['Data'].str.lower()

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]
nuway_df["Extra"] = nuway_df['Data'].str.split('|').str[5]
nuway_df["Missing"] = nuway_df['Data'].str.split('|').str[6]
nuway_df["Similar"] = nuway_df['Data'].str.split('|').str[7]
nuway_df["Unrecorded"] = nuway_df['Data'].str.split('|').str[8]
nuway_df["Other"] = nuway_df['Data'].str.split('|').str[9]
nuway_df["Flex Run"] = nuway_df['Data'].str.split('|').str[12]
nuway_df["Env Run"] = nuway_df['Data'].str.split('|').str[13]
nuway_df["Orders Completed"] = nuway_df['Data'].str.split('|').str[15]
nuway_df["Time Study Start"] = nuway_df['Data'].str.split('|').str[16]
nuway_df["Punch Tab"] = nuway_df['Data'].str.split('|').str[18]

nuway_df.loc[nuway_df['Flex Run'].str[:4] == 'slot', 'Flex Run'] = nuway_df['Flex Run'].str[-1:]
nuway_df.loc[nuway_df['Env Run'].str[:4] == 'slot', 'Env Run'] = nuway_df['Env Run'].str[-1:]

nuway_df = nuway_df[['Punch', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other', 'Flex Run', 'Env Run', 'Orders Completed', 'Time Study Start', 'Punch Tab']]

nuway_df.replace(',', '', regex=True, inplace=True)

###Import PVP SQ Data
pvp_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "PVP.csv"]
pvp_result = separator.join(pvp_string)
pvp_df = pd.read_csv(pvp_result)

### Import SQ Slot Data
slot_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqslot.csv"]
slot_result = separator.join(slot_string)
slot_df = pd.read_csv(slot_result)

slot_df["unique_pcids_by_slot"] = slot_df["unique_pcids_by_slot"].astype('float64')
slot_df["card_qty_by_slot"] = slot_df["card_qty_by_slot"].astype('float64')
##Fix data types
pvp_df['ORDER_COUNT'] = pvp_df['ORDER_COUNT'].astype('float64')
pvp_df['sq_card_quantity'] = pvp_df['sq_card_quantity'].astype('float64')

##Merge Staffing Data to nuway data
nuway_df = pd.merge(nuway_df, staffing_df, how='left')

nuway_df.drop('Puncher', axis=1, inplace=True)
nuway_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

##Parse out punches with time study data
nuway_df = nuway_df.loc[nuway_df['Punch Tab'].str[:5] == "punch"]
nuway_df = nuway_df.loc[nuway_df['Time Study Start'].str[-1:] == "0"]
nuway_df['Time Study Start'] = pd.to_datetime(nuway_df['Time Study Start'])
nuway_df['Time Study Start'] = pd.to_datetime(nuway_df['Time Study Start'], format='%m-%d-%Y %H:%M:%s').dt.round('1s')

nuway_df["Combined"] = nuway_df['Punch'].astype(str) + "|" + nuway_df['Puncher'].astype(str)

##Pull out SQ tasks
nuway_df = nuway_df.loc[(nuway_df['Task'] == 'pulling') | (nuway_df['Task'] == 'pvp') | (nuway_df['Task'] == 'sq error resolution')]

##Clean SQ Number column
nuway_df["clean_sq_number"] = nuway_df['SQ/POQ'].astype(str)

nuway_df.loc[((nuway_df['Task'].astype(str) == 'pvp') | (nuway_df['Task'].astype(str) == 'pulling') | (nuway_df['Task'].astype(str) == 'pull verifying')) & (nuway_df['SQ/POQ'].str[-3:] != 'poq'), 'clean_sq_number'] = nuway_df['SQ/POQ'].str[:-3]

nuway_df.rename(columns={'clean_sq_number':'QUEUE_NUMBER'}, inplace=True)
nuway_df['QUEUE_NUMBER'] = nuway_df['QUEUE_NUMBER'].str.lower()

##Combine with sq info
nuway_df = pd.merge(nuway_df, pvp_df, how='left', on='QUEUE_NUMBER')

nuway_df = nuway_df.loc[nuway_df['sq_card_quantity'] != '']

##Parse out non sco and takeover pvp punches
nuway_pvp_df = nuway_df.copy()
nuway_pvp_df = nuway_pvp_df.loc[(nuway_pvp_df['Task'] == 'pvp') & (nuway_pvp_df['SQ/POQ'].str[-3:] != 'sco') & (nuway_pvp_df['SQ/POQ'].str[-3:] != 'tak')]

##PVP full SQ number
nuway_pvp_df["full_sq_number"] = nuway_pvp_df['SHIPPINGQUEUENUMBER']
nuway_pvp_df.loc[nuway_pvp_df['SQ/POQ'].str[-3:] == 'poq', 'full_sq_number'] = nuway_pvp_df['QUEUE_NUMBER']

##Make final dataframe
nuway_pvp_df.drop(['Location/Cards', 'Flex Run', 'Env Run', 'Orders Completed', 'Punch Tab', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role', 'Shift Length', 'Combined', 'QUEUE_NUMBER', 'SQ/POQ', 'SHIPPINGQUEUENUMBER'], axis=1, inplace=True)

nuway_pvp_df.rename(columns={'pvp_cards':'sq_card_quantity'}, inplace=True)

##Parse out sco pvp punches
nuway_pvp_sco_df = nuway_df.copy()
nuway_pvp_sco_df = nuway_pvp_sco_df.loc[(nuway_pvp_sco_df['Task'] == 'pvp') & (nuway_pvp_sco_df['SQ/POQ'].str[-3:] == 'sco')]

##PVP full SQ number
nuway_pvp_sco_df["full_sq_number"] = nuway_pvp_sco_df['SHIPPINGQUEUENUMBER']
nuway_pvp_sco_df.loc[nuway_pvp_sco_df['SQ/POQ'].str[-3:] == 'poq', 'full_sq_number'] = nuway_pvp_sco_df['QUEUE_NUMBER']

##Make final dataframe
nuway_pvp_sco_df.drop(['Location/Cards', 'Flex Run', 'Env Run', 'Orders Completed', 'Punch Tab', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role', 'Shift Length', 'Combined', 'QUEUE_NUMBER', 'SQ/POQ', 'SHIPPINGQUEUENUMBER'], axis=1, inplace=True)

##Parse out error res punches
nuway_error_res_df = nuway_df.copy()
nuway_error_res_df = nuway_error_res_df.loc[nuway_error_res_df['Task'] == 'sq error resolution']

##PVP full SQ number
nuway_error_res_df["full_sq_number"] = nuway_error_res_df['SHIPPINGQUEUENUMBER']
nuway_error_res_df.loc[nuway_error_res_df['SQ/POQ'].str[-3:] == 'poq', 'full_sq_number'] = nuway_error_res_df['QUEUE_NUMBER']

##Make final dataframe
nuway_error_res_df.drop(['Flex Run', 'Env Run', 'Punch Tab', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role', 'Shift Length', 'Combined', 'QUEUE_NUMBER', 'SQ/POQ', 'SHIPPINGQUEUENUMBER'], axis=1, inplace=True)

nuway_error_res_df.rename(columns={'Extra': 'Yellow Cards', 'Missing': 'Extra', 'Similar':'Missing', 'Unrecorded': 'Similar', 'Other': 'False Red', 'Orders Completed': 'Unrecorded', 'Location/Cards':'Full Verify?'}, inplace=True)

##Parse out pulling punches
nuway_pull_df = nuway_df.copy()
nuway_pull_df = nuway_pull_df.loc[nuway_pull_df['Task'] == 'pulling']

##Pull full SQ number
nuway_pull_df["full_sq_number"] = nuway_pull_df['SHIPPINGQUEUENUMBER']
nuway_pull_df.loc[nuway_pull_df['SQ/POQ'].str[-3:] == 'poq', 'full_sq_number'] = nuway_pull_df['QUEUE_NUMBER']

nuway_pull_df["sq_type"] = nuway_pull_df['full_sq_number'].str[-3:]
nuway_pull_df.loc[(nuway_pull_df['full_sq_number'].map(len) == 16) & (nuway_pull_df['full_sq_number'].str[-3:] != 'poq'), 'sq_type'] = nuway_pull_df['full_sq_number'].str[-6:]

##Deal with slots
nuway_pull_df['Flex Run'] = nuway_pull_df['Flex Run'].str.lower()
nuway_pull_df['Env Run'] = nuway_pull_df['Env Run'].str.lower()

nuway_pull_df.loc[(nuway_pull_df['Flex Run'] != '') & (nuway_pull_df['sq_type'].str[:1] != 'r'), 'Flex Run'] = nuway_pull_df['Flex Run'].str.strip().apply(ord) - 96
nuway_pull_df.loc[(nuway_pull_df['Env Run'] != '') & (nuway_pull_df['sq_type'].str[:1] != 'r'), 'Env Run'] = nuway_pull_df['Env Run'].str.strip().apply(ord) - 96

##Join slot data to pull df
nuway_pull_df = pd.merge(nuway_pull_df, slot_df, on='SHIPPINGQUEUENUMBER', how='left')

##Remove slots/pcids not pulled and aggregate
nuway_pull_df["pulled_quantity"] = ""
nuway_pull_df["pulled_pcids"] = ""

nuway_pull_df.loc[(nuway_pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= nuway_pull_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (nuway_pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= nuway_pull_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'pulled_quantity'] = nuway_pull_df['card_qty_by_slot']

nuway_pull_df.loc[(nuway_pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= nuway_pull_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (nuway_pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= nuway_pull_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'pulled_pcids'] = nuway_pull_df['unique_pcids_by_slot']

nuway_pull_df['pulled_quantity'] = nuway_pull_df['pulled_quantity'].apply(pd.to_numeric, errors='coerce')
nuway_pull_df = nuway_pull_df.loc[nuway_pull_df['pulled_quantity'].apply(pd.to_numeric, errors='coerce') != '']

nuway_pull_df['pulled_pcids'] = nuway_pull_df['pulled_pcids'].apply(pd.to_numeric, errors='coerce')
nuway_pull_df = nuway_pull_df.loc[nuway_pull_df['pulled_pcids'].apply(pd.to_numeric, errors='coerce') != '']

nuway_pull_df["pull_combined"] = nuway_pull_df['Punch'].astype(str) + nuway_pull_df['Puncher'].astype(str) + nuway_pull_df['SHIPPINGQUEUENUMBER'].astype(str) + nuway_pull_df['Flex Run'].astype(str)

pulled_quantity_agg = nuway_pull_df.groupby('pull_combined')['pulled_quantity'].sum()
nuway_pull_df = pd.merge(nuway_pull_df, pulled_quantity_agg, how='right', on='pull_combined', suffixes=("_1", "_2"))

pulled_pcids_agg = nuway_pull_df.groupby('pull_combined')['pulled_pcids'].sum()
nuway_pull_df = pd.merge(nuway_pull_df, pulled_pcids_agg, how='right', on='pull_combined', suffixes=("_1", "_2"))

nuway_pull_df = nuway_pull_df.drop_duplicates(subset=['pull_combined'])

nuway_pull_df.rename(columns={'pulled_quantity_2': 'pulled_quantity', 'pulled_pcids_2': 'pulled_pcids'}, inplace=True)

nuway_pull_df = nuway_pull_df.loc[nuway_pull_df['pulled_pcids'] != 0]

##Calculate density of cards pulled
nuway_pull_df["pulled_density"] = nuway_pull_df['pulled_quantity'] / nuway_pull_df['pulled_pcids']

##Make final dataframe
nuway_pull_df.drop(['Location/Cards', 'Flex Run', 'Env Run', 'Punch Tab', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role', 'Shift Length', 'Combined', 'QUEUE_NUMBER_y', 'SQ/POQ', 'SHIPPINGQUEUENUMBER', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other', 'Orders Completed', 'QUEUE_NUMBER_x', 'SLOT', 'unique_pcids_by_slot', 'card_qty_by_slot', 'pulled_quantity_1', 'pulled_pcids_1', 'pull_combined', 'ORDER_COUNT_x', 'sq_card_quantity', 'sq_type', 'CREATED_AT_x','CREATED_AT_y', 'ORDER_COUNT_y'], axis=1, inplace=True)

##Combine Frames
data_df = pd.DataFrame()

data_df = pd.concat([nuway_pvp_df, nuway_pvp_sco_df, nuway_error_res_df, nuway_pull_df])

data_df.drop(['CREATED_AT'], axis=1, inplace=True)

##Write data to sheet
dataTab = gc.open_by_key('1vCK_DeduSRY25LSRWO2YSeVAwDJxQ3V5FVkG9-f4XQA').worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, data_df)

##Update audit log
from datetime import datetime
import pytz
import os
import pandas as pd

login = os.getlogin()
csv_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Audit CSVs", "AuditLog.csv"]
separator = '\\'
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