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

###Import Shift Data
shift = gc.open_by_key('1Xq6I5LWxUvqRQ3kw8aBHFyPYmTJlajIfeMbWzzmggi4').worksheet('FilteredData')
shiftdata_df = pd.DataFrame.from_dict(shift.get_all_records())
shiftdata_df.dropna(subset=["Date"], inplace=True)
shiftdata_df.drop(shiftdata_df.filter(like='Unnamed'), axis=1, inplace=True)

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Role', 'Regular Hours']]
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date']).dt.date

shiftdata_df = shiftdata_df.loc[(shiftdata_df['Regular Hours'] != '-') & (shiftdata_df['Regular Hours'].astype('float64') > 0)]
shiftdata_df['Primary Email'] = shiftdata_df['Primary Email'].str.lower()
shiftdata_df["combined"] = shiftdata_df['Date'].astype(str) + shiftdata_df['Primary Email'].astype(str)

##Only look at the last 100 days
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date']).dt.date
shiftdata_df["now"] = pd.Timestamp.now()
shiftdata_df['now'] = shiftdata_df['now'].dt.date
shiftdata_df = shiftdata_df.loc[shiftdata_df['Date'] >= (shiftdata_df['now'] - timedelta(days = 100))]

##Adjusted Shift Length
shiftdata_df["negative_check"] = shiftdata_df['Regular Hours'].astype('float64') - 0.5

shiftdata_df.loc[shiftdata_df['Regular Hours'].astype('float64') >= 4, 'adjusted_shift_length'] = shiftdata_df['Regular Hours'].astype('float64') - 1 - (shiftdata_df['Regular Hours'].astype('float64')/0.2/60)

shiftdata_df.loc[(shiftdata_df['Regular Hours'].astype('float64') < 4) & (shiftdata_df['negative_check'].astype('float64') < 2), 'adjusted_shift_length'] = shiftdata_df['Regular Hours'].astype('float64') - (shiftdata_df['Regular Hours'].astype('float64')/0.2/60)

shiftdata_df.loc[(shiftdata_df['Regular Hours'].astype('float64') < 4) & (shiftdata_df['negative_check'].astype('float64') >= 2), 'adjusted_shift_length'] = shiftdata_df['Regular Hours'].astype('float64') - 0.5 - (shiftdata_df['Regular Hours'].astype('float64')/0.2/60)

##Determine time interval to search for punches
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date'])
shiftdata_df["minimum"] = ""
shiftdata_df["maximum"] = ""

shiftdata_df.loc[shiftdata_df['Role'] == 'FC Generalist Overnight', 'minimum'] = shiftdata_df['Date'] - timedelta(hours = 9)
shiftdata_df.loc[shiftdata_df['Role'] == 'FC Generalist Overnight', 'maximum'] = shiftdata_df['Date'] + timedelta(hours = 9)

shiftdata_df.loc[shiftdata_df['Role'] != 'FC Generalist Overnight', 'minimum'] = shiftdata_df['Date']
shiftdata_df.loc[shiftdata_df['Role'] != 'FC Generalist Overnight', 'maximum'] = shiftdata_df['Date'] + timedelta(hours = 24) - timedelta(seconds = 1)

shiftdata_df.rename(columns = {'Primary Email':'Puncher'}, inplace=True)
shiftdata_df['minimum'] = pd.to_datetime(shiftdata_df['minimum'])
shiftdata_df['maximum'] = pd.to_datetime(shiftdata_df['maximum'])

###Import Standards
standards = gc.open_by_key('1kGj5oAedgYC_fCJEwCkQ4aGfRNyJfxXdsAp1gQ6Pvqk').worksheet('Current')
#standards = gc.open_by_key('1kGj5oAedgYC_fCJEwCkQ4aGfRNyJfxXdsAp1gQ6Pvqk').worksheet('Upcoming')

standards_df = pd.DataFrame.from_dict(standards.get_all_records())
standards_df.dropna(subset=['Task'], inplace=True)
standards_df.drop(standards_df.filter(like='Unnamed'), axis=1, inplace=True)
standards_df.loc[standards_df['Y-Int'] == '', 'Y-Int'] = None
standards_df.loc[standards_df['Coeff 1'] == '', 'Coeff 1'] = None
standards_df.loc[standards_df['Coeff 2'] == '', 'Coeff 2'] = None
standards_df.loc[standards_df['Coeff 3'] == '', 'Coeff 3'] = None
standards_df.loc[standards_df['Coeff 4'] == '', 'Coeff 4'] = None
standards_df.loc[standards_df['Coeff 5'] == '', 'Coeff 5'] = None
standards_df.loc[standards_df['Coeff 6'] == '', 'Coeff 6'] = None

standards_df[standards_df.isna()] = 0

###Define Size Settings
for i in range(len(standards_df)):
    if standards_df.iloc[i, 0] == 'Size Settings':
        if standards_df.iloc[i, 1] == 'Small':
            small = standards_df.iloc[i, 2]
        elif standards_df.iloc[i, 1] == 'Large':
            large = standards_df.iloc[i, 2]

small = float(small)
large = float(large)

####Data import module
###Import Nuway Archive Data
nuway_tab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Data')

nuway_df = pd.DataFrame.from_dict(nuway_tab.get_all_records())
nuway_df.dropna(subset=['Data'], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)
nuway_df['Data'] = nuway_df['Data'].str.lower()

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["Subtask"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Units"] = nuway_df['Data'].str.split('|').str[4]
nuway_df["Coeff 1 Units"] = nuway_df['Data'].str.split('|').str[5]
nuway_df["Coeff 2 Units"] = nuway_df['Data'].str.split('|').str[6]
nuway_df["Coeff 3 Units"] = nuway_df['Data'].str.split('|').str[7]
nuway_df["Coeff 4 Units"] = nuway_df['Data'].str.split('|').str[8]
nuway_df["Coeff 5 Units"] = nuway_df['Data'].str.split('|').str[9]
nuway_df["Test"] = nuway_df['Data'].str.split('|').str[10]
nuway_df["Notes"] = nuway_df['Data'].str.split('|').str[11]
nuway_df["Flex Run"] = nuway_df['Data'].str.split('|').str[12]
nuway_df["Env Run"] = nuway_df['Data'].str.split('|').str[13]
nuway_df["Orders Completed"] = nuway_df['Data'].str.split('|').str[15]

nuway_df.loc[nuway_df['Flex Run'].str[:4] == 'slot', 'Flex Run'] = nuway_df['Flex Run'].str[-1:]
nuway_df.loc[nuway_df['Env Run'].str[:4] == 'slot', 'Env Run'] = nuway_df['Env Run'].str[-1:]

nuway_df.drop('Data', axis=1, inplace=True)
nuway_df.replace(',', '', regex=True, inplace=True)

##Clean SQ Number column
nuway_df["clean_sq_number"] = nuway_df['Subtask']

nuway_df.loc[(nuway_df['Subtask'].str[-3:] != 'poq') & ((nuway_df['Task'] == 'pvp')  | (nuway_df['Task'] == 'pulling') | (nuway_df['Task'] == 'pull verifying')), 'clean_sq_number'] = nuway_df['Subtask'].str[:-3]

nuway_df.rename(columns={'clean_sq_number':'QUEUE_NUMBER'}, inplace=True)

nuway_df['QUEUE_NUMBER'] = nuway_df['QUEUE_NUMBER'].str.lower()

##Create Error Dataframe
error_df = pd.DataFrame(columns = ['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe'])

###Import Receiving Data
rec_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Rec.csv"]
rec_result = separator.join(rec_string)
rec_df = pd.read_csv(rec_result)

##Fix data types
rec_df['PROC_MISS_COUNT_ENTERED'] = rec_df['PROC_MISS_COUNT_ENTERED'].astype('float64')
rec_df['PROC_COND_COUNT_ENTERED'] = rec_df['PROC_COND_COUNT_ENTERED'].astype('float64')
rec_df['PROC_EXTRA_COUNT_ENTERED'] = rec_df['PROC_EXTRA_COUNT_ENTERED'].astype('float64')
rec_df['VER_MISS_COUNT_ENTERED'] = rec_df['VER_MISS_COUNT_ENTERED'].astype('float64')
rec_df['VER_COND_COUNT_ENTERED'] = rec_df['VER_COND_COUNT_ENTERED'].astype('float64')
rec_df['VER_EXTRA_COUNT_ENTERED'] = rec_df['VER_EXTRA_COUNT_ENTERED'].astype('float64')
rec_df['sleeved_cards'] = rec_df['sleeved_cards'].astype('float64')
rec_df['cabinet_splits'] = rec_df['cabinet_splits'].astype('float64')

##Import Buylist Data
blo_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "BLO.csv"]
blo_result = separator.join(blo_string)
blo_df = pd.read_csv(blo_result)

##Fix data types
blo_df['PROC_MISS_COUNT_ENTERED'] = blo_df['PROC_MISS_COUNT_ENTERED'].astype('float64')
blo_df['PROC_COND_COUNT_ENTERED'] = blo_df['PROC_COND_COUNT_ENTERED'].astype('float64')
blo_df['PROC_EXTRA_COUNT_ENTERED'] = blo_df['PROC_EXTRA_COUNT_ENTERED'].astype('float64')
blo_df['VER_MISS_COUNT_ENTERED'] = blo_df['VER_MISS_COUNT_ENTERED'].astype('float64')
blo_df['VER_COND_COUNT_ENTERED'] = blo_df['VER_COND_COUNT_ENTERED'].astype('float64')
blo_df['VER_EXTRA_COUNT_ENTERED'] = blo_df['VER_EXTRA_COUNT_ENTERED'].astype('float64')
blo_df['sleeved_cards'] = blo_df['sleeved_cards'].astype('float64')
blo_df['cabinet_splits'] = blo_df['cabinet_splits'].astype('float64')

###Import PVP SQ Data
pvp_sql_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "PVP.csv"]
pvp_sql_result = separator.join(pvp_sql_string)
pvp_sql_df = pd.read_csv(pvp_sql_result)

##Fix data types
pvp_sql_df['ORDER_COUNT'] = pvp_sql_df['ORDER_COUNT'].astype('float64')
pvp_sql_df['sq_card_quantity'] = pvp_sql_df['sq_card_quantity'].astype('float64')
pvp_sql_df['QUEUE_NUMBER'] = pvp_sql_df['QUEUE_NUMBER'].str.lower()

###Import SQ Slot Data
sq_slot_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqslot.csv"]
sq_slot_result = separator.join(sq_slot_string)
sq_slot_df = pd.read_csv(sq_slot_result)

##Fix data types
sq_slot_df['unique_pcids_by_slot'] = sq_slot_df['unique_pcids_by_slot'].astype('float64')
sq_slot_df['card_qty_by_slot'] = sq_slot_df['card_qty_by_slot'].astype('float64')

##Import Paperless Data
paperless_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Paperless.csv"]
paperless_result = separator.join(paperless_string)
paperless_df = pd.read_csv(paperless_result)

##Fix data types
paperless_df["CARDS_PULLED"] = paperless_df["CARDS_PULLED"].astype('float64')
paperless_df["DENSITY_PULLED"] = paperless_df["DENSITY_PULLED"].astype('float64')

####Task Modules
###Receiving Module
##RI Teardown Time
rec_df["teardown_time"] = 61 + (0.185 * rec_df['NUMBER_OF_CARDS'].astype('int64'))

rec_df.loc[rec_df['teardown_time'] > 209, 'teardown_time'] = 209

##Create proc and ver tags
rec_df["RI_PROC_TAG"] = ""
rec_df["RI_VER_TAG"] = ""

rec_df.loc[rec_df['PROC_TIME_MINUTES'].astype('float64') > 0, 'RI_PROC_TAG'] = "RI Proc"
rec_df.loc[(rec_df['VER_TIME_MINUTES'].astype('float64') > 5) & (rec_df['PROCESSOR_EMAIL'] != rec_df['VERIFIER_EMAIL']), 'RI_VER_TAG'] = "RI Ver"

##Split into RI Proc, Ver
rec_proc_standards_df = standards_df.copy()
rec_proc_standards_df = rec_proc_standards_df.loc[rec_proc_standards_df['Task'] == 'RI Proc']

rec_proc_df = rec_df.copy()
rec_proc_df = rec_proc_df.loc[rec_proc_df['RI_PROC_TAG'] != '']
rec_proc_df["combined"] = rec_proc_df['RI_PROC_TAG'].astype(str) + rec_proc_df['Tag'].astype(str)

rec_proc_standards_df["combined"] = rec_proc_standards_df['Task'].astype(str) + rec_proc_standards_df['Subtask'].astype(str)

rec_proc_df = pd.merge(rec_proc_df, rec_proc_standards_df, left_on='combined', right_on='combined')

rec_proc_df = rec_proc_df[['RI_NUMBER', 'NUMBER_OF_CARDS', 'PROCESSOR_EMAIL', 'PROCESSING_ENDED','PROC_MISS_COUNT_ENTERED', 'PROC_COND_COUNT_ENTERED', 'PROC_EXTRA_COUNT_ENTERED', 'sleeved_cards', 'cabinet_splits', 'Tag', 'RI_PROC_TAG', 'teardown_time', 'Y-Int', 'Coeff 1', 'Coeff 2', 'Coeff 3', 'Coeff 4', 'Coeff 5', 'Coeff 6', 'Y-Int Def']]

rec_ver_standards_df = standards_df.copy()
rec_ver_standards_df = rec_ver_standards_df.loc[rec_ver_standards_df['Task'] == 'RI Ver']

rec_ver_df = rec_df.copy()
rec_ver_df = rec_ver_df.loc[rec_ver_df['RI_VER_TAG'] != '']
rec_ver_df["combined"] = rec_ver_df['RI_VER_TAG'].astype(str) + rec_ver_df['Tag'].astype(str)

rec_ver_standards_df["combined"] = rec_ver_standards_df['Task'].astype(str) + rec_ver_standards_df['Subtask'].astype(str)

rec_ver_df = pd.merge(rec_ver_df, rec_ver_standards_df, left_on='combined', right_on='combined')

rec_ver_df = rec_ver_df[['RI_NUMBER', 'NUMBER_OF_CARDS', 'VERIFIER_EMAIL', 'VERIFYING_ENDED','VER_MISS_COUNT_ENTERED', 'VER_COND_COUNT_ENTERED', 'VER_EXTRA_COUNT_ENTERED', 'sleeved_cards', 'cabinet_splits', 'Tag', 'RI_VER_TAG', 'teardown_time', 'Y-Int', 'Coeff 1', 'Coeff 2', 'Coeff 3', 'Coeff 4', 'Coeff 5', 'Coeff 6', 'Y-Int Def']]

##Prep dataframes for merge
rec_proc_df.rename(columns={'PROCESSING_ENDED': 'Punch','PROCESSOR_EMAIL': 'Puncher','RI_PROC_TAG': 'Task','Tag': 'Subtask'}, inplace=True)

rec_proc_df["Dupe"] = rec_proc_df['Puncher'].astype(str) + rec_proc_df['Task'].astype(str) + rec_proc_df['NUMBER_OF_CARDS'].astype(str) + rec_proc_df['Subtask'].astype(str) + rec_proc_df['RI_NUMBER'].astype(str)

rec_proc_df.drop_duplicates(subset=['Dupe'], inplace=True)
rec_proc_df.drop('Dupe', axis=1, inplace=True)


rec_ver_df.rename(columns={'VERIFYING_ENDED': 'Punch','VERIFIER_EMAIL': 'Puncher','RI_VER_TAG': 'Task','Tag': 'Subtask'}, inplace=True)

rec_ver_df["Dupe"] = rec_ver_df['Puncher'].astype(str) + rec_ver_df['Task'].astype(str) + rec_ver_df['NUMBER_OF_CARDS'].astype(str) + rec_ver_df['Subtask'].astype(str) + rec_ver_df['RI_NUMBER'].astype(str)

rec_ver_df.drop_duplicates(subset=['Dupe'], inplace=True)
rec_ver_df.drop('Dupe', axis=1, inplace=True)

##Normalize punches
rec_proc_df["normal_punch"] = pd.to_datetime(rec_proc_df['Punch']).dt.date

rec_ver_df["normal_punch"] = pd.to_datetime(rec_ver_df['Punch']).dt.date

##Merge staffing data and include current team members
rec_proc_df['Puncher'] = rec_proc_df['Puncher'].str.lower()
rec_proc_df = pd.merge(rec_proc_df, staffing_df, how='left', on='Puncher')
rec_proc_df = rec_proc_df.loc[rec_proc_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

rec_ver_df['Puncher'] = rec_ver_df['Puncher'].str.lower()
rec_ver_df = pd.merge(rec_ver_df, staffing_df, how='left', on='Puncher')
rec_ver_df = rec_ver_df.loc[rec_ver_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
rec_proc_df["keep"] = None
rec_proc_df = pd.merge(rec_proc_df, shiftdata_df, how='left', on='Puncher')

rec_proc_df.loc[(rec_proc_df['Punch'] >= rec_proc_df['minimum']) & (rec_proc_df['Punch'] <= rec_proc_df['maximum']), 'keep'] = 1

rec_proc_df = rec_proc_df.loc[rec_proc_df['keep'] == 1]

rec_ver_df["keep"] = None
rec_ver_df = pd.merge(rec_ver_df, shiftdata_df, how='left', on='Puncher')

rec_ver_df.loc[(rec_ver_df['Punch'] >= rec_ver_df['minimum']) & (rec_ver_df['Punch'] <= rec_ver_df['maximum']), 'keep'] = 1

rec_ver_df = rec_ver_df.loc[rec_ver_df['keep'] == 1]

##First Offset
rec_proc_df.rename(columns={'Date':'first_offset'}, inplace=True)
rec_proc_df['first_offset'] = pd.to_datetime(rec_proc_df['first_offset'], format='%m-%d-%Y').dt.date

rec_ver_df.rename(columns={'Date':'first_offset'}, inplace=True)
rec_ver_df['first_offset'] = pd.to_datetime(rec_ver_df['first_offset'], format='%m-%d-%Y').dt.date

##Calculate receiving metrics
rec_proc_df = rec_proc_df.loc[(rec_proc_df['adjusted_shift_length'].astype('float64') > 0) & (rec_proc_df['Shift Name'] != 0)]

rec_proc_df["Day %"] = ""

rec_proc_df["Proc Coeff 1 Units"] = rec_proc_df['NUMBER_OF_CARDS']
rec_proc_df["Proc Coeff 2 Units"] = rec_proc_df['PROC_MISS_COUNT_ENTERED']
rec_proc_df["Proc Coeff 3 Units"] = rec_proc_df['PROC_EXTRA_COUNT_ENTERED']
rec_proc_df["Proc Coeff 4 Units"] = rec_proc_df['PROC_COND_COUNT_ENTERED']
rec_proc_df["Proc Coeff 5 Units"] = 0
rec_proc_df["Proc Coeff 6 Units"] = 0

rec_proc_df.loc[rec_proc_df['Y-Int Def'] == 'CPH', 'Day %'] = rec_proc_df['NUMBER_OF_CARDS'].astype('int64') / ((rec_proc_df['Y-Int'].astype('float64') +
(rec_proc_df['Proc Coeff 1 Units'].astype('float64') * rec_proc_df['Coeff 1'].astype('float64')) +
(rec_proc_df['Proc Coeff 2 Units'].astype('float64') * rec_proc_df['Coeff 2'].astype('float64')) +
(rec_proc_df['Proc Coeff 3 Units'].astype('float64') * rec_proc_df['Coeff 3'].astype('float64')) +
(rec_proc_df['Proc Coeff 4 Units'].astype('float64') * rec_proc_df['Coeff 4'].astype('float64')) +
(rec_proc_df['Proc Coeff 5 Units'].astype('float64') * rec_proc_df['Coeff 5'].astype('float64')) +
(rec_proc_df['Proc Coeff 6 Units'].astype('float64') * rec_proc_df['Coeff 6'].astype('float64'))
) * rec_proc_df['adjusted_shift_length'].astype('float64'))

rec_proc_df.loc[rec_proc_df['Y-Int Def'] == 'SPC', 'Day %'] = rec_proc_df['NUMBER_OF_CARDS'].astype('int64') / (( 3600 / (rec_proc_df['Y-Int'].astype('float64') +
(rec_proc_df['Proc Coeff 1 Units'].astype('float64') * rec_proc_df['Coeff 1'].astype('float64')) +
(rec_proc_df['Proc Coeff 2 Units'].astype('float64') * rec_proc_df['Coeff 2'].astype('float64')) +
(rec_proc_df['Proc Coeff 3 Units'].astype('float64') * rec_proc_df['Coeff 3'].astype('float64')) +
(rec_proc_df['Proc Coeff 4 Units'].astype('float64') * rec_proc_df['Coeff 4'].astype('float64')) +
(rec_proc_df['Proc Coeff 5 Units'].astype('float64') * rec_proc_df['Coeff 5'].astype('float64')) +
(rec_proc_df['Proc Coeff 6 Units'].astype('float64') * rec_proc_df['Coeff 6'].astype('float64'))
)) * rec_proc_df['adjusted_shift_length'].astype('float64'))

rec_ver_df = rec_ver_df.loc[(rec_ver_df['adjusted_shift_length'].astype('float64') > 0) & (rec_ver_df['Shift Name'] != 0)]

rec_ver_df["Day %"] = ""

rec_ver_df["Ver Coeff 1 Units"] = rec_ver_df['NUMBER_OF_CARDS']
rec_ver_df["Ver Coeff 2 Units"] = rec_ver_df['VER_MISS_COUNT_ENTERED']
rec_ver_df["Ver Coeff 3 Units"] = rec_ver_df['VER_EXTRA_COUNT_ENTERED']
rec_ver_df["Ver Coeff 4 Units"] = rec_ver_df['VER_COND_COUNT_ENTERED']
rec_ver_df["Ver Coeff 5 Units"] = 0
rec_ver_df["Ver Coeff 6 Units"] = 0

rec_ver_df.loc[rec_ver_df['Y-Int Def'] == 'CPH', 'Day %'] = rec_ver_df['NUMBER_OF_CARDS'].astype('int64') / ((rec_ver_df['Y-Int'].astype('float64') +
(rec_ver_df['Ver Coeff 1 Units'].astype('float64') * rec_ver_df['Coeff 1'].astype('float64')) +
(rec_ver_df['Ver Coeff 2 Units'].astype('float64') * rec_ver_df['Coeff 2'].astype('float64')) +
(rec_ver_df['Ver Coeff 3 Units'].astype('float64') * rec_ver_df['Coeff 3'].astype('float64')) +
(rec_ver_df['Ver Coeff 4 Units'].astype('float64') * rec_ver_df['Coeff 4'].astype('float64')) +
(rec_ver_df['Ver Coeff 5 Units'].astype('float64') * rec_ver_df['Coeff 5'].astype('float64')) +
(rec_ver_df['Ver Coeff 6 Units'].astype('float64') * rec_ver_df['Coeff 6'].astype('float64'))
) * rec_ver_df['adjusted_shift_length'].astype('float64'))

rec_ver_df.loc[rec_ver_df['Y-Int Def'] == 'SPC', 'Day %'] = rec_ver_df['NUMBER_OF_CARDS'].astype('int64') / (( 3600 / (rec_ver_df['Y-Int'].astype('float64') +
(rec_ver_df['Ver Coeff 1 Units'].astype('float64') * rec_ver_df['Coeff 1'].astype('float64')) +
(rec_ver_df['Ver Coeff 2 Units'].astype('float64') * rec_ver_df['Coeff 2'].astype('float64')) +
(rec_ver_df['Ver Coeff 3 Units'].astype('float64') * rec_ver_df['Coeff 3'].astype('float64')) +
(rec_ver_df['Ver Coeff 4 Units'].astype('float64') * rec_ver_df['Coeff 4'].astype('float64')) +
(rec_ver_df['Ver Coeff 5 Units'].astype('float64') * rec_ver_df['Coeff 5'].astype('float64')) +
(rec_ver_df['Ver Coeff 6 Units'].astype('float64') * rec_ver_df['Coeff 6'].astype('float64'))
)) * rec_ver_df['adjusted_shift_length'].astype('float64'))

##Account for teardown time
rec_proc_df['Day %'] = rec_proc_df['Day %'].astype('float64') + ((rec_proc_df['teardown_time'].astype('float64')/60/60) / rec_proc_df['adjusted_shift_length'].astype('float64'))

###Concat frames together
rec_final_df = pd.DataFrame()

if len(rec_ver_df) > 0:
    rec_final_df = pd.concat([rec_proc_df, rec_ver_df], ignore_index=True)

if len(rec_ver_df) == 0:
    rec_final_df = rec_proc_df

rec_final_df["Test"] = ""
rec_final_df["Notes"] = ""

rec_final_df.drop('Puncher', axis=1, inplace=True)

rec_final_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

##Check for negatives
rec_final_df.loc[rec_final_df['Day %'].astype('float64') < 0, 'Day %'] = rec_final_df['Day %'].astype('float64') * -1

##Create final dataframe
rec_final_df.rename(columns={'NUMBER_OF_CARDS':'Units'}, inplace=True)

rec_final_df = rec_final_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name']]

###BLO Module
##Split into processing and verifying
blo_proc_standards_df = standards_df.loc[standards_df['Subtask'] == 'BLO Proc']

blo_proc_df = blo_df.loc[blo_df['PROCESSOR'] != '']
blo_proc_df["Subtask"] = ""
blo_proc_df.loc[blo_proc_df['PROCESSOR'] != '', 'Subtask'] = 'BLO Proc'
blo_proc_df["Task"] = "BLO Proc"

blo_proc_df = pd.merge(blo_proc_df, blo_proc_standards_df, how='left', on='Subtask')

blo_proc_df.drop(['VERIFIER', 'VERIFYING_TIME_MINUTES', 'VER_MISS_COUNT_ENTERED', 'VER_COND_COUNT_ENTERED', 'VER_EXTRA_COUNT_ENTERED', 'VERIFYING_STARTED_AT', 'proc_cph', 'proc_spc', 'ver_cph', 'ver_spc', 'Size', 'Minutes Credit'], axis=1, inplace=True)

blo_proc_df.rename(columns={'Task_x':'Task'}, inplace=True)


blo_ver_standards_df = standards_df.loc[standards_df['Subtask'] == 'BLO Ver']

blo_ver_df = blo_df.loc[blo_df['VERIFIER'] != '']
blo_ver_df["Subtask"] = ""
blo_ver_df.loc[blo_ver_df['VERIFIER'] != '', 'Subtask'] = 'BLO Ver'
blo_ver_df["Task"] = "BLO Ver"

blo_ver_df = pd.merge(blo_ver_df, blo_ver_standards_df, how='left', on='Subtask')

blo_ver_df.drop(['PROCESSOR', 'PROCESSING_TIME_MINUTES', 'PROC_MISS_COUNT_ENTERED', 'PROC_COND_COUNT_ENTERED', 'PROC_EXTRA_COUNT_ENTERED', 'PROCESSING_STARTED_AT', 'proc_cph', 'proc_spc', 'ver_cph', 'ver_spc', 'Size', 'Minutes Credit'], axis=1, inplace=True)

blo_ver_df.rename(columns={'Task_x':'Task'}, inplace=True)

##Normalize punches
blo_proc_df["normal_punch"] = pd.to_datetime(blo_proc_df['PROCESSING_STARTED_AT']).dt.date
blo_proc_df.rename(columns={'PROCESSING_STARTED_AT':'Punch'}, inplace=True)

blo_ver_df["normal_punch"] = None
blo_ver_df['VERIFYING_STARTED_AT'] = blo_ver_df['VERIFYING_STARTED_AT'].apply(pd.to_datetime, errors='coerce')
blo_ver_df['normal_punch'] = pd.to_datetime(blo_ver_df['VERIFYING_STARTED_AT']).dt.date
blo_ver_df.rename(columns={'VERIFYING_STARTED_AT':'Punch'}, inplace=True)

##Merge staffing data
blo_proc_df['PROCESSOR'] = blo_proc_df['PROCESSOR'].str.lower()
blo_proc_df.rename(columns={'PROCESSOR':'Puncher'}, inplace=True)
blo_proc_df = pd.merge(blo_proc_df, staffing_df, how='left', on='Puncher')

blo_ver_df['VERIFIER'] = blo_ver_df['VERIFIER'].str.lower()
blo_ver_df.rename(columns={'VERIFIER':'Puncher'}, inplace=True)
blo_ver_df = pd.merge(blo_ver_df, staffing_df, how='left', on='Puncher')

##Include current team members
blo_proc_df = blo_proc_df.loc[blo_proc_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

blo_ver_df = blo_ver_df.loc[blo_ver_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
blo_proc_df["keep"] = None
blo_proc_df = pd.merge(blo_proc_df, shiftdata_df, how='left', on='Puncher')

blo_proc_df.loc[(blo_proc_df['Punch'] >= blo_proc_df['minimum']) & (blo_proc_df['Punch'] <= blo_proc_df['maximum']), 'keep'] = 1

blo_proc_df = blo_proc_df.loc[blo_proc_df['keep'] == 1]

blo_ver_df["keep"] = None
blo_ver_df = pd.merge(blo_ver_df, shiftdata_df, how='left', on='Puncher')

blo_ver_df.loc[(blo_ver_df['Punch'] >= blo_ver_df['minimum']) & (blo_ver_df['Punch'] <= blo_ver_df['maximum']), 'keep'] = 1

blo_ver_df = blo_ver_df.loc[blo_ver_df['keep'] == 1]

##First Offset
blo_proc_df.rename(columns={'Date':'first_offset'}, inplace=True)
blo_proc_df['first_offset'] = pd.to_datetime(blo_proc_df['first_offset'], format='%m-%d-%Y').dt.date

blo_ver_df.rename(columns={'Date':'first_offset'}, inplace=True)
blo_ver_df['first_offset'] = pd.to_datetime(blo_ver_df['first_offset'], format='%m-%d-%Y').dt.date

##Calculate bloe metrics
blo_proc_df = blo_proc_df.loc[(blo_proc_df['adjusted_shift_length'].astype('float64') > 0) & (blo_proc_df['Shift Name'] != 0)]

blo_proc_df["Day %"] = ""

blo_proc_df["Proc Coeff 1 Units"] = 0
blo_proc_df["Proc Coeff 2 Units"] = 0
blo_proc_df["Proc Coeff 3 Units"] = 0
blo_proc_df["Proc Coeff 4 Units"] = 0
blo_proc_df["Proc Coeff 5 Units"] = 0
blo_proc_df["Proc Coeff 6 Units"] = 0

blo_proc_df.loc[blo_proc_df['Y-Int Def'] == 'CPH', 'Day %'] = blo_proc_df['BLO_PRODUCT_COUNT'].astype('int64') / ((blo_proc_df['Y-Int'].astype('float64') +
(blo_proc_df['Proc Coeff 1 Units'].astype('float64') * blo_proc_df['Coeff 1'].astype('float64')) +
(blo_proc_df['Proc Coeff 2 Units'].astype('float64') * blo_proc_df['Coeff 2'].astype('float64')) +
(blo_proc_df['Proc Coeff 3 Units'].astype('float64') * blo_proc_df['Coeff 3'].astype('float64')) +
(blo_proc_df['Proc Coeff 4 Units'].astype('float64') * blo_proc_df['Coeff 4'].astype('float64')) +
(blo_proc_df['Proc Coeff 5 Units'].astype('float64') * blo_proc_df['Coeff 5'].astype('float64')) +
(blo_proc_df['Proc Coeff 6 Units'].astype('float64') * blo_proc_df['Coeff 6'].astype('float64'))
) * blo_proc_df['adjusted_shift_length'].astype('float64'))

blo_proc_df.loc[blo_proc_df['Y-Int Def'] == 'SPC', 'Day %'] = blo_proc_df['BLO_PRODUCT_COUNT'].astype('int64') / (( 3600 / (blo_proc_df['Y-Int'].astype('float64') +
(blo_proc_df['Proc Coeff 1 Units'].astype('float64') * blo_proc_df['Coeff 1'].astype('float64')) +
(blo_proc_df['Proc Coeff 2 Units'].astype('float64') * blo_proc_df['Coeff 2'].astype('float64')) +
(blo_proc_df['Proc Coeff 3 Units'].astype('float64') * blo_proc_df['Coeff 3'].astype('float64')) +
(blo_proc_df['Proc Coeff 4 Units'].astype('float64') * blo_proc_df['Coeff 4'].astype('float64')) +
(blo_proc_df['Proc Coeff 5 Units'].astype('float64') * blo_proc_df['Coeff 5'].astype('float64')) +
(blo_proc_df['Proc Coeff 6 Units'].astype('float64') * blo_proc_df['Coeff 6'].astype('float64'))
)) * blo_proc_df['adjusted_shift_length'].astype('float64'))


blo_ver_df = blo_ver_df.loc[(blo_ver_df['adjusted_shift_length'].astype('float64') > 0) & (blo_ver_df['Shift Name'] != 0)]

blo_ver_df["Day %"] = ""

blo_ver_df["Ver Coeff 1 Units"] = 0
blo_ver_df["Ver Coeff 2 Units"] = 0
blo_ver_df["Ver Coeff 3 Units"] = 0
blo_ver_df["Ver Coeff 4 Units"] = 0
blo_ver_df["Ver Coeff 5 Units"] = 0
blo_ver_df["Ver Coeff 6 Units"] = 0

blo_ver_df.loc[blo_ver_df['Y-Int Def'] == 'CPH', 'Day %'] = blo_ver_df['BLO_PRODUCT_COUNT'].astype('int64') / ((blo_ver_df['Y-Int'].astype('float64') +
(blo_ver_df['Ver Coeff 1 Units'].astype('float64') * blo_ver_df['Coeff 1'].astype('float64')) +
(blo_ver_df['Ver Coeff 2 Units'].astype('float64') * blo_ver_df['Coeff 2'].astype('float64')) +
(blo_ver_df['Ver Coeff 3 Units'].astype('float64') * blo_ver_df['Coeff 3'].astype('float64')) +
(blo_ver_df['Ver Coeff 4 Units'].astype('float64') * blo_ver_df['Coeff 4'].astype('float64')) +
(blo_ver_df['Ver Coeff 5 Units'].astype('float64') * blo_ver_df['Coeff 5'].astype('float64')) +
(blo_ver_df['Ver Coeff 6 Units'].astype('float64') * blo_ver_df['Coeff 6'].astype('float64'))
) * blo_ver_df['adjusted_shift_length'].astype('float64'))

blo_ver_df.loc[blo_ver_df['Y-Int Def'] == 'SPC', 'Day %'] = blo_ver_df['BLO_PRODUCT_COUNT'].astype('int64') / (( 3600 / (blo_ver_df['Y-Int'].astype('float64') +
(blo_ver_df['Ver Coeff 1 Units'].astype('float64') * blo_ver_df['Coeff 1'].astype('float64')) +
(blo_ver_df['Ver Coeff 2 Units'].astype('float64') * blo_ver_df['Coeff 2'].astype('float64')) +
(blo_ver_df['Ver Coeff 3 Units'].astype('float64') * blo_ver_df['Coeff 3'].astype('float64')) +
(blo_ver_df['Ver Coeff 4 Units'].astype('float64') * blo_ver_df['Coeff 4'].astype('float64')) +
(blo_ver_df['Ver Coeff 5 Units'].astype('float64') * blo_ver_df['Coeff 5'].astype('float64')) +
(blo_ver_df['Ver Coeff 6 Units'].astype('float64') * blo_ver_df['Coeff 6'].astype('float64'))
)) * blo_ver_df['adjusted_shift_length'].astype('float64'))

###Concat frames together
blo_final_df = pd.DataFrame()
blo_final_df = pd.concat([blo_proc_df, blo_ver_df], ignore_index=True)

blo_final_df.drop('Puncher', axis=1, inplace=True)

blo_final_df.rename(columns={'Preferred Name':'Puncher', 'BLO_PRODUCT_COUNT':'Units'}, inplace=True)

##Check for negatives and create final dataframe
blo_final_df["Test"] = ""
blo_final_df["Notes"] = ""

blo_final_df.loc[blo_final_df['Day %'].astype('float64') < 0, 'Day %'] = blo_final_df['Day %'].astype('float64') * -1

blo_final_df = blo_final_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name']]

###PVP Module
##Make pvp nuway dataframe and pvp sq data frame
pvp_nuway_df = nuway_df.copy()
pvp_nuway_df = pvp_nuway_df.loc[(pvp_nuway_df['Task'] == 'pvp') & (pvp_nuway_df['Subtask'].str[-3:] != 'sco')]

pvp_df = pvp_sql_df.copy()

pvp_df['QUEUE_NUMBER'] = pvp_df['QUEUE_NUMBER'].str.lower()

##Find dupes
pvp_nuway_df["Dupe"] = pvp_nuway_df['Punch'].astype(str) + pvp_nuway_df['Puncher'].astype(str) + pvp_nuway_df['Subtask'].astype(str)

pvp_dupe_df = pvp_nuway_df.copy()
pvp_dupe_df = pvp_dupe_df[pvp_dupe_df.duplicated(subset=['Dupe'], keep=False)]
pvp_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
pvp_dupe_df.loc[pvp_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
pvp_dupe_df = pvp_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pvp_dupe_df])

##Merge remaining data with SQ data and account for POQs
pvp_df = pd.merge(pvp_df, pvp_nuway_df, how='right', on='QUEUE_NUMBER')
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'SHIPPINGQUEUENUMBER'] = pvp_df['QUEUE_NUMBER']

##Find punches with bad sq number data
pvp_sq_error_df = pvp_df.copy()
pvp_sq_error_df = pvp_sq_error_df.loc[pvp_df['SHIPPINGQUEUENUMBER'].isna()]
pvp_sq_error_df['SHIPPINGQUEUENUMBER'] = pvp_sq_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")
pvp_sq_error_df.drop('Subtask', axis=1, inplace=True)
pvp_sq_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
pvp_sq_error_df = pvp_sq_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pvp_sq_error_df])

pvp_df = pvp_df.loc[pvp_df['SHIPPINGQUEUENUMBER'].notna()]

##Find takeovers with no orders completed
pvp_df['Orders Completed'] = pvp_df['Orders Completed'].apply(pd.to_numeric, errors='coerce')
pvp_orders_error_df = pvp_df.loc[(pvp_df['Subtask'].str[-3:] == 'tak') & (pvp_df['Orders Completed'] == '')]
pvp_orders_error_df.loc[pvp_orders_error_df['Orders Completed'] == '', 'Orders Completed'] = "Error"
pvp_orders_error_df = pvp_orders_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pvp_orders_error_df])

pvp_df.loc[(pvp_df['Orders Completed'] == '') & (pvp_df['Subtask'].str[-3:] == 'tak'), 'Punch'] = None

##Find sqs that don't exist
pvp_df['ORDER_COUNT'] = pvp_df['ORDER_COUNT'].apply(pd.to_numeric, errors='coerce').fillna(0)
pvp_na_error_df = pvp_df.loc[pvp_df['ORDER_COUNT'] == 0]
pvp_na_error_df.loc[pvp_na_error_df['ORDER_COUNT'] == 0, 'Subtask'] = "Error"
pvp_na_error_df = pvp_na_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pvp_na_error_df])

pvp_df = pvp_df.loc[pvp_df['ORDER_COUNT'] > 0]

##Remove errors from dataframe
pvp_df.drop_duplicates(subset=['Dupe'], inplace=True)
pvp_df.dropna(subset=['SHIPPINGQUEUENUMBER', 'Punch'], inplace=True)

##Normalize punches
pvp_df["normal_punch"] = pd.to_datetime(pvp_df['Punch']).dt.date

##Merge staffing data
pvp_df['Puncher'] = pvp_df['Puncher'].str.lower()
pvp_df = pd.merge(pvp_df, staffing_df, how='left', on='Puncher')

##Include current team members
pvp_df = pvp_df.loc[pvp_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
pvp_df["keep"] = None
pvp_df = pd.merge(pvp_df, shiftdata_df, how='left', on='Puncher')

pvp_df.loc[(pvp_df['Punch'] >= pvp_df['minimum']) & (pvp_df['Punch'] <= pvp_df['maximum']), 'keep'] = 1

pvp_df = pvp_df.loc[pvp_df['keep'] == 1]

##First Offset
pvp_df.rename(columns={'Date':'first_offset'}, inplace=True)
pvp_df['first_offset'] = pd.to_datetime(pvp_df['first_offset'], format='%m-%d-%Y').dt.date

##Fix columns
pvp_df.drop(['Units', 'Puncher'], axis=1, inplace=True)
pvp_df.rename(columns={'sq_card_quantity':'Units','Preferred Name':'Puncher'}, inplace=True)

##PVP Takeover Ratio
pvp_df.loc[(pvp_df['Subtask'].str[-3:] == 'tak') & (pvp_df['Orders Completed'] == 0), 'Orders Completed'] = 0
pvp_df["pvp_ratio"] = ""
pvp_df.loc[pvp_df['QUEUE_NUMBER'] != '', 'pvp_ratio'] = 1

pvp_df.loc[(pvp_df['Subtask'].str[-3:] == 'tak') & (pd.isnull(pvp_df['Orders Completed']) == True), 'Orders Completed'] = pvp_df['ORDER_COUNT'].astype('float64') / 2
pvp_df.loc[(pvp_df['Subtask'].str[-3:] == 'tak') & (pvp_df['Orders Completed'].astype('float64') == 0), 'Orders Completed'] = 1
pvp_df.loc[pvp_df['Subtask'].str[-3:] == 'tak', 'pvp_ratio'] = pvp_df['Orders Completed'].astype('float64') / pvp_df['ORDER_COUNT'].astype('float64')

pvp_df["pvp_cards"] = pvp_df['Units'].astype('float64') * pvp_df['pvp_ratio'].astype('float64')
pvp_df.drop('Units', axis=1, inplace=True)
pvp_df.rename(columns={'pvp_cards':'Units'}, inplace=True)

##PVP Order Count
pvp_df.loc[pvp_df['Subtask'].str[-3:] == 'tak', 'ORDER_COUNT'] = pvp_df['Orders Completed']

##SQ Type
pvp_df["sq_type"] = pvp_df['SHIPPINGQUEUENUMBER'].str[-3:]
pvp_df.loc[(pvp_df['SHIPPINGQUEUENUMBER'].map(len) == 16) & (pvp_df['SHIPPINGQUEUENUMBER'].str[-3:] != 'poq'), 'sq_type'] = pvp_df['SHIPPINGQUEUENUMBER'].str[-6:]

##Define PVP standards
pvp_standards_df = standards_df.copy()
pvp_standards_df = pvp_standards_df.loc[(pvp_standards_df['Task'] == 'PVP') | (pvp_standards_df['Task'] == 'Pre SQER PVP')]
pvp_standards_df['Subtask'] = pvp_standards_df['Subtask'].str.lower()
pvp_standards_df['Task'] = pvp_standards_df['Task'].str.lower()
pvp_standards_df["combined"] = pvp_standards_df['Task'].astype(str) + pvp_standards_df['Subtask'].astype(str)

pvp_df['SHIPPINGQUEUENUMBER'] = pvp_df['SHIPPINGQUEUENUMBER'].str.lower()
pvp_df['sq_type'] = pvp_df['sq_type'].str.lower()
pvp_df["combined"] = pvp_df['Task'].astype(str) + pvp_df['sq_type'].astype(str)

pvp_df = pd.merge(pvp_df, pvp_standards_df, left_on='combined', right_on='combined')

pvp_df.drop(['Task_y', 'Subtask_y'], axis=1, inplace=True)
pvp_df.rename(columns={'Task_x':'Task', 'Subtask_x':'Subtask'}, inplace=True)

##Aggragate errors
pvp_df["total_errors"] = pvp_df['Coeff 1 Units'].apply(pd.to_numeric, errors='coerce') + pvp_df['Coeff 2 Units'].apply(pd.to_numeric, errors='coerce') + pvp_df['Coeff 3 Units'].apply(pd.to_numeric, errors='coerce') + pvp_df['Coeff 4 Units'].apply(pd.to_numeric, errors='coerce') + pvp_df['Coeff 5 Units'].apply(pd.to_numeric, errors='coerce')

##Calculate Metrics
pvp_df = pvp_df.loc[(pvp_df['adjusted_shift_length'].astype('float64') > 0) & (pvp_df['Shift Name'] != 0)]

pvp_df["Coeff 1 Units"] = pvp_df['ORDER_COUNT']
pvp_df["Coeff 2 Units"] = 0
pvp_df["Coeff 3 Units"] = 0
pvp_df["Coeff 4 Units"] = 0
pvp_df["Coeff 5 Units"] = 0
pvp_df["Coeff 6 Units"] = 0

pvp_df.loc[pvp_df['Y-Int Def'] == 'CPH', 'Day %'] = pvp_df['Units'].astype('int64') / ((pvp_df['Y-Int'].astype('float64') +
(pvp_df['Coeff 1 Units'].astype('float64') * pvp_df['Coeff 1'].astype('float64')) +
(pvp_df['Coeff 2 Units'].astype('float64') * pvp_df['Coeff 2'].astype('float64')) +
(pvp_df['Coeff 3 Units'].astype('float64') * pvp_df['Coeff 3'].astype('float64')) +
(pvp_df['Coeff 4 Units'].astype('float64') * pvp_df['Coeff 4'].astype('float64')) +
(pvp_df['Coeff 5 Units'].astype('float64') * pvp_df['Coeff 5'].astype('float64')) +
(pvp_df['Coeff 6 Units'].astype('float64') * pvp_df['Coeff 6'].astype('float64'))
) * pvp_df['adjusted_shift_length'].astype('float64'))

pvp_df.loc[pvp_df['Y-Int Def'] == 'SPC', 'Day %'] = pvp_df['Units'].astype('int64') / (( 3600 / (pvp_df['Y-Int'].astype('float64') +
(pvp_df['Coeff 1 Units'].astype('float64') * pvp_df['Coeff 1'].astype('float64')) +
(pvp_df['Coeff 2 Units'].astype('float64') * pvp_df['Coeff 2'].astype('float64')) +
(pvp_df['Coeff 3 Units'].astype('float64') * pvp_df['Coeff 3'].astype('float64')) +
(pvp_df['Coeff 4 Units'].astype('float64') * pvp_df['Coeff 4'].astype('float64')) +
(pvp_df['Coeff 5 Units'].astype('float64') * pvp_df['Coeff 5'].astype('float64')) +
(pvp_df['Coeff 6 Units'].astype('float64') * pvp_df['Coeff 6'].astype('float64'))
)) * pvp_df['adjusted_shift_length'].astype('float64'))

##Create final dataframe
pvp_df.loc[(pvp_df['Task'] != '') & (pvp_df['Subtask'].str[-3:] == 'tak'), 'Task'] = "PVP Takeover"
pvp_df.loc[(pvp_df['Task'] != '') & (pvp_df['Subtask'].str[-3:] != 'tak'), 'Task'] = "PVP"
pvp_df.drop('Subtask', axis=1, inplace=True)

pvp_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask', 'ORDER_COUNT':'Orders'}, inplace=True)

pvp_df = pvp_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes','Orders']]

###PVP SCO Module
##Make pvp sco nuway dataframe, merge with sq data
pvp_sco_nuway_df = nuway_df.loc[(nuway_df['Task'] == 'pvp') & (nuway_df['Subtask'].str[-3:] == 'sco')]

pvp_sco_df = sq_slot_df.copy()
pvp_sco_df['QUEUE_NUMBER'] = pvp_sco_df['QUEUE_NUMBER'].str.lower()

##Merge remaining data with SQ data
pvp_sco_df = pd.merge(pvp_sco_df, pvp_sco_nuway_df, how='right', on='QUEUE_NUMBER')

##Find dupes
pvp_sco_df["Dupe"] = pvp_sco_df['SLOT'].astype(str) + pvp_sco_df['Flex Run'].astype(str) + pvp_sco_df['Env Run'].astype(str) + pvp_sco_df['Puncher'].astype(str) + pvp_sco_df['Subtask'].astype(str) + pvp_sco_df['Flex Run'].astype(str) + pvp_sco_df['Env Run'].astype(str)

pvp_sco_dupe_df = pvp_sco_df.copy()
pvp_sco_dupe_df = pvp_sco_dupe_df[pvp_sco_dupe_df.duplicated(subset=['Dupe'], keep=False)]
pvp_sco_dupe_df.drop('Dupe', axis=1, inplace=True)
pvp_sco_dupe_df["Dupe"] = pvp_sco_dupe_df['Flex Run'].astype(str) + pvp_sco_dupe_df['Env Run'].astype(str) + pvp_sco_dupe_df['Puncher'].astype(str) + pvp_sco_dupe_df['Subtask'].astype(str) + pvp_sco_dupe_df['Flex Run'].astype(str) + pvp_sco_dupe_df['Env Run'].astype(str)
pvp_sco_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)

pvp_sco_dupe_df.loc[pvp_sco_dupe_df['Dupe'] != '', 'Dupe'] = "Error"

pvp_sco_dupe_df = pvp_sco_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pvp_sco_dupe_df])

pvp_sco_df = pvp_sco_df.drop_duplicates(subset=['Dupe'])

##Find punches with bad sq number data
pvp_sco_sq_error_df = pvp_sco_df.copy()
pvp_sco_sq_error_df = pvp_sco_sq_error_df[pvp_sco_sq_error_df['SHIPPINGQUEUENUMBER'].isna()]
pvp_sco_sq_error_df['SHIPPINGQUEUENUMBER'] = pvp_sco_sq_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")
pvp_sco_sq_error_df.drop('Subtask', axis=1, inplace=True)
pvp_sco_sq_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
pvp_sco_sq_error_df = pvp_sco_sq_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pvp_sco_sq_error_df])

##Remove SQs without slot data or bad sq number data
pvp_sco_df.loc[pvp_sco_df['Flex Run'] == '', 'Flex Run'] = None
pvp_sco_df.loc[pvp_sco_df['Env Run'] == '', 'Env Run'] = None

pvp_sco_error_df = pvp_sco_df.copy()
pvp_sco_error_df = pvp_sco_error_df[(pvp_sco_error_df['SHIPPINGQUEUENUMBER'].isna()) | (pvp_sco_error_df['Flex Run'].isna()) | (pvp_sco_error_df['Env Run'].isna())]

pvp_sco_error_df['pvp_full_sq_number'] = pvp_sco_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")
pvp_sco_error_df.drop('Subtask', axis=1, inplace=True)
pvp_sco_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)

pvp_sco_error_df["sco_combined"] = pvp_sco_error_df['Punch'].astype(str) + pvp_sco_error_df['Puncher'].astype(str) + pvp_sco_error_df['Subtask'].astype(str)
pvp_sco_error_df = pvp_sco_error_df.drop_duplicates(subset=['sco_combined'])

pvp_sco_error_df = pvp_sco_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pvp_sco_error_df])

##Remove errors from dataframe
pvp_sco_error_df['Flex Run'] = pvp_sco_error_df['Flex Run'].fillna("Error")
pvp_sco_error_df['Env Run'] = pvp_sco_error_df['Env Run'].fillna("Error")
pvp_sco_df.dropna(subset=['SHIPPINGQUEUENUMBER', 'Flex Run', 'Env Run'], inplace=True)

##Normalize punches
pvp_sco_df["normal_punch"] = pd.to_datetime(pvp_sco_df['Punch']).dt.date

##Merge staffing data
pvp_sco_df['Puncher'] = pvp_sco_df['Puncher'].str.lower()
pvp_sco_df = pd.merge(pvp_sco_df, staffing_df, how='left', on='Puncher')

##Include current team members
pvp_sco_df = pvp_sco_df.loc[pvp_sco_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
pvp_sco_df["keep"] = None
pvp_sco_df = pd.merge(pvp_sco_df, shiftdata_df, how='left', on='Puncher')

pvp_sco_df.loc[(pvp_sco_df['Punch'] >= pvp_sco_df['minimum']) & (pvp_sco_df['Punch'] <= pvp_sco_df['maximum']), 'keep'] = 1

pvp_sco_df = pvp_sco_df.loc[pvp_sco_df['keep'] == 1]

##First Offset
pvp_sco_df.rename(columns={'Date':'first_offset'}, inplace=True)
pvp_sco_df['first_offset'] = pd.to_datetime(pvp_sco_df['first_offset'], format='%m-%d-%Y').dt.date

##Fix columns
pvp_sco_df.drop(['Units', 'Puncher'], axis=1, inplace=True)
pvp_sco_df.rename(columns={'card_qty_by_slot':'Units','Preferred Name':'Puncher'}, inplace=True)

##Deal with SCO slots
pvp_sco_df['Flex Run'] = pvp_sco_df['Flex Run'].str.lower()
pvp_sco_df['Env Run'] = pvp_sco_df['Env Run'].str.lower()

pvp_sco_df.loc[pvp_sco_df['Flex Run'] != '', 'Flex Run'] = pvp_sco_df['Flex Run'].str.strip().apply(ord) - 96
pvp_sco_df.loc[pvp_sco_df['Env Run'] != '', 'Env Run'] = pvp_sco_df['Env Run'].str.strip().apply(ord) - 96

pvp_sco_df["pvpd_quantity"] = ""
pvp_sco_df['Units'] = pvp_sco_df['Units'].apply(pd.to_numeric, errors='coerce')

pvp_sco_df.loc[(pvp_sco_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= pvp_sco_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (pvp_sco_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= pvp_sco_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'pvpd_quantity'] = pvp_sco_df['Units']

pvp_sco_df = pvp_sco_df.loc[pvp_sco_df['pvpd_quantity'] != '']

pvp_sco_df["sco_combined"] = pvp_sco_df['Punch'].astype(str) + pvp_sco_df['Puncher'].astype(str) + pvp_sco_df['SHIPPINGQUEUENUMBER'].astype(str)

pvpd_quantity_agg = pvp_sco_df.groupby('sco_combined')['pvpd_quantity'].sum()
pvp_sco_df = pd.merge(pvp_sco_df, pvpd_quantity_agg, how='right', on='sco_combined', suffixes=("_1", "_2"))

pvp_sco_df = pvp_sco_df.drop_duplicates(subset=['sco_combined'])
pvp_sco_df.drop(['sco_combined', 'Units'], axis=1, inplace=True)

pvp_sco_df.rename(columns={'pvpd_quantity_2': 'pvpd_quantity'}, inplace=True)

pvp_sco_df.drop_duplicates(subset='Dupe', inplace=True)

##Define SCO PVP standards
pvp_sco_standards_df = standards_df.copy()
pvp_sco_standards_df = pvp_sco_standards_df.loc[pvp_sco_standards_df['Task'] == 'PVP']

##Merge Standards with Dataframe
pvp_sco_standards_df['Subtask'] = pvp_sco_standards_df['Subtask'].str.lower()

pvp_sco_df["sq_type"] = pvp_sco_df['SHIPPINGQUEUENUMBER'].str[-6:].str.lower()
pvp_sco_df['SHIPPINGQUEUENUMBER'] = pvp_sco_df['SHIPPINGQUEUENUMBER'].str.lower()
pvp_sco_df.loc[pvp_sco_df['SHIPPINGQUEUENUMBER'].map(len) == 13, 'sq_type'] = pvp_sco_df['SHIPPINGQUEUENUMBER'].str[-3:]

pvp_sco_df = pd.merge(pvp_sco_df, pvp_sco_standards_df, left_on='sq_type', right_on='Subtask')

##Aggragate errors
pvp_sco_df["total_errors"] = pvp_sco_df['Coeff 1 Units'].apply(pd.to_numeric, errors='coerce') + pvp_sco_df['Coeff 2 Units'].apply(pd.to_numeric, errors='coerce') + pvp_sco_df['Coeff 3 Units'].apply(pd.to_numeric, errors='coerce') + pvp_sco_df['Coeff 4 Units'].apply(pd.to_numeric, errors='coerce') + pvp_sco_df['Coeff 5 Units'].apply(pd.to_numeric, errors='coerce')

##Calculate Metrics
pvp_sco_df = pvp_sco_df.loc[(pvp_sco_df['adjusted_shift_length'].astype('float64') > 0) & (pvp_sco_df['Shift Name'] != 0)]

pvp_sco_df["Units"] = pvp_sco_df['pvpd_quantity']

pvp_sco_df["Coeff 1 Units"] = pvp_sco_df['pvpd_quantity']
pvp_sco_df["Coeff 2 Units"] = 0
pvp_sco_df["Coeff 3 Units"] = 0
pvp_sco_df["Coeff 4 Units"] = 0
pvp_sco_df["Coeff 5 Units"] = 0
pvp_sco_df["Coeff 6 Units"] = 0

pvp_sco_df.loc[pvp_sco_df['Y-Int Def'] == 'CPH', 'Day %'] = pvp_sco_df['Units'].astype('int64') / ((pvp_sco_df['Y-Int'].astype('float64') +
(pvp_sco_df['Coeff 1 Units'].astype('float64') * pvp_sco_df['Coeff 1'].astype('float64')) +
(pvp_sco_df['Coeff 2 Units'].astype('float64') * pvp_sco_df['Coeff 2'].astype('float64')) +
(pvp_sco_df['Coeff 3 Units'].astype('float64') * pvp_sco_df['Coeff 3'].astype('float64')) +
(pvp_sco_df['Coeff 4 Units'].astype('float64') * pvp_sco_df['Coeff 4'].astype('float64')) +
(pvp_sco_df['Coeff 5 Units'].astype('float64') * pvp_sco_df['Coeff 5'].astype('float64')) +
(pvp_sco_df['Coeff 6 Units'].astype('float64') * pvp_sco_df['Coeff 6'].astype('float64'))
) * pvp_sco_df['adjusted_shift_length'])

pvp_sco_df.loc[pvp_sco_df['Y-Int Def'] == 'SPC', 'Day %'] = pvp_sco_df['Units'].astype('int64') / (( 3600 / (pvp_sco_df['Y-Int'].astype('float64') +
(pvp_sco_df['Coeff 1 Units'].astype('float64') * pvp_sco_df['Coeff 1'].astype('float64')) +
(pvp_sco_df['Coeff 2 Units'].astype('float64') * pvp_sco_df['Coeff 2'].astype('float64')) +
(pvp_sco_df['Coeff 3 Units'].astype('float64') * pvp_sco_df['Coeff 3'].astype('float64')) +
(pvp_sco_df['Coeff 4 Units'].astype('float64') * pvp_sco_df['Coeff 4'].astype('float64')) +
(pvp_sco_df['Coeff 5 Units'].astype('float64') * pvp_sco_df['Coeff 5'].astype('float64')) +
(pvp_sco_df['Coeff 6 Units'].astype('float64') * pvp_sco_df['Coeff 6'].astype('float64'))
)) * pvp_sco_df['adjusted_shift_length'])

##Create final dataframe
pvp_sco_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask', 'Task_x':'Task', 'pvpd_quantity':'Orders'}, inplace=True)

pvp_sco_df['Task'] = pvp_sco_df['Task'].str.upper()

pvp_sco_df = pvp_sco_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes','Orders']]

###Pull Verifying Module
##Make pull ver nuway dataframe
pullver_nuway_df = nuway_df.copy()
pullver_nuway_df = pullver_nuway_df.loc[pullver_nuway_df.Task == 'pull verifying']

pullver_df = sq_slot_df.copy()
pullver_df['QUEUE_NUMBER'] = pullver_df['QUEUE_NUMBER'].str.lower()

##Find dupes
pullver_nuway_df["Dupe"] = pullver_nuway_df['Puncher'].astype(str) + pullver_nuway_df['Subtask'].astype(str) + pullver_nuway_df['Flex Run'].astype(str) + pullver_nuway_df['Env Run'].astype(str)

pullver_dupe_df = pullver_nuway_df.copy()
pullver_dupe_df = pullver_dupe_df[pullver_dupe_df.duplicated(subset=['Dupe'], keep=False)]
pullver_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
pullver_dupe_df.loc[pullver_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
pullver_dupe_df = pullver_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pullver_dupe_df])

##Merge remaining data with SQ data and account for POQs
pullver_df = pd.merge(pullver_df, pullver_nuway_df, how='right', on='QUEUE_NUMBER')
pullver_df.loc[pullver_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'SHIPPINGQUEUENUMBER'] = pullver_df['QUEUE_NUMBER']

##Find punches with bad sq number data and punches missing slot data
pullver_df.loc[pullver_df['Flex Run'] == '', 'Flex Run'] = None
pullver_df.loc[pullver_df['Env Run'] == '', 'Env Run'] = None

pullver_sq_error_df = pullver_df.copy()
pullver_sq_error_df = pullver_sq_error_df[(pullver_sq_error_df['SHIPPINGQUEUENUMBER'].isna()) | (pullver_sq_error_df['Flex Run'].isna()) | (pullver_sq_error_df['Env Run'].isna())]

pullver_sq_error_df['SHIPPINGQUEUENUMBER'] = pullver_sq_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")
pullver_sq_error_df.drop('Subtask', axis=1, inplace=True)
pullver_sq_error_df['Flex Run'] = pullver_sq_error_df['Flex Run'].fillna("Error")
pullver_sq_error_df['Env Run'] = pullver_sq_error_df['Env Run'].fillna("Error")
pullver_sq_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
pullver_sq_error_df.drop_duplicates(subset=['Dupe'], inplace=True)

pullver_sq_error_df = pullver_sq_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pullver_sq_error_df])

##Remove errors from dataframe
pullver_df.dropna(subset=["SHIPPINGQUEUENUMBER", "Flex Run", "Env Run"], inplace=True)

##Normalize punches
pullver_df["normal_punch"] = pd.to_datetime(pullver_df['Punch']).dt.date

##Merge staffing data
pullver_df['Puncher'] = pullver_df['Puncher'].str.lower()
pullver_df = pd.merge(pullver_df, staffing_df, how='left', on='Puncher')

##Include current team members
pullver_df = pullver_df.loc[pullver_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
pullver_df["keep"] = None
pullver_df = pd.merge(pullver_df, shiftdata_df, how='left', on='Puncher')

pullver_df.loc[(pullver_df['Punch'] >= pullver_df['minimum']) & (pullver_df['Punch'] <= pullver_df['maximum']), 'keep'] = 1

pullver_df = pullver_df.loc[pullver_df['keep'] == 1]

##First Offset
pullver_df.rename(columns={'Date':'first_offset'}, inplace=True)
pullver_df['first_offset'] = pd.to_datetime(pullver_df['first_offset'], format='%m-%d-%Y').dt.date

##Fix columns
pullver_df.drop(['Units', 'Puncher'], axis=1, inplace=True)
pullver_df.rename(columns={'card_qty_by_slot':'Units','Preferred Name':'Puncher'}, inplace=True)

##Deal with slots
pullver_df['Flex Run'] = pullver_df['Flex Run'].str.lower()
pullver_df['Env Run'] = pullver_df['Env Run'].str.lower()

pullver_df.loc[(pullver_df['Flex Run'] != '') & (pullver_df['Subtask'].str[-3:] != 'roc'), 'Flex Run'] = pullver_df['Flex Run'].str.strip().apply(ord) - 96
pullver_df.loc[(pullver_df['Env Run'] != '') & (pullver_df['Subtask'].str[-3:] != 'roc'), 'Env Run'] = pullver_df['Env Run'].str.strip().apply(ord) - 96

##Remove slots/pcids not verified and aggregate
pullver_df["verified_quantity"] = ""

pullver_df['Units'] = pullver_df['Units'].apply(pd.to_numeric, errors='coerce')

pullver_df = pullver_df.loc[pullver_df['SLOT'].apply(pd.to_numeric, errors='coerce') != 0]

pullver_df.loc[(pullver_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= pullver_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (pullver_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= pullver_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'verified_quantity'] = pullver_df['Units']

pullver_df["pullver_combined"] = pullver_df['Punch'].astype(str) + pullver_df['Puncher'].astype(str) + pullver_df['SHIPPINGQUEUENUMBER'].astype(str)

pullver_df = pullver_df.loc[pullver_df['verified_quantity'] != '']
pullver_df['verified_quantity'] = pullver_df['verified_quantity'].astype('int64')

verified_quantity_agg = pullver_df.groupby('pullver_combined')['verified_quantity'].sum()
pullver_df = pd.merge(pullver_df, verified_quantity_agg, how='right', on='pullver_combined')

pullver_df = pullver_df.drop_duplicates(subset=['pullver_combined'])

##Sum pull ver discrepancies
pullver_df['Coeff 1 Units'] = pullver_df['Coeff 1 Units'].apply(pd.to_numeric, errors='coerce').fillna(0)
pullver_df['Coeff 2 Units'] = pullver_df['Coeff 2 Units'].apply(pd.to_numeric, errors='coerce').fillna(0)
pullver_df['Coeff 3 Units'] = pullver_df['Coeff 3 Units'].apply(pd.to_numeric, errors='coerce').fillna(0)
pullver_df['Coeff 4 Units'] = pullver_df['Coeff 4 Units'].apply(pd.to_numeric, errors='coerce').fillna(0)
pullver_df['Coeff 5 Units'] = pullver_df['Coeff 5 Units'].apply(pd.to_numeric, errors='coerce').fillna(0)

pullver_df["total_errors"] = pullver_df['Coeff 1 Units'] + pullver_df['Coeff 2 Units'] + pullver_df['Coeff 3 Units'] + pullver_df['Coeff 4 Units'] + pullver_df['Coeff 5 Units']

##Define pull ver standards
pullver_standards_df = standards_df.loc[standards_df.Task == 'Pull Ver']

##Merge pull ver standards with dataframe
pullver_df['Task'] = 'Pull Ver'
pullver_df = pd.merge(pullver_df, pullver_standards_df, left_on = "Task", right_on = "Task")

##Calculate Metrics
pullver_df.drop('Units', axis=1, inplace=True)
pullver_df = pullver_df.loc[(pullver_df['adjusted_shift_length'].astype('float64') > 0) & (pullver_df['Shift Name'] != 0)]

pullver_df["Units"] = pullver_df['verified_quantity_y']

pullver_df["Coeff 1 Units"] = pullver_df['total_errors']
pullver_df["Coeff 2 Units"] = 0
pullver_df["Coeff 3 Units"] = 0
pullver_df["Coeff 4 Units"] = 0
pullver_df["Coeff 5 Units"] = 0
pullver_df["Coeff 6 Units"] = 0

pullver_df.loc[pullver_df['Y-Int Def'] == 'CPH', 'Day %'] = pullver_df['Units'].astype('int64') / ((pullver_df['Y-Int'].astype('float64') +
(pullver_df['Coeff 1 Units'].astype('float64') * pullver_df['Coeff 1'].astype('float64')) +
(pullver_df['Coeff 2 Units'].astype('float64') * pullver_df['Coeff 2'].astype('float64')) +
(pullver_df['Coeff 3 Units'].astype('float64') * pullver_df['Coeff 3'].astype('float64')) +
(pullver_df['Coeff 4 Units'].astype('float64') * pullver_df['Coeff 4'].astype('float64')) +
(pullver_df['Coeff 5 Units'].astype('float64') * pullver_df['Coeff 5'].astype('float64')) +
(pullver_df['Coeff 6 Units'].astype('float64') * pullver_df['Coeff 6'].astype('float64'))
) * pullver_df['adjusted_shift_length'])

pullver_df.loc[pullver_df['Y-Int Def'] == 'SPC', 'Day %'] = pullver_df['Units'].astype('int64') / (( 3600 / (pullver_df['Y-Int'].astype('float64') +
(pullver_df['Coeff 1 Units'].astype('float64') * pullver_df['Coeff 1'].astype('float64')) +
(pullver_df['Coeff 2 Units'].astype('float64') * pullver_df['Coeff 2'].astype('float64')) +
(pullver_df['Coeff 3 Units'].astype('float64') * pullver_df['Coeff 3'].astype('float64')) +
(pullver_df['Coeff 4 Units'].astype('float64') * pullver_df['Coeff 4'].astype('float64')) +
(pullver_df['Coeff 5 Units'].astype('float64') * pullver_df['Coeff 5'].astype('float64')) +
(pullver_df['Coeff 6 Units'].astype('float64') * pullver_df['Coeff 6'].astype('float64'))
)) * pullver_df['adjusted_shift_length'])

##Create final dataframe
pullver_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)

pullver_df = pullver_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###Pulling Module
##Make pull nuway dataframe, merge wuth nuway dataframe
pull_nuway_df = nuway_df.copy()
pull_nuway_df = nuway_df.loc[nuway_df.Task == 'pulling']

pull_df = sq_slot_df.copy()

pull_df['QUEUE_NUMBER'] = pull_df['QUEUE_NUMBER'].str.lower()

##Find dupes
pull_dupe_df = pull_nuway_df.copy()
pull_dupe_df["Dupe"] = pull_dupe_df['Puncher'].astype(str) + pull_dupe_df['Subtask'].astype(str) + pull_dupe_df['Flex Run'].astype(str) + pull_dupe_df['Env Run'].astype(str)
pull_dupe_df = pull_dupe_df[pull_dupe_df.duplicated(subset=['Dupe'], keep=False)]
pull_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
pull_dupe_df.loc[pull_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
pull_dupe_df = pull_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pull_dupe_df])

##Merge remaining data with SQ data and account for POQs
pull_df = pd.merge(pull_df, pull_nuway_df, how='right', on='QUEUE_NUMBER')
pull_df.loc[pull_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'SHIPPINGQUEUENUMBER'] = pull_df['QUEUE_NUMBER']

##Remove punches with bad sq number data and punches missing slot data
pull_df.loc[pull_df['Flex Run'] == '', 'Flex Run'] = None
pull_df.loc[pull_df['Env Run'] == '', 'Env Run'] = None

pull_sq_error_df = pull_df.copy()
pull_sq_error_df = pull_sq_error_df[(pull_sq_error_df['SHIPPINGQUEUENUMBER'].isna()) | (pull_sq_error_df['Flex Run'].isna()) | (pull_sq_error_df['Env Run'].isna())]
pull_sq_error_df['SHIPPINGQUEUENUMBER'] = pull_sq_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")

pull_sq_error_df.drop('Subtask', axis=1, inplace=True)
pull_sq_error_df['Flex Run'] = pull_sq_error_df['Flex Run'].fillna("Error")
pull_sq_error_df['Env Run'] = pull_sq_error_df['Env Run'].fillna("Error")
pull_sq_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)

pull_sq_error_df = pull_sq_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, pull_sq_error_df])

##Remove errors from dataframe
pull_df.dropna(subset=["SHIPPINGQUEUENUMBER", "Flex Run", "Env Run"], inplace=True)

##Normalize punches
pull_df["normal_punch"] = pd.to_datetime(pull_df['Punch']).dt.date

##Merge staffing data
pull_df['Puncher'] = pull_df['Puncher'].str.lower()
pull_df = pd.merge(pull_df, staffing_df, how='left', on='Puncher')

##Include current team members
pull_df = pull_df.loc[pull_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
pull_df["keep"] = None
pull_df = pd.merge(pull_df, shiftdata_df, how='left', on='Puncher')

pull_df.loc[(pull_df['Punch'] >= pull_df['minimum']) & (pull_df['Punch'] <= pull_df['maximum']), 'keep'] = 1

pull_df = pull_df.loc[pull_df['keep'] == 1]

##First Offset
pull_df.rename(columns={'Date':'first_offset'}, inplace=True)
pull_df['first_offset'] = pd.to_datetime(pull_df['first_offset'], format='%m-%d-%Y').dt.date

##Fix columns
pull_df.drop(['Units', 'Puncher'], axis=1, inplace=True)
pull_df.rename(columns={'card_qty_by_slot':'Units', 'Preferred Name':'Puncher'}, inplace=True)

##Deal with slots
pull_df['Flex Run'] = pull_df['Flex Run'].str.lower()
pull_df['Env Run'] = pull_df['Env Run'].str.lower()

pull_df.loc[(pull_df['Flex Run'] != '') & (pull_df['Subtask'].str[-3:] != 'roc'), 'Flex Run'] = pull_df['Flex Run'].str.strip().apply(ord) - 96
pull_df.loc[(pull_df['Env Run'] != '') & (pull_df['Subtask'].str[-3:] != 'roc'), 'Env Run'] = pull_df['Env Run'].str.strip().apply(ord) - 96

##Remove slots/pcids not pulled and aggregate
pull_df["pulled_quantity"] = ""
pull_df["pulled_pcids"] = ""

pull_df = pull_df.loc[pull_df['SLOT'] != '']

pull_df.loc[(pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= pull_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= pull_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'pulled_quantity'] = pull_df['Units']

pull_df.loc[(pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') >= pull_df['Flex Run'].apply(pd.to_numeric, errors='coerce')) & (pull_df['SLOT'].apply(pd.to_numeric, errors='coerce') <= pull_df['Env Run'].apply(pd.to_numeric, errors='coerce')), 'pulled_pcids'] = pull_df['unique_pcids_by_slot']

pull_df['pulled_quantity'] = pull_df['pulled_quantity'].apply(pd.to_numeric, errors='coerce')
pull_df['pulled_pcids'] = pull_df['pulled_pcids'].apply(pd.to_numeric, errors='coerce')

pull_df = pull_df.loc[pull_df['pulled_quantity'] != '']
pull_df = pull_df.loc[pull_df['pulled_pcids'] != '']

pull_df["pull_combined"] = pull_df['Punch'].astype(str) + pull_df['Puncher'].astype(str) + pull_df['SHIPPINGQUEUENUMBER'].astype(str)

pulled_quantity_agg = pull_df.groupby('pull_combined')['pulled_quantity'].sum()
pull_df = pd.merge(pull_df, pulled_quantity_agg, how='right', on='pull_combined', suffixes=("_1", "_2"))

pulled_pcids_agg = pull_df.groupby('pull_combined')['pulled_pcids'].sum()
pull_df = pd.merge(pull_df, pulled_pcids_agg, how='right', on='pull_combined', suffixes=("_1", "_2"))

pull_df = pull_df.drop_duplicates(subset=['pull_combined'])

pull_df.rename(columns={'pulled_quantity_2': 'pulled_quantity', 'pulled_pcids_2': 'pulled_pcids'}, inplace=True)

pull_df = pull_df.loc[pull_df['pulled_pcids'] != 0]

##Calculate density of cards pulled
pull_df["pulled_density"] = pull_df['pulled_quantity'] / pull_df['pulled_pcids']

##Define pull standards
pull_standards_df = standards_df.copy()
pull_standards_df = pull_standards_df.loc[pull_standards_df['Task'] == 'Pull']

##Merge pull standards with dataframe
pull_standards_df['Subtask'] = pull_standards_df['Subtask'].str.lower()
pull_df['SHIPPINGQUEUENUMBER'] = pull_df['SHIPPINGQUEUENUMBER'].str.lower()

pull_df["sq_type"] = pull_df['SHIPPINGQUEUENUMBER'].str[-3:]
pull_df.loc[(pull_df['SHIPPINGQUEUENUMBER'].map(len) == 16) & (pull_df['SHIPPINGQUEUENUMBER'].str[-3:] != 'poq'), 'sq_type'] = pull_df['SHIPPINGQUEUENUMBER'].str[-6:]

pull_df = pd.merge(pull_df, pull_standards_df, left_on = "sq_type", right_on = "Subtask")

##Calculate Metrics
pull_df.drop('Units', axis=1, inplace=True)
pull_df = pull_df.loc[(pull_df['adjusted_shift_length'].astype('float64') > 0) & (pull_df['Shift Name'] != 0)]

pull_df["Units"] = pull_df['pulled_quantity']

pull_df["Coeff 1 Units"] = pull_df['pulled_density']
pull_df["Coeff 2 Units"] = 0
pull_df["Coeff 3 Units"] = 0
pull_df["Coeff 4 Units"] = 0
pull_df["Coeff 5 Units"] = 0
pull_df["Coeff 6 Units"] = 0

pull_df.loc[pull_df['Y-Int Def'] == 'CPH', 'Day %'] = pull_df['Units'].astype('int64') / ((pull_df['Y-Int'].astype('float64') +
(pull_df['Coeff 1 Units'].astype('float64') * pull_df['Coeff 1'].astype('float64')) +
(pull_df['Coeff 2 Units'].astype('float64') * pull_df['Coeff 2'].astype('float64')) +
(pull_df['Coeff 3 Units'].astype('float64') * pull_df['Coeff 3'].astype('float64')) +
(pull_df['Coeff 4 Units'].astype('float64') * pull_df['Coeff 4'].astype('float64')) +
(pull_df['Coeff 5 Units'].astype('float64') * pull_df['Coeff 5'].astype('float64')) +
(pull_df['Coeff 6 Units'].astype('float64') * pull_df['Coeff 6'].astype('float64'))
) * pull_df['adjusted_shift_length'])

pull_df.loc[pull_df['Y-Int Def'] == 'SPC', 'Day %'] = pull_df['Units'].astype('int64') / (( 3600 / (pull_df['Y-Int'].astype('float64') +
(pull_df['Coeff 1 Units'].astype('float64') * pull_df['Coeff 1'].astype('float64')) +
(pull_df['Coeff 2 Units'].astype('float64') * pull_df['Coeff 2'].astype('float64')) +
(pull_df['Coeff 3 Units'].astype('float64') * pull_df['Coeff 3'].astype('float64')) +
(pull_df['Coeff 4 Units'].astype('float64') * pull_df['Coeff 4'].astype('float64')) +
(pull_df['Coeff 5 Units'].astype('float64') * pull_df['Coeff 5'].astype('float64')) +
(pull_df['Coeff 6 Units'].astype('float64') * pull_df['Coeff 6'].astype('float64'))
)) * pull_df['adjusted_shift_length'])

##Create final dataframe
pull_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask', 'Task_y':'Task'}, inplace=True)
pull_df = pull_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###Gen Tasks Module
##Make gen nuway dataframe
gen_df = nuway_df.copy()
gen_df = gen_df.loc[gen_df['Task'] == 'general tasks']

##Normalize punches
gen_df["normal_punch"] = pd.to_datetime(gen_df['Punch']).dt.date

##Merge staffing data
gen_df['Puncher'] = gen_df['Puncher'].str.lower()
gen_df = pd.merge(gen_df, staffing_df, how='left', on='Puncher')

##Include current team members
gen_df = gen_df.loc[gen_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
gen_df["keep"] = None
gen_df = pd.merge(gen_df, shiftdata_df, how='left', on='Puncher')

gen_df.loc[(gen_df['Punch'] >= gen_df['minimum']) & (gen_df['Punch'] <= gen_df['maximum']), 'keep'] = 1

gen_df = gen_df.loc[gen_df['keep'] == 1]

##First Offset
gen_df.rename(columns={'Date':'first_offset'}, inplace=True)
gen_df['first_offset'] = pd.to_datetime(gen_df['first_offset'], format='%m-%d-%Y').dt.date

##Define gen standards
gen_standards_df = standards_df.copy()
gen_standards_df = gen_standards_df.loc[gen_standards_df['Task'] == 'General Tasks']
gen_standards_df['Subtask'] = gen_standards_df['Subtask'].str.lower()
gen_df = pd.merge(gen_df, gen_standards_df, left_on='Subtask', right_on='Subtask')

##Determine gen credit
gen_df["gen_credit"] = ""

gen_df['Minutes Credit'] = gen_df['Minutes Credit'].apply(pd.to_numeric, errors='coerce')
gen_df['Minutes Credit'] = gen_df['Minutes Credit'].fillna('None')

gen_df.loc[gen_df['Units'] != None, 'gen_credit'] = gen_df['Units']
gen_df.loc[gen_df['Minutes Credit'] != 'None', 'gen_credit'] = gen_df['Minutes Credit']

##Remove punches with no gen credit
gen_nc_df = gen_df.copy()
gen_nc_df = gen_nc_df.loc[gen_nc_df['gen_credit'] == '']
gen_nc_df.loc[gen_nc_df['gen_credit'] == '', 'gen_credit'] = None

gen_nc_df['Units'] = gen_nc_df['Units'].fillna("Error")
gen_nc_df.rename(columns={'Task_x':'Task'}, inplace=True)

gen_nc_df = gen_nc_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, gen_nc_df])

gen_df = gen_df.loc[gen_df['gen_credit'] != '']

##Calculate gen metrics
gen_df = gen_df.loc[pd.to_numeric(gen_df['adjusted_shift_length'].astype('float64'), errors='coerce') > 0]

gen_df["Day %"] = (gen_df['gen_credit'].astype('float64') / 60) / gen_df['adjusted_shift_length'].astype('float64')

##Prepare dataframe for final write and create final dataframe
gen_df.drop('Puncher', axis=1, inplace=True)
gen_df.rename(columns={'Task_y':'Task', 'Preferred Name':'Puncher'}, inplace=True)

gen_df = gen_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

gen_df["dupe"] = gen_df['Punch'].astype(str) + gen_df['Puncher'].astype(str) + gen_df['Subtask'].astype(str) + gen_df['Units'].astype(str)
gen_df.drop_duplicates(subset=['dupe'], inplace=True)
gen_df.drop('dupe', axis=1, inplace=True)

###Filing Module
##Build filing dataframe
filing_df = nuway_df.copy()
filing_df = filing_df.loc[filing_df.Task == 'filing']

##Find dupes
filing_df["Dupe"] = filing_df['Punch'].astype(str) + filing_df['Puncher'].astype(str) + filing_df['Units'].astype(str)

filing_dupe_df = filing_df.copy()
filing_dupe_df = filing_dupe_df[filing_dupe_df.duplicated(subset=['Dupe'], keep=False)]
filing_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
filing_dupe_df.loc[filing_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
filing_dupe_df = filing_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, filing_dupe_df])

##Find non numerical filing data
filing_df['Units'] = filing_df['Units'].apply(pd.to_numeric, errors='coerce')
filing_cards_error_df = filing_df[filing_df['Units'].isna()]
filing_cards_error_df['Units'] = filing_cards_error_df['Units'].fillna("Error")

filing_cards_error_df = filing_cards_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, filing_cards_error_df])

##Remove errors from dataframe
filing_df.drop_duplicates(subset='Dupe', inplace=True)
filing_df.dropna(subset=["Units"], inplace=True)

##Normalize punches
filing_df["normal_punch"] = pd.to_datetime(filing_df['Punch']).dt.date

##Merge staffing data
filing_df['Puncher'] = filing_df['Puncher'].str.lower()
filing_df = pd.merge(filing_df, staffing_df, how='left', on='Puncher')

##Include current team members
filing_df = filing_df.loc[filing_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
filing_df["keep"] = None
filing_df = pd.merge(filing_df, shiftdata_df, how='left', on='Puncher')

filing_df.loc[(filing_df['Punch'] >= filing_df['minimum']) & (filing_df['Punch'] <= filing_df['maximum']), 'keep'] = 1

filing_df = filing_df.loc[filing_df['keep'] == 1]

##First Offset
filing_df.rename(columns={'Date':'first_offset'}, inplace=True)
filing_df['first_offset'] = pd.to_datetime(filing_df['first_offset'], format='%m-%d-%Y').dt.date

##Define sizes for RI filing runs
filing_df["ri_size"] = "Medium"

filing_df.loc[(filing_df['Units'] <= float(small)) & (filing_df['Subtask'] == 'ris'), 'ri_size'] = "Small"
filing_df.loc[(filing_df['Units'] > float(large)) & (filing_df['Subtask'] == 'ris'), 'ri_size'] = "Large"
filing_df.loc[filing_df['Subtask'] != 'ris', 'ri_size'] = "-"

filing_df["Combined"] = filing_df['Subtask'].map(str) + filing_df['ri_size'].map(str)

##Build filing standards dataframe
filing_standards_df = standards_df.copy()
filing_standards_df = filing_standards_df.loc[filing_standards_df['Task'] == 'Filing']
filing_standards_df['Subtask'] = filing_standards_df['Subtask'].str.lower()
filing_standards_df["Combined"] = filing_standards_df['Subtask'].map(str) + filing_standards_df['Size'].map(str)

##Merge Standards with Dataframe
filing_df = pd.merge(filing_df, filing_standards_df, left_on = "Combined", right_on = "Combined")

##Calculate Metrics
filing_df = filing_df.loc[(filing_df['adjusted_shift_length'].astype('float64') > 0) & (filing_df['Shift Name'] != 0)]

filing_df["Coeff 1 Units"] = filing_df['Units']
filing_df["Coeff 2 Units"] = 0
filing_df["Coeff 3 Units"] = 0
filing_df["Coeff 4 Units"] = 0
filing_df["Coeff 5 Units"] = 0
filing_df["Coeff 6 Units"] = 0

filing_df.loc[filing_df['Y-Int Def'] == 'CPH', 'Day %'] = filing_df['Units'].astype('int64') / ((filing_df['Y-Int'].astype('float64') +
(filing_df['Coeff 1 Units'].astype('float64') * filing_df['Coeff 1'].astype('float64')) +
(filing_df['Coeff 2 Units'].astype('float64') * filing_df['Coeff 2'].astype('float64')) +
(filing_df['Coeff 3 Units'].astype('float64') * filing_df['Coeff 3'].astype('float64')) +
(filing_df['Coeff 4 Units'].astype('float64') * filing_df['Coeff 4'].astype('float64')) +
(filing_df['Coeff 5 Units'].astype('float64') * filing_df['Coeff 5'].astype('float64')) +
(filing_df['Coeff 6 Units'].astype('float64') * filing_df['Coeff 6'].astype('float64'))
) * filing_df['adjusted_shift_length'])

filing_df.loc[filing_df['Y-Int Def'] == 'SPC', 'Day %'] = filing_df['Units'].astype('int64') / (( 3600 / (filing_df['Y-Int'].astype('float64') +
(filing_df['Coeff 1 Units'].astype('float64') * filing_df['Coeff 1'].astype('float64')) +
(filing_df['Coeff 2 Units'].astype('float64') * filing_df['Coeff 2'].astype('float64')) +
(filing_df['Coeff 3 Units'].astype('float64') * filing_df['Coeff 3'].astype('float64')) +
(filing_df['Coeff 4 Units'].astype('float64') * filing_df['Coeff 4'].astype('float64')) +
(filing_df['Coeff 5 Units'].astype('float64') * filing_df['Coeff 5'].astype('float64')) +
(filing_df['Coeff 6 Units'].astype('float64') * filing_df['Coeff 6'].astype('float64'))
)) * filing_df['adjusted_shift_length'])

##Create final dataframe
filing_df.drop(['Puncher', 'Task_x'], axis=1, inplace=True)
filing_df.rename(columns={'Preferred Name':'Puncher', 'Subtask_y':'Subtask', 'Task_y':'Task'}, inplace=True)

filing_df.loc[filing_df['Subtask'] == 'ris', 'Subtask'] = 'RIs'
filing_df.loc[filing_df['Subtask'] == 'buylist', 'Subtask'] = 'Buylist'
filing_df.loc[filing_df['Subtask'] == 'syp', 'Subtask'] = 'SYP'

filing_df = filing_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###Sort Module
##Build sort dataframe
sort_df = nuway_df.copy()
sort_df = sort_df.loc[sort_df['Task'] == 'sort']

##Find dupes
sort_df["Dupe"] = sort_df['Punch'].astype(str) + sort_df['Puncher'].astype(str) + sort_df['Units'].astype(str)

sort_dupe_df = sort_df[sort_df.duplicated(subset=['Dupe'], keep=False)]
sort_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
sort_dupe_df.loc[sort_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
sort_dupe_df = sort_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, sort_dupe_df])

##Find non numerical sort data
sort_df['Units'] = sort_df['Units'].apply(pd.to_numeric, errors='coerce')
sort_cards_error_df = sort_df[sort_df['Units'].isna()]
sort_cards_error_df['Units'] = sort_cards_error_df['Units'].fillna("Error")

sort_cards_error_df = sort_cards_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, sort_cards_error_df])

##Remove errors from dataframe
sort_df.dropna(subset=["Units"], inplace=True)

##Normalize punches
sort_df["normal_punch"] = pd.to_datetime(sort_df['Punch']).dt.date

##Merge staffing data
sort_df['Puncher'] = sort_df['Puncher'].str.lower()
sort_df = pd.merge(sort_df, staffing_df, how='left', on='Puncher')

##Include current team members
sort_df = sort_df.loc[sort_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
sort_df["keep"] = None
sort_df = pd.merge(sort_df, shiftdata_df, how='left', on='Puncher')

sort_df.loc[(sort_df['Punch'] >= sort_df['minimum']) & (sort_df['Punch'] <= sort_df['maximum']), 'keep'] = 1

sort_df = sort_df.loc[sort_df['keep'] == 1]

##First Offset
sort_df.rename(columns={'Date':'first_offset'}, inplace=True)
sort_df['first_offset'] = pd.to_datetime(sort_df['first_offset'], format='%m-%d-%Y').dt.date

##Build Sort standards dataframe
sort_standards_df = standards_df.copy()
sort_standards_df = sort_standards_df.loc[sort_standards_df['Task'] == 'Sort']

##Merge Standards with Dataframe
sort_df.loc[sort_df['Subtask'] == 'roca operator', 'Subtask'] = 'Roca Operator'
sort_standards_df['Subtask'] = sort_standards_df['Subtask'].str.lower()
sort_df = pd.merge(sort_df, sort_standards_df, left_on = "Subtask", right_on = "Subtask")

##Calculate Metrics
sort_df = sort_df.loc[(sort_df['adjusted_shift_length'].astype('float64') > 0) & (sort_df['Shift Name'] != 0)]

sort_df["Coeff 1 Units"] = sort_df['Units']
sort_df["Coeff 2 Units"] = 0
sort_df["Coeff 3 Units"] = 0
sort_df["Coeff 4 Units"] = 0
sort_df["Coeff 5 Units"] = 0
sort_df["Coeff 6 Units"] = 0

sort_df.loc[sort_df['Y-Int Def'] == 'CPH', 'Day %'] = sort_df['Units'].astype('int64') / ((sort_df['Y-Int'].astype('float64') +
(sort_df['Coeff 1 Units'].astype('float64') * sort_df['Coeff 1'].astype('float64')) +
(sort_df['Coeff 2 Units'].astype('float64') * sort_df['Coeff 2'].astype('float64')) +
(sort_df['Coeff 3 Units'].astype('float64') * sort_df['Coeff 3'].astype('float64')) +
(sort_df['Coeff 4 Units'].astype('float64') * sort_df['Coeff 4'].astype('float64')) +
(sort_df['Coeff 5 Units'].astype('float64') * sort_df['Coeff 5'].astype('float64')) +
(sort_df['Coeff 6 Units'].astype('float64') * sort_df['Coeff 6'].astype('float64'))
) * sort_df['adjusted_shift_length'])

sort_df.loc[sort_df['Y-Int Def'] == 'SPC', 'Day %'] = sort_df['Units'].astype('int64') / (( 3600 / (sort_df['Y-Int'].astype('float64') +
(sort_df['Coeff 1 Units'].astype('float64') * sort_df['Coeff 1'].astype('float64')) +
(sort_df['Coeff 2 Units'].astype('float64') * sort_df['Coeff 2'].astype('float64')) +
(sort_df['Coeff 3 Units'].astype('float64') * sort_df['Coeff 3'].astype('float64')) +
(sort_df['Coeff 4 Units'].astype('float64') * sort_df['Coeff 4'].astype('float64')) +
(sort_df['Coeff 5 Units'].astype('float64') * sort_df['Coeff 5'].astype('float64')) +
(sort_df['Coeff 6 Units'].astype('float64') * sort_df['Coeff 6'].astype('float64'))
)) * sort_df['adjusted_shift_length'])

##Create final dataframe
sort_df.drop('Puncher', axis=1, inplace=True)
sort_df.rename(columns={'Preferred Name':'Puncher','Task_y':'Task'}, inplace=True)

sort_df.loc[sort_df['Subtask'] == "mtg processing", 'Subtask'] = "MTG Processing"
sort_df.loc[sort_df['Subtask'] == "mtg verifying", 'Subtask'] = "MTG Verifying"
sort_df.loc[sort_df['Subtask'] == "mtg manual entry", 'Subtask'] = "MTG Manual Entry"
sort_df.loc[sort_df['Subtask'] == "intake", 'Subtask'] = "Intake"
sort_df.loc[sort_df['Subtask'] == "pkm processing", 'Subtask'] = "PKM Processing"
sort_df.loc[sort_df['Subtask'] == "pkm verifying", 'Subtask'] = "PKM Verifying"
sort_df.loc[sort_df['Subtask'] == "pkm manual entry", 'Subtask'] = "PKM Manual Entry"

sort_df = sort_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###SYP Module
##Build syp dataframe
syp_df = nuway_df.copy()
syp_df = syp_df.loc[syp_df['Task'] == 'syp proc']

##Find dupes
syp_df["Dupe"] = syp_df['Punch'].astype(str) + syp_df['Puncher'].astype(str) + syp_df['Units'].astype(str)

syp_dupe_df = syp_df[syp_df.duplicated(subset=['Dupe'], keep=False)]
syp_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
syp_dupe_df.loc[syp_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
syp_dupe_df = syp_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, syp_dupe_df])

##Find non numerical syp data
syp_df['Units'] = syp_df['Units'].apply(pd.to_numeric, errors='coerce')
syp_cards_error_df = syp_df[syp_df['Units'].isna()]

syp_cards_error_df['Units'] = syp_cards_error_df['Units'].fillna("Error")

syp_cards_error_df = syp_cards_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, syp_cards_error_df])

##Remove errors from dataframe
syp_df.dropna(subset=["Units"], inplace=True)

##Normalize punches
syp_df["normal_punch"] = pd.to_datetime(syp_df['Punch']).dt.date

##Merge staffing data
syp_df['Puncher'] = syp_df['Puncher'].str.lower()
syp_df = pd.merge(syp_df, staffing_df, how='left', on='Puncher')

##Include current team members
syp_df = syp_df.loc[syp_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Define syp proc sizes
syp_df["syp_size"] = "Medium"
syp_df.loc[syp_df['Units'] <= small, 'syp_size'] = "Small"
syp_df.loc[syp_df['Units'] > large, 'syp_size'] = "Large"

##Merge shift data
syp_df["keep"] = None
syp_df = pd.merge(syp_df, shiftdata_df, how='left', on='Puncher')

syp_df.loc[(syp_df['Punch'] >= syp_df['minimum']) & (syp_df['Punch'] <= syp_df['maximum']), 'keep'] = 1

syp_df = syp_df.loc[syp_df['keep'] == 1]

##First Offset
syp_df.rename(columns={'Date':'first_offset'}, inplace=True)
syp_df['first_offset'] = pd.to_datetime(syp_df['first_offset'], format='%m-%d-%Y').dt.date

##Build SYP standards dataframe
syp_standards_df = standards_df.copy()
syp_standards_df = syp_standards_df.loc[syp_standards_df['Task'] == 'SYP']

##Merge Standards with Dataframe
syp_standards_df['Subtask'] = syp_standards_df['Subtask'].str.lower()
syp_df = pd.merge(syp_df, syp_standards_df, left_on = "syp_size", right_on = "Size")

##Calculate Metrics
syp_df = syp_df.loc[(syp_df['adjusted_shift_length'].astype('float64') > 0) & (syp_df['Shift Name'] != 0)]

syp_df['Coeff 1 Units'] = syp_df['Units']
syp_df['Coeff 2 Units'] = 0
syp_df['Coeff 3 Units'] = 0
syp_df['Coeff 4 Units'] = 0
syp_df['Coeff 5 Units'] = 0
syp_df['Coeff 6 Units'] = 0

syp_df.loc[syp_df['Y-Int Def'] == 'CPH', 'Day %'] = syp_df['Units'].astype('int64') / ((syp_df['Y-Int'].astype('float64') +
(syp_df['Coeff 1 Units'].astype('float64') * syp_df['Coeff 1'].astype('float64')) +
(syp_df['Coeff 2 Units'].astype('float64') * syp_df['Coeff 2'].astype('float64')) +
(syp_df['Coeff 3 Units'].astype('float64') * syp_df['Coeff 3'].astype('float64')) +
(syp_df['Coeff 4 Units'].astype('float64') * syp_df['Coeff 4'].astype('float64')) +
(syp_df['Coeff 5 Units'].astype('float64') * syp_df['Coeff 5'].astype('float64')) +
(syp_df['Coeff 6 Units'].astype('float64') * syp_df['Coeff 6'].astype('float64'))
) * syp_df['adjusted_shift_length'])

syp_df.loc[syp_df['Y-Int Def'] == 'SPC', 'Day %'] = syp_df['Units'].astype('int64') / (( 3600 / (syp_df['Y-Int'].astype('float64') +
(syp_df['Coeff 1 Units'].astype('float64') * syp_df['Coeff 1'].astype('float64')) +
(syp_df['Coeff 2 Units'].astype('float64') * syp_df['Coeff 2'].astype('float64')) +
(syp_df['Coeff 3 Units'].astype('float64') * syp_df['Coeff 3'].astype('float64')) +
(syp_df['Coeff 4 Units'].astype('float64') * syp_df['Coeff 4'].astype('float64')) +
(syp_df['Coeff 5 Units'].astype('float64') * syp_df['Coeff 5'].astype('float64')) +
(syp_df['Coeff 6 Units'].astype('float64') * syp_df['Coeff 6'].astype('float64'))
)) * syp_df['adjusted_shift_length'])

##Create final dataframe
syp_df.drop('Puncher', axis=1, inplace=True)
syp_df.rename(columns={'Preferred Name':'Puncher','Task_y':'Task', 'Subtask_y':'Subtask'}, inplace=True)

syp_df = syp_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###SQ Error Resolution Module
##Build SQ error dataframe
sq_error_res_df = nuway_df.copy()
sq_error_res_df = sq_error_res_df.loc[sq_error_res_df['Task'] == 'sq error resolution']

sq_err_df = pvp_sql_df.copy()

sq_err_df['QUEUE_NUMBER'] = sq_err_df['QUEUE_NUMBER'].str.lower()

##Find dupes
sq_error_res_df["Dupe"] = sq_error_res_df['Punch'].astype(str) + sq_error_res_df['Puncher'].astype(str) + sq_error_res_df['Subtask'].astype(str)

sq_error_res_dupe_df = sq_error_res_df.copy()
sq_error_res_dupe_df = sq_error_res_dupe_df[sq_error_res_dupe_df.duplicated(subset=['Dupe'], keep=False)]
sq_error_res_dupe_df.drop_duplicates(subset=['Dupe'], inplace=True)
sq_error_res_dupe_df.loc[sq_error_res_dupe_df['Dupe'] != '', 'Dupe'] = "Error"
sq_error_res_dupe_df = sq_error_res_dupe_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, sq_error_res_dupe_df])

##Merge remaining data with SQ data and account for POQs
sq_err_df = pd.merge(sq_err_df, sq_error_res_df, how='right', on='QUEUE_NUMBER')

sq_err_df.loc[sq_err_df['QUEUE_NUMBER'].str[-3:] == 'poq', 'SHIPPINGQUEUENUMBER'] = sq_err_df['QUEUE_NUMBER']

##Find punches with bad sq number data
sq_err_sq_error_df = sq_err_df.copy()
sq_err_sq_error_df = sq_err_sq_error_df[sq_err_sq_error_df['SHIPPINGQUEUENUMBER'].isna()]
sq_err_sq_error_df['SHIPPINGQUEUENUMBER'] = sq_err_sq_error_df['SHIPPINGQUEUENUMBER'].fillna("Error")
sq_err_sq_error_df.drop('Subtask', axis=1, inplace=True)
sq_err_sq_error_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
sq_err_sq_error_df = sq_err_sq_error_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, sq_err_sq_error_df])

##Find non Roca SQs
sq_err_df["sq_type"] = sq_err_df['SHIPPINGQUEUENUMBER'].str[-3:]
sq_err_df['SHIPPINGQUEUENUMBER'] = sq_err_df['SHIPPINGQUEUENUMBER'].astype(str)
sq_err_df.loc[(sq_err_df['SHIPPINGQUEUENUMBER'].map(len) == 16) & (sq_err_df['SHIPPINGQUEUENUMBER'].str[-3:] != 'poq'), 'sq_type'] = sq_err_df['SHIPPINGQUEUENUMBER'].str[-6:]

sq_err_sq_type_df = sq_err_df.copy()
sq_err_sq_type_df = sq_err_sq_type_df[sq_err_sq_type_df['sq_type'].str[:1] != 'r']
sq_err_sq_type_df['SHIPPINGQUEUENUMBER'] = "Error"
sq_err_sq_type_df.drop('Subtask', axis=1, inplace=True)
sq_err_sq_type_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
sq_err_sq_type_df = sq_err_sq_type_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, sq_err_sq_type_df])

##Find SQs without verify status
sq_err_verify_df = sq_err_df.copy()
sq_err_verify_df = sq_err_verify_df[sq_err_verify_df['Units'] == '']
sq_err_verify_df['Units'] = "Error"
sq_err_verify_df.drop('Subtask', axis=1, inplace=True)
sq_err_verify_df.rename(columns={'SHIPPINGQUEUENUMBER':'Subtask'}, inplace=True)
sq_err_verify_df = sq_err_verify_df[['Punch', 'Puncher', 'Task', 'Subtask', 'Units', 'Orders Completed', 'Flex Run', 'Env Run']]

error_df = pd.concat([error_df, sq_err_verify_df])

##Remove errors from dataframe
sq_err_df.dropna(subset=["SHIPPINGQUEUENUMBER"], inplace=True)
sq_err_df = sq_err_df[sq_err_df['sq_type'].str[:1] == 'r']
sq_err_df = sq_err_df[sq_err_df['Units'] != '']

##Rename columns
sq_err_df.rename(columns={'Units':'Full Verify?','sq_card_quantity':'Units'}, inplace=True)

##Normalize punches
sq_err_df["normal_punch"] = pd.to_datetime(sq_err_df['Punch']).dt.date

##Merge staffing data
sq_err_df['Puncher'] = sq_err_df['Puncher'].str.lower()
sq_err_df = pd.merge(sq_err_df, staffing_df, how='left', on='Puncher')

##Include current team members
sq_err_df = sq_err_df.loc[sq_err_df['Shift Length'].apply(pd.to_numeric, errors='coerce') > 0]

##Merge shift data
sq_err_df["keep"] = None
sq_err_df = pd.merge(sq_err_df, shiftdata_df, how='left', on='Puncher')

sq_err_df.loc[(sq_err_df['Punch'] >= sq_err_df['minimum']) & (sq_err_df['Punch'] <= sq_err_df['maximum']), 'keep'] = 1

sq_err_df = sq_err_df.loc[sq_err_df['keep'] == 1]

##First Offset
sq_err_df.rename(columns={'Date':'first_offset'}, inplace=True)
sq_err_df['first_offset'] = pd.to_datetime(sq_err_df['first_offset'], format='%m-%d-%Y').dt.date

##Build SQ error res standards dataframe
sq_error_res_standards_df = standards_df.copy()
sq_error_res_standards_df = sq_error_res_standards_df.loc[sq_error_res_standards_df['Task'] == 'SQ Error Resolution']

##Merge Standards with Dataframe
sq_error_res_standards_df['Subtask'] = sq_error_res_standards_df['Subtask'].str.lower()
sq_err_df['sq_type'] = sq_err_df['sq_type'].str.lower()

sq_err_df.drop('Subtask', axis=1, inplace=True)
sq_err_df.rename(columns={'sq_type':'Subtask'}, inplace=True)

sq_err_df = pd.merge(sq_err_df, sq_error_res_standards_df, how='left', on='Subtask')

##Calculate Metrics
sq_err_df = sq_err_df.loc[(sq_err_df['adjusted_shift_length'].astype('float64') > 0) & (sq_err_df['Shift Name'] != 0)]

sq_err_df['Coeff 1 Units'] = sq_err_df['Units']
sq_err_df['Coeff 2 Units'] = 0
sq_err_df['Coeff 3 Units'] = 0
sq_err_df['Coeff 4 Units'] = 0
sq_err_df['Coeff 5 Units'] = 0
sq_err_df['Coeff 6 Units'] = 0

sq_err_df.loc[sq_err_df['Y-Int Def'] == 'CPH', 'Day %'] = sq_err_df['Units'].astype('int64') / ((sq_err_df['Y-Int'].astype('float64') +
(sq_err_df['Coeff 1 Units'].astype('float64') * sq_err_df['Coeff 1'].astype('float64')) +
(sq_err_df['Coeff 2 Units'].astype('float64') * sq_err_df['Coeff 2'].astype('float64')) +
(sq_err_df['Coeff 3 Units'].astype('float64') * sq_err_df['Coeff 3'].astype('float64')) +
(sq_err_df['Coeff 4 Units'].astype('float64') * sq_err_df['Coeff 4'].astype('float64')) +
(sq_err_df['Coeff 5 Units'].astype('float64') * sq_err_df['Coeff 5'].astype('float64')) +
(sq_err_df['Coeff 6 Units'].astype('float64') * sq_err_df['Coeff 6'].astype('float64'))
) * sq_err_df['adjusted_shift_length'].astype('float64'))

sq_err_df.loc[sq_err_df['Y-Int Def'] == 'SPC', 'Day %'] = sq_err_df['Units'].astype('int64') / (( 3600 / (sq_err_df['Y-Int'].astype('float64') +
(sq_err_df['Coeff 1 Units'].astype('float64') * sq_err_df['Coeff 1'].astype('float64')) +
(sq_err_df['Coeff 2 Units'].astype('float64') * sq_err_df['Coeff 2'].astype('float64')) +
(sq_err_df['Coeff 3 Units'].astype('float64') * sq_err_df['Coeff 3'].astype('float64')) +
(sq_err_df['Coeff 4 Units'].astype('float64') * sq_err_df['Coeff 4'].astype('float64')) +
(sq_err_df['Coeff 5 Units'].astype('float64') * sq_err_df['Coeff 5'].astype('float64')) +
(sq_err_df['Coeff 6 Units'].astype('float64') * sq_err_df['Coeff 6'].astype('float64'))
)) * sq_err_df['adjusted_shift_length'].astype('float64'))

##Create final dataframe
sq_err_df.drop(['Puncher', 'Subtask'], axis=1, inplace=True)
sq_err_df.rename(columns={'Preferred Name':'Puncher', 'SHIPPINGQUEUENUMBER':'Subtask', 'Task_y':'Task'}, inplace=True)
sq_err_df['Task'] = "SQ Error Resolution"

sq_err_df = sq_err_df[['Punch','first_offset','Puncher','Units','Task','Subtask','adjusted_shift_length','Regular Hours','Day %','Shift Name','Test','Notes']]

###Paperless Pulling
##Connect staffing data
paperless_df['PUNCHER'] = paperless_df['PUNCHER'].str.lower()
paperless_df.rename(columns={'PUNCHER':'Puncher', 'PUNCH':'Punch', 'DENSITY_PULLED':'Density'}, inplace=True)
paperless_df['Density'] = paperless_df['Density'].astype('float64')
paperless_df = pd.merge(paperless_df, staffing_df, how='left')

paperless_df.dropna(subset=['Shift Name'], inplace=True)

##Prepare frame for merge
paperless_df["Task"] = "Paperless Pull"

##Remove duplicates
paperless_df["combined"] = paperless_df['Puncher'].astype(str) + "~" + paperless_df['SQ'].astype(str) + "~" + paperless_df['CARDS_PULLED'].astype(str)

paperless_dupes_df = paperless_df[paperless_df.duplicated(subset=['combined'], keep=False)]

paperless_df.drop_duplicates(subset=['combined'], inplace=True)
paperless_df.drop('combined', axis=1, inplace=True)

##Connect staffing data
paperless_df.rename(columns={'CARDS_PULLED':'Units'}, inplace=True)

paperless_df['Puncher'] = paperless_df['Puncher'].str.lower()
paperless_df = pd.merge(paperless_df, staffing_df, how='left')

paperless_df.dropna(subset=['Shift Name'], inplace=True)

##Merge shift data
paperless_df["keep"] = None
paperless_df = pd.merge(paperless_df, shiftdata_df, how='left', on='Puncher')

paperless_df.loc[(paperless_df['Punch'] >= paperless_df['minimum']) & (paperless_df['Punch'] <= paperless_df['maximum']), 'keep'] = 1

paperless_df = paperless_df.loc[paperless_df['keep'] == 1]

##First Offset
paperless_df.rename(columns={'Date':'first_offset'}, inplace=True)
paperless_df['first_offset'] = pd.to_datetime(paperless_df['first_offset'], format='%m-%d-%Y').dt.date

##Define Pprless pull standards
pprless_pull_standards_df = standards_df.loc[standards_df.Task == 'Paperless Pull']

##SQ Type
paperless_df["sq_type"] = paperless_df['SQ'].str[-3:]
paperless_df.loc[(paperless_df['SQ'].map(len) == 16) & (paperless_df['SQ'].str[-3:] != 'poq'), 'sq_type'] = paperless_df['SQ'].str[-6:]

##Merge standard with dataframe
paperless_df = pd.merge(paperless_df, pprless_pull_standards_df, left_on = 'sq_type', right_on = 'Subtask')

##Calculate metrics
paperless_df["paperless_day_%"] = ""

paperless_df.loc[paperless_df['Y-Int Def'] == 'SPC', 'paperless_day_%'] = (paperless_df['Units'].astype('float64')) / ((3600 / (paperless_df['Y-Int'].astype('float64') + (paperless_df['Density'].astype('float64') * paperless_df['Coeff 1'].astype('float64')))) * paperless_df['adjusted_shift_length'].astype('float64'))

paperless_df.loc[paperless_df['Y-Int Def'] == 'CPH', 'paperless_day_%'] = (paperless_df['Units'].astype('float64')) / ((paperless_df['Y-Int'].astype('float64') + (paperless_df['Density'].astype('float64') * paperless_df['Coeff 1'].astype('float64'))) * paperless_df['adjusted_shift_length'].astype('float64'))

##Write final dataframe
paperless_df["Task"] = "Paperless Pull"
paperless_df["Test"] = ""
paperless_df["Notes"] = ""

paperless_df = paperless_df[['Punch','first_offset','Preferred Name','Units','SQ','paperless_day_%','Task','adjusted_shift_length','Regular Hours','Shift Name']]

paperless_df.rename(columns={'Preferred Name':'Puncher','SQ':'Subtask','paperless_day_%':'Day %'},inplace=True)

###Final tweaks
##Combine Data Frames
data_df = pd.DataFrame()

data_df = pd.concat([pvp_df, pvp_sco_df])

data_df = pd.concat([data_df, pullver_df])

data_df = pd.concat([data_df, pull_df])

data_df = pd.concat([data_df, gen_df])

data_df = pd.concat([data_df, filing_df])

data_df = pd.concat([data_df, sort_df])

data_df = pd.concat([data_df, syp_df])

data_df = pd.concat([data_df, sq_err_df])

data_df = pd.concat([data_df, rec_final_df])

data_df = pd.concat([data_df, blo_final_df])

data_df = pd.concat([data_df, paperless_df])

##Aggragate Day %'s
data_df["combined"] = data_df['first_offset'].astype(str) + data_df['Puncher'].astype(str)

day_per_agg = data_df.groupby('combined')['Day %'].sum()
data_df = pd.merge(data_df, day_per_agg, how='right', on='combined')

data_df.rename(columns={'Day %_y':'Total_Day_%', 'Day %_x':'Day %'}, inplace=True)

data_df['Total_Day_%'] = data_df['Total_Day_%'] + 0.0833

data_df["Total_Earned_Hours"] = ((data_df['Regular Hours'].astype('float64'))/24) * data_df['Total_Day_%']
data_df["Earned_Hours"] = ((data_df['adjusted_shift_length'])/24) * data_df['Day %']

data_df['Punch'] = data_df['Punch'].apply(pd.to_datetime, errors='coerce')

data_df.sort_values(by=['Puncher', 'Punch'], ascending=[True,False], inplace=True)

##Only include last 90 days
data_df['first_offset'] = pd.to_datetime(data_df['first_offset'], format='%m-%d-%Y').dt.date

data_df["now"] = pd.Timestamp.now()
data_df['now'] = data_df['now'].dt.date

data_df = data_df.loc[data_df['first_offset'] >= (data_df['now'] - timedelta(days = 90))]

##Make pfep and parsed data
data_df['Test'] = data_df['Test'].fillna('')
data_df['Notes'] = data_df['Notes'].fillna('')

parsed_data_df = data_df.copy()

pfep_df = data_df.copy()

pfep_df["combined"] = pfep_df['first_offset'].astype(str) + pfep_df['Puncher'].astype(str)

pfep_df.drop_duplicates(subset=['combined'], inplace=True)

pfep_df = pfep_df[['first_offset', 'Puncher', 'Total_Earned_Hours', 'Regular Hours']]

data_df['Data'] = data_df['Punch'].astype(str) + "|" + data_df['first_offset'].astype(str) + "|" + data_df['Puncher'].astype(str) + "|" + data_df['Units'].astype(str) + "|" + data_df['Subtask'].astype(str) + "|" + data_df['Day %'].astype(str) + "|" + data_df['Earned_Hours'].astype(str) + "|" + data_df['Task'].astype(str) + "|" + data_df['adjusted_shift_length'].astype(str) + "|" + data_df['Total_Day_%'].astype(str) + "|" + data_df['Total_Earned_Hours'].astype(str) + "|" + data_df['Shift Name'].astype(str) + "|" + data_df['Regular Hours'].astype(str) + "|" + "" + "|" + data_df['Orders'].astype(str) + "|" + data_df['Test'].astype(str) + "|" + data_df['Notes'].astype(str)

data_df = data_df[['Data']]

error_df.loc[error_df['Puncher'] == 'Error', 'Punch'] = None
error_df.dropna(subset=["Punch"], inplace=True)

##Parse down to individual + day totals
parsed_data_df.drop_duplicates(subset=['combined'], inplace=True)

parsed_data_df = parsed_data_df[['first_offset','Puncher','Regular Hours','Total_Day_%','Total_Earned_Hours','Shift Name']]

parsed_data_df.rename(columns={
'first_offset':'Date',
'Puncher': 'Team Member',
'Regular Hours': 'Hours Worked',
'Total_Day_%': 'Total Day %',
'Total_Earned_Hours': 'Total Earned Hours',
'Shift Name': 'Pillar'}, inplace=True)

##Create csvs
data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
data_result = separator.join(data_string)
data_df.to_csv(data_result, index=False)

error_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Error.csv"]
error_result = separator.join(error_string)
error_df.to_csv(error_result, index=False)

pfep_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "PFEP.csv"]
pfep_result = separator.join(pfep_string)
pfep_df.to_csv(pfep_result, index=False)

parsed_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "ParsedData.csv"]
parsed_data_result = separator.join(parsed_data_string)
parsed_data_df.to_csv(parsed_data_result, index=False)

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