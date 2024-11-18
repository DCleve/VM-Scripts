import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import time
startTime = time.time()
import numpy as np
import requests
import os
separator = '\\'
login = os.getlogin()

startTime = time.time()

gc=gspread.service_account()

###Import NuWay Data
dataTab = gc.open_by_key('1xbP4pPxgIDBiG7U46J_8R7yerpthO3I41aAkdmiXkkI').worksheet('NuWayData')

data_tab_switch = dataTab.range('A2')

for cell in data_tab_switch:
    cell.value = 'Off'

    time.sleep(5)

dataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvData.csv"]
dataTab_result = separator.join(dataTab_string)
dataTab_df = pd.read_csv(dataTab_result)

dataTab.batch_clear(['A2:A'])
gd.set_with_dataframe(dataTab, dataTab_df, row=3, col=1)

for cell in data_tab_switch:
    cell.value = 'On'

    time.sleep(5)

###Receiving
recTab = gc.open_by_key('1YURRAC6Est9IluBye_6N7R6U94tixEUE0f2d0zyzQ60').worksheet('RecData')

##Import Receiving Data
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

##Write data to sheet
recDataTab = gc.open_by_key('1xbP4pPxgIDBiG7U46J_8R7yerpthO3I41aAkdmiXkkI').worksheet('RecData')
recDataTab.clear()
gd.set_with_dataframe(recDataTab, rec_df)