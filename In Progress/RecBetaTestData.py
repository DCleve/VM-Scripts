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

###Receiving
recTab = gc.open_by_key('1Saxvy2hHznB6tZzmVM2iVn6xfQEmcqGHlu4hs8DAjUc').worksheet('Data')

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

rec_df['PROC_TIME_MINUTES'] = rec_df['PROC_TIME_MINUTES'].apply(pd.to_numeric, errors='coerce').fillna(0)

rec_df['VER_TIME_MINUTES'] = rec_df['VER_TIME_MINUTES'].apply(pd.to_numeric, errors='coerce').fillna(0)

##Calculate task speeds
rec_df["proc_cph"] = 0.0
rec_df["proc_spc"] = 0.0

rec_df.loc[rec_df['PROC_TIME_MINUTES'] > 0, 'proc_cph'] = rec_df['NUMBER_OF_CARDS'].astype('float64').apply(pd.to_numeric, errors='coerce').fillna(0) / (rec_df['PROC_TIME_MINUTES'] / 60)


rec_df.loc[rec_df['proc_cph'] > 0, 'proc_spc'] = 3600 / rec_df['proc_cph'].astype('float64')


rec_df["ver_cph"] = 0.0
rec_df["ver_spc"] = 0.0

rec_df.loc[rec_df['VER_TIME_MINUTES'] > 0, 'ver_cph'] = rec_df['NUMBER_OF_CARDS'].astype('float64').apply(pd.to_numeric, errors='coerce').fillna(0) / (rec_df['VER_TIME_MINUTES'] / 60)


rec_df.loc[rec_df['ver_cph'] > 0, 'ver_spc'] = 3600 / rec_df['ver_cph'].astype('float64')

##Write data to sheet
recTab.clear()
gd.set_with_dataframe(recTab, rec_df)