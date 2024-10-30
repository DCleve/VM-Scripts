import requests
import time
from datetime import datetime
import pytz
import pandas as pd
import os
import gspread
import pandas as pd
import gspread_dataframe as gd

gc=gspread.service_account()

IST = pytz.timezone('America/New_York')

now = datetime.now(IST)
dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
dt_string = pd.to_datetime(dt_string)

currentHour = dt_string.hour
currentMinute = dt_string.minute

##Import schedule
schedule = gc.open_by_key('1wnOGL4EtTB0Jk3YuP46zp5XBLwvWTPSg4R3G32VfkaY').worksheet('ScriptTiming')
schedule_df = pd.DataFrame.from_dict(schedule.get_all_records())
schedule_df.dropna(subset=["Path"], inplace=True)

schedule_df = schedule_df.loc[(schedule_df['Hour'].astype('float64') == currentHour) | (schedule_df['Hour'].astype('float64') == 25) | (schedule_df['Hour'].astype('float64') == 26) | (schedule_df['Hour'].astype('float64') == 27) | (schedule_df['Hour'].astype('float64') == 29)]

for i in range(len(schedule_df)):
    scriptName = schedule_df.iloc[i][1]
    scriptHour = schedule_df.iloc[i][2]
    scriptMinute = schedule_df.iloc[i][3]

    scriptHour = float(scriptHour)
    currentMinute = float(currentMinute)

    if (scriptHour == 25) & (currentMinute <= 15):
        os.system(scriptName)
        time.sleep(15)

    elif (scriptHour == 29) & (currentMinute >= 35):
        os.system(scriptName)
        time.sleep(15)

    elif (scriptHour == currentHour) & (currentMinute > 15) & (currentMinute < 35):
        print(scriptName)
        os.system(scriptName)
        time.sleep(15)

    elif ((scriptHour == 26) | (scriptHour == 27)) & (currentMinute <= 15):
        int_check_2 = divmod(currentHour, 2)
        int_check_6 = divmod(currentHour, 6)

        if (int_check_2[1] == 0) & (scriptHour == 26):
            os.system(scriptName)
            time.sleep(15)

        elif (int_check_6[1] == 0) & (scriptHour == 27):
            os.system(scriptName)
            time.sleep(15)