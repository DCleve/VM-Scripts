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

archive = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE')
importTab = gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE').worksheet('Import')

##Let sort doc know data is being pulled
#sortDataTab = gc.open_by_key('1cXFsju_uztNtYDTSt30b3wVqjUGt1Qr6O1mMya4H3cM').worksheet('Data')
#sortDataTab.update('C2', 'TRUE')

##Open workflow doc and get keys of nuways to pull data from
keys = gc.open_by_key('1U38UjtKRdtgvjCvLEgceZtzdCDSjYiZOAIzEbVovZa4').worksheet('ToPull')
keys_df = pd.DataFrame.from_dict(keys.get_all_records())
keys_df[keys_df.isna()] = 0

for i in range(len(keys_df)):
    google_key = keys_df.iloc[i, 0]

    if google_key != '':
        nuway_data_tab = gc.open_by_key(google_key).worksheet('Data')
        nuway_data_tab_data = pd.DataFrame(nuway_data_tab.get_all_values())
        nuway_data_tab_data.columns = nuway_data_tab_data.iloc[0]
        nuway_data_tab_data = nuway_data_tab_data[1:]
        nuway_data_tab_df = pd.DataFrame(nuway_data_tab_data)
        nuway_data_tab_df = nuway_data_tab_df[['Enter a # Please']]

        if nuway_data_tab_df.iloc[0,0] != 0:
            nuway_data_tab_df_data = nuway_data_tab_df[1:]

            nuway_data = nuway_data_tab_df_data.values.tolist()
            importTab.append_rows(nuway_data)
            nuway_data_tab.batch_clear(['F3:F'])

#           if google_key == '1cXFsju_uztNtYDTSt30b3wVqjUGt1Qr6O1mMya4H3cM':
#               punch_a_tab = gc.open_by_key(google_key).worksheet('PunchA')
#               punch_a_tab.batch_clear(['A2:L'])

        time.sleep(5)

#sortDataTab.update('C2', '')

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