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

IST = pytz.timezone('America/New_York')
now = datetime.now(IST)
dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
dt_string = pd.to_datetime(dt_string)
hour = dt_string.hour

##Determine which pillar to update
pillar = 'None'

if (hour == 1) or (hour == 9) or (hour == 17):
    pillar = "Shipping Lead"

elif (hour == 2) or (hour == 10) or (hour == 18):
    pillar = "Receiving Supe"

elif (hour == 3) or (hour == 11) or (hour == 19):
    pillar = "Receiving Lead"

elif (hour == 4) or (hour == 12) or (hour == 20):
    pillar = "Shipping Supe"

elif (hour == 5) or (hour == 13) or (hour == 21):
    pillar = "Overnight Supe"

elif (hour == 6) or (hour == 14) or (hour == 22):
    pillar = "Overnight Lead"

elif (hour == 7) or (hour == 15) or (hour == 23):
    pillar = "Operations"

elif (hour == 8) or (hour == 16) or (hour == 0):
    pillar = "Training"

if pillar == "None":
    exit()

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Length','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role']]
staffing_df.rename(columns={'Email':'Puncher'}, inplace=True)

##Import scorecard data
url_df = gd.get_as_dataframe(gc.open_by_key('1AtWbOiHUmWRUoNmWaPxSfj1qMYmRBHeUXf87gvXSF6s').worksheet('Scorecards'))
url_df.dropna(subset=["Google Key"], inplace=True)
url_df = url_df[['Google Key', 'User', 'Subgroup', 'Email']]

dataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
dataTab_result = separator.join(dataTab_string)
dataTab_df = pd.read_csv(dataTab_result)

errorTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Error.csv"]
errorTab_result = separator.join(errorTab_string)
errorTab_df = pd.read_csv(errorTab_result)

testDataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvData.csv"]
testDataTab_result = separator.join(testDataTab_string)
testDataTab_df = pd.read_csv(testDataTab_result)

pfep_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "PFEP.csv"]
pfep_result = separator.join(pfep_string)
pfep_df = pd.read_csv(pfep_result)

if pillar == 'Operations':
    print("PFEP Script")

    pfepDataTab = gc.open_by_key('1l6w9TYvCFdyKmpCg5jK4XSKZLWtuIilDjD0CO6s_5kY').worksheet('NewData')
    pfepPersonnelTab = gc.open_by_key('1l6w9TYvCFdyKmpCg5jK4XSKZLWtuIilDjD0CO6s_5kY').worksheet('Personnel')

    pfepDataTab.batch_clear(['A1:D'])
    gd.set_with_dataframe(pfepDataTab, pfep_df, row=1, col=1)

    pfepPersonnelTab.clear()
    gd.set_with_dataframe(pfepPersonnelTab, staffing_df)

s = len(url_df);
i = 0;

while i < s:
    subgroup = url_df.iloc[i, 2]

    if subgroup == pillar:
        url = url_df.iloc[i, 0]

        print(url)

        dataTab = gc.open_by_key(url).worksheet('Data')


        cell_list = dataTab.range('A1')
        for cell in cell_list:
            cell.value = 'Off'
        dataTab.update_cells(cell_list, value_input_option='USER_ENTERED')

        dataTab.batch_clear(['A2:A'])
        gd.set_with_dataframe(dataTab, dataTab_df, row=2, col=1)

        time.sleep(5)

        testDataTab = gc.open_by_key(url).worksheet('TestData')

        test_cell_list = testDataTab.range('A1')

        for cell in test_cell_list:
            cell.value = 'Off'

        testDataTab.update_cells(test_cell_list, value_input_option='USER_ENTERED')

        testDataTab.batch_clear(['A2:A'])
        gd.set_with_dataframe(testDataTab, testDataTab_df, row=2, col=1)

        time.sleep(5)

        staffingtab = gc.open_by_key(url).worksheet('Personnel')
        staffingtab.clear()
        gd.set_with_dataframe(staffingtab, staffing_df)

        time.sleep(5)

        errorstab = gc.open_by_key(url).worksheet('Errors')
        errorstab.clear()
        gd.set_with_dataframe(errorstab, errorTab_df)

        cell_list = dataTab.range('A1')
        for cell in cell_list:
            cell.value = 'On'
        dataTab.update_cells(cell_list, value_input_option='USER_ENTERED')

        test_cell_list = testDataTab.range('A1')

        for cell in test_cell_list:
            cell.value = 'On'

        testDataTab.update_cells(test_cell_list, value_input_option='USER_ENTERED')

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