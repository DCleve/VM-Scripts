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

##Get Requests
requests = gc.open_by_key('1RhJ3ff2rK4ovQz8VAgocRSrvmbqObYSczcG9NlyVa7c').worksheet('RequestList')
requestsData = pd.DataFrame(requests.get_all_values())
requestsData.columns = requestsData.iloc[0]
requestsData = requestsData[1:]
requests_df = pd.DataFrame(requestsData)
requests_df.dropna(subset=["Type"], inplace=True)

##Clear requests tab and insert timestamp
requests.batch_clear(['A2:C'])

retrievals = gc.open_by_key('1RhJ3ff2rK4ovQz8VAgocRSrvmbqObYSczcG9NlyVa7c').worksheet('Retrievals')
retrievalsData = pd.DataFrame(retrievals.get_all_values())
retrievalsData.columns = retrievalsData.iloc[0]
retrievalsData = retrievalsData[1:]
retrievals_df = pd.DataFrame(retrievalsData)
retrievals_df.dropna(subset=["Retrievals"], inplace=True)

dt_string = datetime.now(pytz.timezone('America/New_York')).strftime("%m/%d/%Y %H:%M:%S")

retrievals_list = retrievals_df['Retrievals'].tolist()
retrievals_list.append(dt_string)
retrievals_list_df = pd.DataFrame(list(zip(retrievals_list)), columns=['Retrievals'])

retrievals.clear()
gd.set_with_dataframe(retrievals, retrievals_list_df)

##Scorecard
scorecard_requests_df = requests_df.copy()
scorecard_requests_df = scorecard_requests_df.loc[scorecard_requests_df['Type'] == 'Scorecard']
scorecard_requests_df = scorecard_requests_df[['Url']]
scorecard_url_list = scorecard_requests_df['Url'].tolist()

scorecard_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "NuWay", "Data", "ScorecardPopulaterRequest.py"]
scorecard_result = separator.join(scorecard_string)

if(len(scorecard_url_list) > 0):
    for i in range(len(scorecard_url_list)):
        url = scorecard_url_list[i];
        arg1 = url
        subprocess.run(['python', scorecard_result, arg1])

##48 doc
forty8_requests_df = requests_df.copy()
forty8_requests_df = forty8_requests_df.loc[forty8_requests_df['Type'] == '48 Doc']
forty8_requests_df = forty8_requests_df[['Url']]
forty8_url_list = forty8_requests_df['Url'].tolist()

forty8_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "OperationsDocs", "48DocUpdater.py"]
forty8_result = separator.join(forty8_string)

if(len(forty8_url_list) > 0):
    subprocess.run(['python', forty8_result])

##Filing Audit Data
file_audit_requests_df = requests_df.copy()
file_audit_requests_df = file_audit_requests_df.loc[file_audit_requests_df['Type'] == 'Filing Audit']
file_audit_requests_df = file_audit_requests_df[['Url']]
file_audit_url_list = file_audit_requests_df['Url'].tolist()

file_audit_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "OperationsDocs", "FilingAudit.py"]
file_audit_result = separator.join(file_audit_string)

if(len(file_audit_url_list) > 0):
    subprocess.run(['python', file_audit_result])

##First Leads Organizer
first_organ_requests_df = requests_df.copy()
first_organ_requests_df = first_organ_requests_df.loc[first_organ_requests_df['Type'] == 'First Leads Organizer']
first_organ_requests_df = first_organ_requests_df[['Url']]
first_organ_url_list = first_organ_requests_df['Url'].tolist()

first_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "OperationsDocs", "FirstLeadsOrganizer.py"]
first_result = separator.join(first_string)

if(len(first_organ_url_list) > 0):
    subprocess.run(['python', first_result])

##Gen Task Reporting
gen_task_requests_df = requests_df.copy()
gen_task_requests_df = gen_task_requests_df.loc[gen_task_requests_df['Type'] == 'Gen Task Reporting']
gen_task_requests_df = gen_task_requests_df[['Url']]
gen_task_url_list = gen_task_requests_df['Url'].tolist()

gen_task_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "OperationsDocs", "GenTasksReporting.py"]
gen_task_result = separator.join(gen_task_string)

if(len(gen_task_url_list) > 0):
    subprocess.run(['python', gen_task_result])

##Pull/Ver Accuracy
pv_acc_requests_df = requests_df.copy()
pv_acc_requests_df = pv_acc_requests_df.loc[(pv_acc_requests_df['Type'] == 'Pull Accuracy') | (pv_acc_requests_df['Type'] == 'PullVer Accuracy')]
pv_acc_requests_df = pv_acc_requests_df[['Url']]
pv_acc_url_list = pv_acc_requests_df['Url'].tolist()

pv_acc_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "Quality", "Pull-PullVerAccuracy.py"]
pv_acc_result = separator.join(pv_acc_string)

if(len(pv_acc_url_list) > 0):
    subprocess.run(['python', pv_acc_result])

##PVP Accuracy
pvp_acc_requests_df = requests_df.copy()
pvp_acc_requests_df = pvp_acc_requests_df.loc[pvp_acc_requests_df['Type'] == 'PVP Accuracy']
pvp_acc_requests_df = pvp_acc_requests_df[['Url']]
pvp_acc_url_list = pvp_acc_requests_df['Url'].tolist()

pvp_acc_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "Quality", "PVPAccuracy.py"]
pvp_acc_result = separator.join(pvp_acc_string)

if(len(pvp_acc_url_list) > 0):
    subprocess.run(['python', pvp_acc_result])

##Test Environment
test_env_requests_df = requests_df.copy()
test_env_requests_df = test_env_requests_df.loc[test_env_requests_df['Type'] == 'Test Environment']
test_env_requests_df = test_env_requests_df[['Url']]
test_env_url_list = test_env_requests_df['Url'].tolist()

test_env_data_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "NuWay", "TestEnvironment", "TestEnvironmentData.py"]
test_env_data_result = separator.join(test_env_data_string)

test_env_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "NuWay", "TestEnvironment", "TestEnvironmentPopulater.py"]
test_env_result = separator.join(test_env_string)

if(len(test_env_url_list) > 0):
    subprocess.run(['python', test_env_data_result])
    time.sleep(15)
    subprocess.run(['python', test_env_result])

##VLart List
vlart_requests_df = requests_df.copy()
vlart_requests_df = vlart_requests_df.loc[vlart_requests_df['Type'] == 'VLart List']
vlart_requests_df = vlart_requests_df[['Url']]
vlart_url_list = vlart_requests_df['Url'].tolist()

vlart_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "Snowflake", "VLastList.py"]
vlart_result = separator.join(vlart_string)

if(len(vlart_url_list) > 0):
    subprocess.run(['python', vlart_result])

##Workflow
wrk_flw_requests_df = requests_df.copy()
wrk_flw_requests_df = wrk_flw_requests_df.loc[wrk_flw_requests_df['Type'] == 'Workflow']
wrk_flw_requests_df = wrk_flw_requests_df[['Url']]
wrk_flw_url_list = wrk_flw_requests_df['Url'].tolist()

wrk_flw_string = ["C:", "Users", login, "Desktop", "AC-Scripting", "OperationsDocs", "WorkflowDocData.py"]
wrk_flw_result = separator.join(wrk_flw_string)

if(len(wrk_flw_url_list) > 0):
    subprocess.run(['python', wrk_flw_result])

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