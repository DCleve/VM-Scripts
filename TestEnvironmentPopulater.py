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

dataTab = gc.open_by_key('13akI3e2UiKYHxvT0Sy8WilXj0JPJ7ZXvhgkVhQa8oB0').worksheet('Data')
parsedDataTab = gc.open_by_key('13akI3e2UiKYHxvT0Sy8WilXj0JPJ7ZXvhgkVhQa8oB0').worksheet('ParsedData')
currParsedDataTab = gc.open_by_key('13akI3e2UiKYHxvT0Sy8WilXj0JPJ7ZXvhgkVhQa8oB0').worksheet('CurrentParsedData')
currDataTab = gc.open_by_key('13akI3e2UiKYHxvT0Sy8WilXj0JPJ7ZXvhgkVhQa8oB0').worksheet('CurrentData')
errorTab = gc.open_by_key('13akI3e2UiKYHxvT0Sy8WilXj0JPJ7ZXvhgkVhQa8oB0').worksheet('Errors')

##Import and parse data csvs
data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvData.csv"]
data_result = separator.join(data_string)
data_df = pd.read_csv(data_result)

parsed_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvParsedData.csv"]
parsed_data_result = separator.join(parsed_data_string)
parsed_data_df = pd.read_csv(parsed_data_result)

curr_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
curr_data_result = separator.join(curr_data_string)
curr_data_df = pd.read_csv(curr_data_result)

curr_parsed_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "ParsedData.csv"]
curr_parsed_data_result = separator.join(curr_parsed_data_string)
curr_parsed_data_df = pd.read_csv(curr_parsed_data_result)

errorTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvError.csv"]
errorTab_result = separator.join(errorTab_string)
errorTab_df = pd.read_csv(errorTab_result)

##Write data to sheets
parsedDataTab.clear()
gd.set_with_dataframe(parsedDataTab, parsed_data_df)

currParsedDataTab.clear()
gd.set_with_dataframe(currParsedDataTab, curr_parsed_data_df)

dataTab.batch_clear(['A3:A'])
gd.set_with_dataframe(dataTab, data_df, row=3, col=1)

currDataTab.batch_clear(['A3:A'])
gd.set_with_dataframe(currDataTab, curr_data_df, row=3, col=1)

errorTab.clear()
gd.set_with_dataframe(errorTab, errorTab_df)

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