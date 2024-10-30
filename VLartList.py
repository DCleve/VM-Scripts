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

import snowflake.connector
from snowflake.connector import connect
snowflake_pull = connect(user='Dave', password='Quantum314!', account='fva14998.us-east-1')

vlart_raw=pd.DataFrame()
error_sheet_raw=pd.DataFrame()

##SQL
sql = ("""
select
    di.product_condition_id as pcid
    , di.product_id as product_id
    , di.product_line as game
    , di.product_name as card_name
    , di.set_name as set_name
    , di.is_foil as finish

from
    "ANALYTICS"."CORE"."DIRECT_INVENTORY" as di
        inner join
            "HVR_TCGSTORE_PRODUCTION"."PDT"."PRODUCTCONDITION" as pc
                on pc.productconditionid = di.product_condition_id

where
    di.product_line = 'Magic'
    and pc.usecondition = 'TRUE'
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql)

results = cursor.fetch_pandas_all()

vlart_df = pd.concat([vlart_raw, results])

vlart_df.drop(vlart_df.filter(like='Unnamed'), axis=1, inplace=True)
vlart_df.dropna(subset=["PCID"], inplace=True)

##Historical Run Data
hist_data = gc.open_by_key('1ei7gnHAuHr94XNwz4M1vPEnOqSy98ChnLwVuRy5fGk8')
hist_data_tab = hist_data.worksheet('ProductTotals')
hist_data_df = gd.get_as_dataframe(hist_data_tab)

hist_data_df["Combined"] = hist_data_df['Product'].astype(str) + hist_data_df['Set Name'].astype(str) + hist_data_df['Card Name'].astype(str)

hist_agg = hist_data_df.groupby('Combined')['Quantity'].sum()
hist_data_df = pd.merge(hist_data_df, hist_agg, how='right', on='Combined')

vlart_df = pd.merge(hist_data_df, vlart_df, how='right', on='PCID')

vlart_df = vlart_df[['PCID', 'PRODUCT_ID', 'GAME', 'CARD_NAME', 'SET_NAME', 'FINISH', 'Quantity_y']]
vlart_df.rename(columns={'Quantity_y':'Run_Quantity'}, inplace=True)

##Import error data
error_archive = gc.open_by_key('1w3YC-JIwH8DjkP6Z1GurvucY1AvC1J4oNnT7DiidJWw')
error_archive_tab = error_archive.worksheet('ETArchives')
error_archive_df = gd.get_as_dataframe(error_archive_tab)

error_archive_df.dropna(subset=["URL"], inplace=True)

error_archives = error_archive_df[['URL']]

archive1 = pd.DataFrame()
archive2 = pd.DataFrame()

for i in range (len(error_archives)):
    url = error_archives.iloc[i, 0]
    error_archive_open = gc.open_by_url(url)
    error_tab = error_archive_open.worksheet('ErrorTracker')
    error_tab_df = gd.get_as_dataframe(error_tab)

    if i == 0:
        archive1 = pd.concat([archive1, error_tab_df.iloc[:, 3:16]])
    elif i != 0:
        archive2 = pd.concat([archive2, error_tab_df.iloc[:, 4:17]])
    time.sleep(10)

error_sheet_df = pd.concat([error_sheet_raw, archive1])
error_sheet_df = pd.concat([error_sheet_df, archive2])
error_sheet_df.drop(error_sheet_df.filter(like='Unnamed'), axis=1, inplace=True)

error_sheet_df.loc[error_sheet_df['Discrepancy Reason'] != 'Wrong', 'Quantity'] = None

error_sheet_df.dropna(subset=["Quantity"], inplace=True)

##Clean error data
error_sheet_df['Quantity'] = error_sheet_df['Quantity'].apply(pd.to_numeric, errors='coerce')
error_sheet_df['PCID.1'] = error_sheet_df['PCID.1'].apply(pd.to_numeric, errors='coerce')

error_sheet_df = error_sheet_df[error_sheet_df['Quantity'].notnull()]
error_sheet_df = error_sheet_df[error_sheet_df['PCID.1'].notnull()]

error_sheet_df.loc[error_sheet_df['PCID.1'] == 0, 'PCID.1'] = None
error_sheet_df.dropna(subset=["PCID.1"], inplace=True)

error_sheet_df['Quantity'] = pd.to_numeric(error_sheet_df['Quantity'])

total_errors_agg = error_sheet_df.groupby('PCID.1')['Quantity'].sum()
error_sheet_df = pd.merge(error_sheet_df, total_errors_agg, how='right', on='PCID.1')
error_sheet_df = error_sheet_df.drop_duplicates(subset=["PCID.1"])

error_sheet_df = error_sheet_df[['PCID.1', 'Condition.1', 'Set Name.1', 'Card Name.1', 'Quantity_y']]
error_sheet_df.rename(columns={'PCID.1': 'PCID','Condition.1': 'Condition','Set Name.1': 'Set Name','Card Name.1': 'Card Name','Quantity_y': 'Error_Quantity'}, inplace=True)

##Merge with vlart list
vlart_df = pd.merge(vlart_df, error_sheet_df, how='left', on='PCID')

##Split foils and non-foils and aggregate
vlart_foils_df = vlart_df.copy()
vlart_foils_df.loc[vlart_foils_df.FINISH != True, 'PCID'] = None
vlart_foils_df.dropna(subset=['PCID'], inplace=True)
vlart_foils_df["Finish"] = ""
vlart_foils_df.loc[vlart_foils_df.PCID != '', 'Finish'] = "Foil"

foil_errors_agg=vlart_foils_df.groupby('PRODUCT_ID')['Error_Quantity'].sum()
vlart_foils_df = pd.merge(vlart_foils_df, foil_errors_agg, how='right', on='PRODUCT_ID')
vlart_foils_df = vlart_foils_df.drop_duplicates(subset=["PRODUCT_ID"])

vlart_foils_df = vlart_foils_df[['PRODUCT_ID', 'GAME', 'CARD_NAME', 'SET_NAME', 'Finish', 'Run_Quantity', 'Error_Quantity_y']]

vlart_normal_df = vlart_df.copy()
vlart_normal_df.loc[vlart_normal_df.FINISH != False, 'PCID'] = None
vlart_normal_df.dropna(subset=['PCID'], inplace=True)
vlart_normal_df["Finish"] = ""
vlart_normal_df.loc[vlart_normal_df.PCID != '', 'Finish'] = "Normal"


normal_errors_agg=vlart_normal_df.groupby('PRODUCT_ID')['Error_Quantity'].sum()
vlart_normal_df = pd.merge(vlart_normal_df, normal_errors_agg, how='right', on='PRODUCT_ID')
vlart_normal_df = vlart_normal_df.drop_duplicates(subset=["PRODUCT_ID"])

vlart_normal_df = vlart_normal_df[['PRODUCT_ID', 'GAME', 'CARD_NAME', 'SET_NAME', 'Finish', 'Run_Quantity', 'Error_Quantity_y']]

##Calculate accuracy
vlart_foils_df["accuracy"] = ""
vlart_foils_df=vlart_foils_df.fillna(0)
vlart_foils_df.loc[vlart_foils_df.Run_Quantity != 0, 'accuracy'] = vlart_foils_df['Error_Quantity_y'] / vlart_foils_df['Run_Quantity']

vlart_normal_df["accuracy"] = ""
vlart_normal_df=vlart_normal_df.fillna(0)
vlart_normal_df.loc[vlart_normal_df.Run_Quantity != 0, 'accuracy'] = vlart_normal_df['Error_Quantity_y'] / vlart_normal_df['Run_Quantity']

##Calculate min Rc and min Rs
vlart_foils_df["min_Rc"] = ""
vlart_foils_df["min_Rs"] = ""
vlart_foils_df['Run_Quantity'] = pd.to_numeric(vlart_foils_df['Run_Quantity'])
vlart_foils_df['accuracy'] = pd.to_numeric(vlart_foils_df['accuracy'])

vlart_foils_df.loc[(vlart_foils_df.Run_Quantity >= 50) & (vlart_foils_df.accuracy <= 0.01), 'min_Rc'] = 0
vlart_foils_df.loc[(vlart_foils_df.Run_Quantity >= 50) & (vlart_foils_df.accuracy <= 0.01), 'min_Rs'] = 0
vlart_foils_df.loc[(vlart_foils_df.Run_Quantity < 50) | (vlart_foils_df.accuracy > 0.01), 'min_Rc'] = 1
vlart_foils_df.loc[(vlart_foils_df.Run_Quantity < 50) | (vlart_foils_df.accuracy > 0.01), 'min_Rs'] = 1

vlart_normal_df["min_Rc"] = ""
vlart_normal_df["min_Rs"] = ""
vlart_normal_df['Run_Quantity'] = pd.to_numeric(vlart_normal_df['Run_Quantity'])
vlart_normal_df['accuracy'] = pd.to_numeric(vlart_normal_df['accuracy'])

vlart_normal_df.loc[(vlart_normal_df.Run_Quantity >= 50) & (vlart_normal_df.accuracy <= 0.01), 'min_Rc'] = 0
vlart_normal_df.loc[(vlart_normal_df.Run_Quantity >= 50) & (vlart_normal_df.accuracy <= 0.01), 'min_Rs'] = 0
vlart_normal_df.loc[(vlart_normal_df.Run_Quantity < 50) | (vlart_normal_df.accuracy > 0.01), 'min_Rc'] = 1
vlart_normal_df.loc[(vlart_normal_df.Run_Quantity < 50) | (vlart_normal_df.accuracy > 0.01), 'min_Rs'] = 1

vlart_df = pd.concat([vlart_foils_df, vlart_normal_df])

## Fix foils
vlart_df = vlart_df[['PRODUCT_ID', 'Finish', 'GAME', 'CARD_NAME', 'SET_NAME', 'min_Rc', 'min_Rs']]

vlart_df.rename(columns = {'PRODUCT_ID': 'ProductId', 'GAME': 'Game', 'CARD_NAME': 'Name', 'SET_NAME': 'Set', 'min_Rc': 'Min Rc', 'min_Rs': 'Min Rs'}, inplace = True)

vlart_df = vlart_df.sort_values(by=['ProductId'], ascending=True)

with open('NTV.csv', 'w'):
    pass

vlart_df.to_csv(r'G:\.shortcut-targets-by-id\1aBQd_LpInOAd54lGMUuJFO7Sas4rc0Vd\TCG NTV List\NTV.csv', index=False)

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