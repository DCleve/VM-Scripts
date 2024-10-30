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

filingAuditDoc = gc.open_by_key('10AGK6oHv6TIIwVtH7wv8G7DGs74i4XGjWD_VhDnvaDc')

import snowflake.connector
from snowflake.connector import connect
snowflake_pull = connect(user='Dave', password='Quantum314!', account='fva14998.us-east-1')

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
nuway_df = pd.DataFrame.from_dict(nuway_tab.get_all_records())
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)
nuway_df = nuway_df[['Data']]

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]

nuway_df = nuway_df.loc[nuway_df['Task'] == 'Filing']

nuway_df = nuway_df[['Punch', 'Puncher',  'SQ/POQ', 'Location/Cards']]

##Merge Staffing Data to nuway data
nuway_df['Puncher'] = nuway_df['Puncher'].str.lower()
nuway_df = pd.merge(nuway_df, staffing_df, how='left')

nuway_df.drop('Puncher', axis=1, inplace=True)
nuway_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

nuway_df = nuway_df[['Punch', 'Location/Cards', 'Puncher',  'Shift Name']]

##Write data to sheet
dataTab = filingAuditDoc.worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, nuway_df)

##Import RI Data
sql_rec = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoices.created_at_et::date as created_at_date
    , reimbursement_invoices.received_at_et::date as received_at_date
    , reimbursement_invoices.processing_ended_at_et::date as processed_date
    , reimbursement_invoices.verification_ended_at_et::date as verified_date
    , reimbursement_invoices.shelved_at_et::date as shelved_date
    , reimbursement_invoices.total_product_quantity as total_ri_qty

from
analytics.core.reimbursement_invoices

where
created_at_date >= cast(dateadd(dd, -120, getdate()) as date)
and reimbursement_invoices.is_auto = false
and reimbursement_invoices.seller_name <> 'mtg rares'
and reimbursement_invoices.received_at_et is not null

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_rec)
rec_df = cursor.fetch_pandas_all()
rec_df.drop(rec_df.filter(like='Unnamed'), axis=1, inplace=True)
rec_df.dropna(subset = ["RI_NUMBER"], inplace=True)

rec_df['TOTAL_RI_QTY'] = rec_df['TOTAL_RI_QTY'].astype('float64')

##Aggragate RI products Generated
num_prod_gen = rec_df.groupby('CREATED_AT_DATE')['TOTAL_RI_QTY'].sum()
rec_df = pd.merge(rec_df, num_prod_gen, how='left', on='CREATED_AT_DATE')

rec_df.rename(columns={'TOTAL_RI_QTY_x':'TOTAL_RI_QTY', 'TOTAL_RI_QTY_y': 'RI: Generated (Pdt Ct)'}, inplace=True)

##Aggragate RI products/count received
num_ri_rec = rec_df.groupby('RECEIVED_AT_DATE')['RI_NUMBER'].nunique()
rec_df = pd.merge(rec_df, num_ri_rec, how='left', on='RECEIVED_AT_DATE')

num_prod_rec = rec_df.groupby('RECEIVED_AT_DATE')['TOTAL_RI_QTY'].sum()
rec_df = pd.merge(rec_df, num_prod_rec, how='left', on='RECEIVED_AT_DATE')

rec_df.rename(columns={'RI_NUMBER_x':'RI_NUMBER', 'TOTAL_RI_QTY_x': 'TOTAL_RI_QTY', 'RI_NUMBER_y':'RI: Received (RI Ct)', 'TOTAL_RI_QTY_y':'RI: Received (Pdt Ct)'}, inplace=True)

##Aggragate RI product count processed
num_prod_proc = rec_df.groupby('PROCESSED_DATE')['TOTAL_RI_QTY'].sum()
rec_df = pd.merge(rec_df, num_prod_proc, how='left', on='PROCESSED_DATE')

rec_df.rename(columns={'TOTAL_RI_QTY_x': 'TOTAL_RI_QTY', 'TOTAL_RI_QTY_y':'RI: Processed (Pdt Ct)'}, inplace=True)

##Aggragate RI product count verified
num_prod_ver = rec_df.groupby('VERIFIED_DATE')['TOTAL_RI_QTY'].sum()
rec_df = pd.merge(rec_df, num_prod_ver, how='left', on='VERIFIED_DATE')

rec_df.rename(columns={'TOTAL_RI_QTY_x': 'TOTAL_RI_QTY', 'TOTAL_RI_QTY_y':'RI: Verified (Pdt Ct)'}, inplace=True)

##Aggragate RIs products/count shelved
num_ri_shel = rec_df.groupby('SHELVED_DATE')['RI_NUMBER'].nunique()
rec_df = pd.merge(rec_df, num_ri_shel, how='left', on='SHELVED_DATE')

num_prod_shel = rec_df.groupby('SHELVED_DATE')['TOTAL_RI_QTY'].sum()
rec_df = pd.merge(rec_df, num_prod_shel, how='left', on='SHELVED_DATE')

rec_df.rename(columns={'RI_NUMBER_x':'RI_NUMBER', 'TOTAL_RI_QTY_x': 'TOTAL_RI_QTY', 'TOTAL_RI_QTY_y':'RI: Shelved (Pdt Ct)', 'RI_NUMBER_y':'RI: Shelved (RI Ct)'}, inplace=True)

##Make final dataframe and write data to sheet
rec_df = rec_df[['SHELVED_DATE', 'RI: Generated (Pdt Ct)', 'RI: Received (RI Ct)', 'RI: Received (Pdt Ct)', 'RI: Processed (Pdt Ct)', 'RI: Verified (Pdt Ct)', 'RI: Shelved (RI Ct)', 'RI: Shelved (Pdt Ct)']]

rec_df.drop_duplicates(subset=['SHELVED_DATE'], inplace=True)
rec_df.dropna(subset=['SHELVED_DATE'], inplace=True)

rec_df.sort_values(by=['SHELVED_DATE'], ascending=[False], inplace=True)

tableauTab = filingAuditDoc.worksheet('Tableau')
tableauTab.clear()
gd.set_with_dataframe(tableauTab, rec_df)

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