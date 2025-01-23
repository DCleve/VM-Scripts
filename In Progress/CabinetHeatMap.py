import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import datetime as dt
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

##Import pulling data
sql_pull = ("""
select
    ppbp.location_name as cabinet
    , ppbp.location_pulled_at_et::date as pulled_date
    , ppbp.number_of_cards as total_cards_pulled_per_location
    , ppbp.number_of_skus as total_skus_per_location
    , ppbp.pull_duration_seconds as total_pull_time_seconds


from analytics.core.paperless_pulling_by_pull as ppbp

where
    ppbp.location_pulled_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_pull)

pull_df = cursor.fetch_pandas_all()
pull_df.drop(pull_df.filter(like='Unnamed'), axis=1, inplace=True)
pull_df.dropna(subset = ['CABINET'], inplace=True)

##Aggragate numbers
pull_df["combined"] = pull_df['CABINET'].astype(str) + pull_df['PULLED_DATE'].astype(str)

cards_per_location = pull_df.groupby('combined')['TOTAL_CARDS_PULLED_PER_LOCATION'].sum()
pull_df = pd.merge(pull_df, cards_per_location, how='right', on='combined')

skus_per_location = pull_df.groupby('combined')['TOTAL_SKUS_PER_LOCATION'].sum()
pull_df = pd.merge(pull_df, skus_per_location, how='right', on='combined')

time_per_location = pull_df.groupby('combined')['TOTAL_PULL_TIME_SECONDS'].sum()
pull_df = pd.merge(pull_df, time_per_location, how='right', on='combined')

pull_df.drop_duplicates(subset='combined', inplace=True)


pull_df.rename(columns={'TOTAL_CARDS_PULLED_PER_LOCATION_y':'TOTAL_CARDS_PULLED_PER_LOCATION', 'TOTAL_SKUS_PER_LOCATION_y':'TOTAL_SKUS_PER_LOCATION', 'TOTAL_PULL_TIME_SECONDS_y':'TOTAL_PULL_TIME_SECONDS'}, inplace=True)

pull_df.drop(['TOTAL_CARDS_PULLED_PER_LOCATION_x', 'TOTAL_SKUS_PER_LOCATION_x', 'TOTAL_PULL_TIME_SECONDS_x', 'combined'], axis=1,  inplace=True)

##Import RI Shelved data
sql_file = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as "ri_number"
    , reimbursement_invoices.shelved_at_et::date as "shelved_date"
    , reimbursement_invoice_products.quantity_stocked as "quantity_stocked"
    , reimbursement_invoice_products.product_condition_id as "pcid"
    , reimbursement_invoice_products.product_name as "card_name"
    , reimbursement_invoice_products.set_name as "set_name"
    , reimbursement_invoice_products.condition as "condition_name"
    , reimbursement_invoice_products.cabinet as "cabinet"

from
analytics.core.reimbursement_invoices
inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
    "shelved_date" >= cast(dateadd(dd, -90, getdate()) as date)
    and "quantity_stocked" is not null

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_file)

file_df = cursor.fetch_pandas_all()
file_df.drop(file_df.filter(like='Unnamed'), axis=1, inplace=True)
file_df.dropna(subset = ['ri_number'], inplace=True)

##Aggragate cards filed per cabinet per day
file_df["combined"] = file_df['cabinet'].astype(str) + file_df['shelved_date'].astype(str)

cards_per_day = file_df.groupby('combined')['quantity_stocked'].sum()
file_df = pd.merge(file_df, cards_per_day, how='right', on='combined')
file_df.rename(columns={'quantity_stocked_x':'quantity_stocked', 'quantity_stocked_y':'cards_filed_per_cab_per_day'}, inplace=True)

pcids_per_day = file_df.groupby('combined')['pcid'].nunique()
file_df = pd.merge(file_df, pcids_per_day, how='right', on='combined')
file_df.rename(columns={'pcid_x':'pcid', 'pcid_y':'pcids_filed_per_cab_per_day'}, inplace=True)

file_df.drop_duplicates(subset='combined', inplace=True)

file_df = file_df[['shelved_date', 'cabinet', 'cards_filed_per_cab_per_day', 'pcids_filed_per_cab_per_day']]

##Write data to sheet
file_tab = gc.open_by_key('1MPwAf4465wcmIhRl7u8JcWSbpNfJWfkg5sy0KXfJO7Q').worksheet('FileData')
file_tab.batch_clear(['A1:D'])
gd.set_with_dataframe(file_tab, file_df, row=1, col=1)

pull_tab = gc.open_by_key('1MPwAf4465wcmIhRl7u8JcWSbpNfJWfkg5sy0KXfJO7Q').worksheet('PullData')
pull_tab.batch_clear(['A1:E'])
gd.set_with_dataframe(pull_tab, pull_df, row=1, col=1)