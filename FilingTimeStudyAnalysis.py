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

##Import Run Data
run_gen = gc.open_by_key('1mhdpT207rfUi505J33wAn3m0L_AXuwH28PBzJ22Lp-s').worksheet('Archive')
run_gen_df = pd.DataFrame.from_dict(run_gen.get_all_records())
run_gen_df.dropna(subset=['Run'], inplace=True)

##Parse down run data
run_gen_df["Date"] = run_gen_df['Run'].str.split('-').str[0]

run_gen_df = run_gen_df.loc[run_gen_df['Date'].astype('float64') >= 240608]

##Import RI Data
sql_ri_products = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoice_products.cabinet as cabinet
    , reimbursement_invoice_products.quantity_stocked as quantity_stocked
    , reimbursement_invoices.total_product_quantity as total_cards
    , reimbursement_invoice_products.product_name as card_name

from
analytics.core.reimbursement_invoices
inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
    reimbursement_invoices.is_auto = false
    and reimbursement_invoices.processing_ended_at_et::date >= dateadd(dd, -90, getdate())::date
    and reimbursement_invoices.processing_ended_at_et is not null
    and quantity_stocked is not null

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri_products)

ri_prod_df = cursor.fetch_pandas_all()
ri_prod_df.drop(ri_prod_df.filter(like='Unnamed'), axis=1, inplace=True)
ri_prod_df.dropna(subset = ['RI_NUMBER'], inplace=True)

##Join product data to run data
run_gen_df = pd.merge(run_gen_df, ri_prod_df, left_on='RI', right_on='RI_NUMBER')

##Sum quantities per run / cabinet combo
run_gen_df["count"] = run_gen_df['Run'].astype(str) + run_gen_df['CABINET'].astype(str)

cards_filed_per_cabinet = run_gen_df.groupby('count')['QUANTITY_STOCKED'].sum()
run_gen_df = pd.merge(run_gen_df, cards_filed_per_cabinet, how='right', on='count')

unique_cards_per_cabinet = run_gen_df.groupby('count')['CARD_NAME'].nunique()
run_gen_df = pd.merge(run_gen_df, unique_cards_per_cabinet, how='right', on='count')

run_gen_df.rename(columns={'QUANTITY_STOCKED_x':'qty_stocked_per_pcid_per_ri', 'QUANTITY_STOCKED_y':'qty_stocked_per_cabinet', 'CARD_NAME_y':'unique_cards_per_cabinet'}, inplace=True)

##Calculate density per cabinet
run_gen_df["Density"] = run_gen_df['qty_stocked_per_cabinet'].astype('float64') / run_gen_df['unique_cards_per_cabinet'].astype('float64')

##Parse down dataframe
run_gen_df.drop_duplicates(subset=['count'], inplace=True)

run_gen_df = run_gen_df[['Run', 'CABINET', 'qty_stocked_per_cabinet', 'Density']]

run_gen_df.rename(columns={'CABINET':'Cabinet', 'qty_stocked_per_cabinet':'Cards'}, inplace=True)

##Write data to sheet
filing_tab = gc.open_by_key('1ZCBTdfSlfJRr0iRmDErxiRMUfs8nJuzpDTfzg5aZBxc').worksheet('CardData')
filing_tab.clear()
gd.set_with_dataframe(filing_tab, run_gen_df)