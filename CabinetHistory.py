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

##Import direct inventory history
sql_sku_hist = ("""
with
    inventory_staging  as (
      select
          di_hist.product_condition_id as pcid
          , di_hist.valid_from_et as valid_from
          , di_hist.valid_to_et as valid_to
          , di_hist.quantity_available as qty_available

      from
          analytics.core.direct_inventory_history as di_hist

      where
          (valid_to is null) or (valid_to::date <= cast(dateadd(dd, -100, getdate()) as date))
    )

    , sku_history_staging as (
        select
          sku_hist.product_condition_id as pcid
          , sku_hist.location_name as cabinet
          , sku_hist.effective_from as effective_from
          , sku_hist.effective_to as effective_to
          , sku_hist.is_active as is_active

        from
            analytics.core.fulfillment_center_skus_location_history as sku_hist

        where
            (effective_to is null) or (effective_to::date <= cast(dateadd(dd, -100, getdate()) as date))
    )

select
    inventory_staging.*
    , sku_history_staging.cabinet
    //, sku_history_staging.effective_from
    //, sku_history_staging.effective_to
from
    inventory_staging
        left outer join sku_history_staging on inventory_staging.pcid = sku_history_staging.pcid

where
    inventory_staging.qty_available > 0
    and inventory_staging.valid_from >= sku_history_staging.effective_from
    and (inventory_staging.valid_to <= sku_history_staging.effective_to) or (valid_to is null)
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_sku_hist)

sku_hist_df = cursor.fetch_pandas_all()
sku_hist_df.drop(sku_hist_df.filter(like='Unnamed'), axis=1, inplace=True)
sku_hist_df.dropna(subset = ['PCID'], inplace=True)
sku_hist_df.rename(columns={'CABINET':'Cabinet'}, inplace=True)

##Import RI Data
sql_ri_products = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoice_products.cabinet as cabinet
    , reimbursement_invoice_products.quantity_stocked as quantity_stocked
    , reimbursement_invoice_products.product_condition_id as pcid

from
analytics.core.reimbursement_invoices
inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
    reimbursement_invoices.is_auto = false
    and reimbursement_invoices.processing_ended_at_et::date >= dateadd(dd, -110, getdate())::date
    and reimbursement_invoices.processing_ended_at_et is not null
    and quantity_stocked is not null
    and reimbursement_invoices.seller_name <> 'mtg rares'
    and cabinet is not null

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri_products)

ri_prod_df = cursor.fetch_pandas_all()
ri_prod_df.drop(ri_prod_df.filter(like='Unnamed'), axis=1, inplace=True)
ri_prod_df.dropna(subset = ['RI_NUMBER'], inplace=True)

ri_prod_df["cab_len"] = ri_prod_df['CABINET'].str.len()

ri_prod_df = ri_prod_df.loc[ri_prod_df['cab_len'].astype('float64') > 0]

##Import time study data
file_ts = gc.open_by_key('1ZCBTdfSlfJRr0iRmDErxiRMUfs8nJuzpDTfzg5aZBxc').worksheet('Parse')
file_ts_df = pd.DataFrame.from_dict(file_ts.get_all_records())
file_ts_df.dropna(subset=['Date of Run'], inplace=True)

file_ts_df = file_ts_df[['Timestamp', 'Start Time', 'End Time', 'Email Address', 'Full Cabinet', 'Run', 'Time Elapsed', 'Exclude', 'In test and sorted', 'In test and unsorted', 'Sort Def']]

##Parse down timestudy data to the last 90 days
file_ts_df['Timestamp'] = pd.to_datetime(file_ts_df['Timestamp'], format='mixed', dayfirst=False).dt.date

file_ts_df["now"] = pd.Timestamp.now()
file_ts_df['now'] = file_ts_df['now'].dt.date

file_ts_df = file_ts_df.loc[file_ts_df['Timestamp'] >= (file_ts_df['now'] - timedelta(days = 90))]

##Import Run Data
run_gen = gc.open_by_key('1mhdpT207rfUi505J33wAn3m0L_AXuwH28PBzJ22Lp-s').worksheet('Archive')
run_gen_df = pd.DataFrame.from_dict(run_gen.get_all_records())
run_gen_df.dropna(subset=['Run'], inplace=True)

##Join product data to run data
run_gen_df = pd.merge(run_gen_df, ri_prod_df, left_on='RI', right_on='RI_NUMBER')
run_gen_df.drop(['RI'], axis=1, inplace=True)

##Sum quantities per run / cabinet combo
run_gen_df["count"] = run_gen_df['Run'].astype(str) + run_gen_df['CABINET'].astype(str)

cards_filed_per_cabinet = run_gen_df.groupby('count')['QUANTITY_STOCKED'].sum()
run_gen_df = pd.merge(run_gen_df, cards_filed_per_cabinet, how='right', on='count')

unique_cards_per_cabinet = run_gen_df.groupby('count')['PCID'].nunique()
run_gen_df = pd.merge(run_gen_df, unique_cards_per_cabinet, how='right', on='count')

run_gen_df.rename(columns={'QUANTITY_STOCKED_x':'qty_stocked_per_pcid_per_ri', 'QUANTITY_STOCKED_y':'qty_stocked_per_cabinet', 'PCID_y':'unique_cards_filed_per_cabinet', 'PCID_x':'pcid_filed'}, inplace=True)

run_gen_df = run_gen_df[['Run', 'CABINET', 'pcid_filed', 'qty_stocked_per_cabinet', 'unique_cards_filed_per_cabinet']]

##Calculate density per cabinet
run_gen_df["density_filed"] = run_gen_df['qty_stocked_per_cabinet'].astype('float64') / run_gen_df['unique_cards_filed_per_cabinet'].astype('float64')

##Join to time study data
file_ts_df["to_join"] = file_ts_df['Run'].astype(str) + file_ts_df['Full Cabinet'].astype(str)
run_gen_df["to_join"] = run_gen_df['Run'].astype(str) + run_gen_df['CABINET'].astype(str)

file_ts_df = pd.merge(file_ts_df, run_gen_df, how='left', on='to_join')

file_ts_df.dropna(subset=['Run_y'], inplace=True)
file_ts_df.rename(columns={'Run_y':'Run'}, inplace=True)

file_ts_df.drop(['Run_x', 'Full Cabinet', 'now', 'to_join', 'Email Address'], axis=1, inplace=True)

##Fix start/End Timestamps
file_ts_df['Start Time'] = file_ts_df['Timestamp'].astype(str) + ' ' + file_ts_df['Start Time'].astype(str)
file_ts_df['End Time'] = file_ts_df['Timestamp'].astype(str) + ' ' + file_ts_df['End Time'].astype(str)

file_ts_df['Start Time'] = pd.to_datetime(file_ts_df['Start Time'], format='mixed')
file_ts_df['End Time'] = pd.to_datetime(file_ts_df['End Time'], format='mixed')

##Parse down dataframe
file_ts_df["dupe"] = file_ts_df['Run'].astype(str) + file_ts_df['CABINET'].astype(str)
file_ts_df.drop_duplicates(subset=['dupe'], inplace=True)
file_ts_df.rename(columns={'CABINET':'Cabinet'}, inplace=True)

##Join sku history data to run data
file_ts_df = pd.merge(file_ts_df, sku_hist_df, how='left', on='Cabinet')
file_ts_df["now"] = pd.Timestamp.now()

file_ts_df['VALID_FROM'] = pd.to_datetime(file_ts_df['VALID_FROM'])
file_ts_df['VALID_TO'] = pd.to_datetime(file_ts_df['VALID_TO'])

file_ts_df.loc[pd.isnull(file_ts_df['VALID_TO']) == True, 'VALID_TO'] = file_ts_df['now']

file_ts_df = file_ts_df.loc[(file_ts_df['VALID_FROM'] <= file_ts_df['Start Time']) & (file_ts_df['VALID_TO'] >= file_ts_df['End Time'])]

##Aggragate cabinet quantities after removing 0 quantities
file_ts_df["count"] = file_ts_df['Run'].astype(str) + file_ts_df['Cabinet'].astype(str)

tags_per_cab = file_ts_df.groupby('count')['PCID'].nunique()
file_ts_df = pd.merge(file_ts_df, tags_per_cab, how='right', on='count')

file_ts_df.rename(columns={'PCID_x':'PCID', 'PCID_y':'num_tags_per_cab'}, inplace=True)

file_ts_df = file_ts_df.loc[file_ts_df['QTY_AVAILABLE'].astype('float64') > 0]

qty_cards_in_cab = file_ts_df.groupby('count')['QTY_AVAILABLE'].sum()
file_ts_df = pd.merge(file_ts_df, qty_cards_in_cab, how='right', on='count')

qty_pcids_in_cab = file_ts_df.groupby('count')['PCID'].nunique()
file_ts_df = pd.merge(file_ts_df, qty_pcids_in_cab, how='right', on='count')

file_ts_df.rename(columns={'QTY_AVAILABLE_x':'qty_avail_per_pcid_in_cab', 'QTY_AVAILABLE_y':'total_qty_in_cab', 'PCID_x':'PCID', 'PCID_y':'num_pcids_in_cab'}, inplace=True)

##Calculate density in cabinet
file_ts_df["density_in_cab"] = file_ts_df['total_qty_in_cab'].astype('float64') / file_ts_df['num_pcids_in_cab'].astype('float64')

##Reduce dataframe
file_ts_df.drop_duplicates(subset=['count'], inplace=True)

file_ts_df = file_ts_df.loc[file_ts_df['Exclude'] == 'FALSE']

file_ts_df = file_ts_df[['Time Elapsed', 'In test and sorted', 'In test and unsorted', 'Run', 'Cabinet', 'qty_stocked_per_cabinet', 'unique_cards_filed_per_cabinet', 'density_filed', 'num_tags_per_cab', 'num_pcids_in_cab', 'density_in_cab', 'Sort Def']]

file_ts_df["Condition"] = file_ts_df['Cabinet'].str.split('-').str[1]





test = gc.open_by_key('1ZCBTdfSlfJRr0iRmDErxiRMUfs8nJuzpDTfzg5aZBxc')
testTab = test.worksheet('Python')
testTab.batch_clear(['A1:M'])
gd.set_with_dataframe(testTab, file_ts_df)











































