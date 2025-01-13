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

##Define document
efficFactorsDoc = gc.open_by_key('1dWEG8-WpJwRDUpQqIsJAqidj3bAv8sJZUoiSTWn8pwA')

nuwayDataTab = efficFactorsDoc.worksheet('NuWayData')
nuwayDataTab.batch_clear(['A3:A'])

recDataTab = efficFactorsDoc.worksheet('RecData')
recDataTab.clear()

recAvlTab = efficFactorsDoc.worksheet('RecAvl')
recAvlTab.clear()

shpAvlTab = efficFactorsDoc.worksheet('ShpAvl')
shpAvlTab.clear()

time.sleep(15)

###Import NuWay Data
nuwayDataTab_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "TestEnvData.csv"]
nuwayDataTab_result = separator.join(nuwayDataTab_string)
nuwayDataTab_df = pd.read_csv(nuwayDataTab_result)

gd.set_with_dataframe(nuwayDataTab, nuwayDataTab_df, row=3, col=1)

##Import Receiving Data
rec_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Rec.csv"]
rec_result = separator.join(rec_string)
rec_df = pd.read_csv(rec_result)

##Fix data types
rec_df['PROC_MISS_COUNT_ENTERED'] = rec_df['PROC_MISS_COUNT_ENTERED'].astype('float64')
rec_df['PROC_COND_COUNT_ENTERED'] = rec_df['PROC_COND_COUNT_ENTERED'].astype('float64')
rec_df['PROC_EXTRA_COUNT_ENTERED'] = rec_df['PROC_EXTRA_COUNT_ENTERED'].astype('float64')
rec_df['VER_MISS_COUNT_ENTERED'] = rec_df['VER_MISS_COUNT_ENTERED'].astype('float64')
rec_df['VER_COND_COUNT_ENTERED'] = rec_df['VER_COND_COUNT_ENTERED'].astype('float64')
rec_df['VER_EXTRA_COUNT_ENTERED'] = rec_df['VER_EXTRA_COUNT_ENTERED'].astype('float64')
rec_df['sleeved_cards'] = rec_df['sleeved_cards'].astype('float64')
rec_df['cabinet_splits'] = rec_df['cabinet_splits'].astype('float64')

##Write data to sheet
gd.set_with_dataframe(recDataTab, rec_df)

##Rec available work
sql_rec_avl = ("""
select
    distinct reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoices.total_product_quantity as prod_qty
    , reimbursement_invoice_products.inspection_level as inspection_level
    , reimbursement_invoices.received_at_et as received_at
    , reimbursement_invoices.active_processing_started_at_et as proc_started_at
    , reimbursement_invoices.processing_ended_at_et as proc_ended_at
    , reimbursement_invoices.shelved_at_et as shelved_at


from
analytics.core.reimbursement_invoices
inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
reimbursement_invoices.received_at_et is not null
and reimbursement_invoices.is_auto = false
and reimbursement_invoices.seller_id != 249
and inspection_level is not null
and reimbursement_invoices.received_at_et::date >= cast(dateadd(dd, -120, getdate()) as date)
and reimbursement_invoices.was_marked_missing = false


order by
received_at desc
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_rec_avl)

rec_avl_df = cursor.fetch_pandas_all()

rec_avl_df.drop(rec_avl_df.filter(like='Unnamed'), axis=1, inplace=True)
rec_avl_df.dropna(subset=['RI_NUMBER'], inplace=True)

##Write to sheet
gd.set_with_dataframe(recAvlTab, rec_avl_df)

##Shipping available work
sql_shp_avl = ("""
with
    sq_staging as (
      select
          shippingqueue.shippingqueuenumber as sq_number
          , shippingqueue.ordercount as order_count
          , shippingqueue.productcount as product_count
          , shippingqueue.createdat as created_at
          , shippingqueue.updatedat as last_updated_at
          , shippingqueuestatus.name as sq_status

      from
      hvr_tcgstore_production.tcgd.shippingqueue
      inner join hvr_tcgstore_production.tcgd.shippingqueuestatus on shippingqueue.shippingqueuestatusid = shippingqueuestatus.shippingqueuestatusid


      where
          shippingqueue.createdat::date >= cast(dateadd(dd, -120, getdate()) as date)

      order by
          sq_number asc
     )

     , paperless_pull_staging as (
       select
        analytics.core.paperless_pulling_agg.shipping_queue_number as sq_number
        , min(analytics.core.paperless_pulling_agg.pulling_start) as pulling_start
        //, max(analytics.core.paperless_pulling_agg.pulling_end) as pulling_end

        from
        analytics.core.paperless_pulling_agg

        where
            analytics.core.paperless_pulling_agg.pulling_end::date >= cast(dateadd(dd, -120, getdate()) as date)
       group by 1
      )

select
    sq_staging.*
    , paperless_pull_staging.pulling_start
    , paperless_pull_staging.pulling_end

from sq_staging
    left outer join paperless_pull_staging on sq_staging.sq_number = paperless_pull_staging.sq_number

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_shp_avl)

shp_avl_df = cursor.fetch_pandas_all()

shp_avl_df.drop(shp_avl_df.filter(like='Unnamed'), axis=1, inplace=True)
shp_avl_df.dropna(subset=['SQ_NUMBER'], inplace=True)


shp_avl_df['PULLING_START'] = pd.to_datetime(shp_avl_df['PULLING_START'])
shp_avl_df["first_offset"] = shp_avl_df['PULLING_START'] - timedelta(hours = 12)

##Write to sheet
gd.set_with_dataframe(shpAvlTab, shp_avl_df)

##Turn document back on
cell_list = nuwayDataTab.range('A2')

for cell in cell_list:
    cell.value = "On"

nuwayDataTab.update_cells(cell_list)

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