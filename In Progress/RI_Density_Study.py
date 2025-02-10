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

##Pull density information
density_sql = ("""
select
    distinct reimbursement_invoices.reimbursement_invoice_number as "ri_number"
    , reimbursement_invoices.created_at_et::date as "ri_created_at_date"
    , sum(reimbursement_invoice_products.expected_quantity) as "expected_qty"
    , count(distinct concat(reimbursement_invoice_products.product_name, '~', reimbursement_invoice_products.set_name, '~', reimbursement_invoice_products.condition)) as "count_pcids"
    , "expected_qty" / "count_pcids" as "density"

from analytics.core.reimbursement_invoices
    inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
    reimbursement_invoices.created_at_et::date >= cast(dateadd(dd, -365, getdate()) as date)

group by 1, 2

""")

cursor = snowflake_pull.cursor()
cursor.execute(density_sql)

density_df = cursor.fetch_pandas_all()
density_df.drop(density_df.filter(like='Unnamed'), axis=1, inplace=True)
density_df.dropna(subset = ['ri_number'], inplace=True)

##Write data to sheet
dataTab = gc.open_by_key('1lfntRVfU2pb23R_zXL8g6eZligQr106cebJkVZIuHUA').worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, density_df)