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

##Import RI Data
sql_ri = ("""
select
    seller_orders.seller_paid_date::date as "Seller Paid Date"
    , order_items.quantity as "Sold Product Quantity"
    , order_items.total_usd as "Sold Product Amount USD"
    , case
        when seller_orders.seller_paid_date <= reimbursement_invoices.created_at_et then 'Before RI Creation'
        when seller_orders.seller_paid_date < reimbursement_invoices.received_at_et then 'Before RI Receipt'
        when seller_orders.seller_paid_date < reimbursement_invoices.processing_ended_at_et then 'Before RI Processing'
        when seller_orders.seller_paid_date is not null then 'After RI Finalization'
        when seller_orders.seller_paid_date is null then 'Payment to be Scheduled'
        else null
      end as "When Was Seller Paid?"

from analytics.core.orders
inner join analytics.core.seller_orders on orders.id = seller_orders.order_id
inner join analytics.core.sellers on seller_orders.seller_id = sellers.id
left outer join analytics.core.refunds on seller_orders.id = refunds.seller_order_id
inner join analytics.core.order_items on seller_orders.id = order_items.seller_order_id
left outer join hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct as rosop on
    order_items.id = rosop.SellerOrderProductId
    and rosop.hvr_deleted = 0
left outer join analytics.core.reimbursement_invoices on rosop.ReimOrderId = reimbursement_invoices.id

where
    orders.status = 'Complete'
    and seller_orders.is_direct_order = true
    and seller_orders.seller_id != 249
    and sellers.is_store_your_products = false
    and len(reimbursement_invoices.reimbursement_invoice_number) = 26
    and split_part(reimbursement_invoices.reimbursement_invoice_number, '-', 3) in('PRF', 'STD')

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri)

ri_df = cursor.fetch_pandas_all()
ri_df.drop(ri_df.filter(like='Unnamed'), axis=1, inplace=True)

ri_df = ri_df[['Sold Product Amount USD', 'When Was Seller Paid?', 'Seller Paid Date', 'Sold Product Quantity']]

##Aggragate data
ri_df["combined"] = ri_df['Seller Paid Date'].astype(str) + ri_df['When Was Seller Paid?'].astype(str)

dollar_amount = ri_df.groupby('combined')['Sold Product Amount USD'].sum()
ri_df = pd.merge(ri_df, dollar_amount, how='right', on='combined')
ri_df.rename(columns={'Sold Product Amount USD_x':'Sold Product Amount USD', 'Sold Product Amount USD_y':'dollar_amount_per_day'}, inplace=True)

product_quantity = ri_df.groupby('combined')['Sold Product Quantity'].sum()
ri_df = pd.merge(ri_df, product_quantity, how='right', on='combined')
ri_df.rename(columns={'Sold Product Quantity_x':'Sold Product Quantity', 'Sold Product Quantity_y':'product_amount_per_day'}, inplace=True)

ri_df.drop_duplicates(subset='combined', inplace=True)

ri_df = ri_df[['Seller Paid Date', 'When Was Seller Paid?', 'dollar_amount_per_day', 'product_amount_per_day']]

##Write Data to sheet
finTab = gc.open_by_key('10uGqyQKL8Igby5hFa6fH9sfY4t_eqztK3qodLMJZrrU').worksheet('Data')
finTab.clear()
gd.set_with_dataframe(finTab, ri_df)