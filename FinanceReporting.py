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
with
    ri_staging as (
        select
            seller_orders.order_number as seller_order_number
            , seller_orders.seller_paid_date
            , seller_orders.seller_order_status
            , order_items.quantity
            , order_items.total_usd
            , reimbursement_invoices.reimbursement_invoice_number
            , reimbursement_invoices.created_at_et
            , reimbursement_invoices.received_at_et
            , reimbursement_invoices.processing_ended_at_et
            , reimbursement_invoices.shelved_at_et
            , seller_orders.seller_id
            , seller_orders.ordered_at_et

            , case
                when reimbursement_invoices.reimbursement_invoice_number like '%STD%' then 'Standard'
                when reimbursement_invoices.reimbursement_invoice_number like '%PRF%' then 'Preferred'
              end as ri_type
            , case
                when seller_orders.seller_paid_date <= reimbursement_invoices.created_at_et then 'Before RI Creation'
                when seller_orders.seller_paid_date <= reimbursement_invoices.received_at_et then 'Before RI Receipt'
                when seller_orders.seller_paid_date <= reimbursement_invoices.processing_ended_at_et then 'Before RI Processing'
                when seller_orders.seller_paid_date is not null then 'After RI Finalization'
                when seller_orders.seller_paid_date is null then 'Payment to be Scheduled'
                else null
              end as when_was_seller_paid
            , case
                when refunds.refund_note ilike any('%shipment%', '%condition%', '%arrive%', '%returned%', '%shipped%') then true
                when refunds.id is null then null
                else false
              end as package_left_ac

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
    )

    , created_dates_per_so as (
        select
            ri_staging.seller_order_number
            , count(distinct ri_staging.created_at_et) as number_of_created_at_dates

        from ri_staging

        group by 1
    )

select
    ri_staging.seller_order_number as "Seller Order Number"
    , ri_staging.seller_paid_date::date as "Seller Paid Date"
    , ri_staging.seller_order_status as "Seller Order Status"
    , ri_staging.quantity as "Sold Product Quantity"
    , ri_staging.total_usd as "Sold Product Amount USD"
    //, ri_staging.reimbursement_invoice_number as "RI Number"
    , ri_staging.created_at_et::date as "RI Created Date"
    , ri_staging.received_at_et::date as "RI Received Date"
    , ri_staging.processing_ended_at_et::date as "RI Processing Date"
    , ri_staging.shelved_at_et::date as "RI Shelved Date"
    , ri_staging.ri_type as "RI Type"
    , ri_staging.when_was_seller_paid as "When Was Seller Paid?"
    , ri_staging.package_left_ac as "Package Left the AC?"
    , ri_staging.seller_id as "Seller ID"
    , ri_staging.ordered_at_et::date as "Ordered Date"
    //, created_dates_per_so.number_of_created_at_dates as "Timestamps per Seller Order"

from ri_staging
inner join created_dates_per_so on ri_staging.seller_order_number = created_dates_per_so.seller_order_number
where created_dates_per_so.number_of_created_at_dates > 1

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri)

ri_df = cursor.fetch_pandas_all()
ri_df.drop(ri_df.filter(like='Unnamed'), axis=1, inplace=True)
#ri_df.dropna(subset = ['SELLER ORDER NUMBER'], inplace=True)

ri_df = ri_df [['Seller Order Number', 'Seller ID', 'Sold Product Quantity', 'Sold Product Amount USD', 'Ordered Date', 'Seller Paid Date', 'RI Created Date', 'RI Received Date', 'RI Processing Date', 'RI Shelved Date', 'RI Type', 'When Was Seller Paid?', 'Seller Order Status', 'Package Left the AC?']]

##Create data csv
rec_norm_string = ["C:", "Users", login, "Desktop", "Rec.csv"]
rec_norm_result = separator.join(rec_norm_string)
ri_df.to_csv(rec_norm_result, index=False)




























