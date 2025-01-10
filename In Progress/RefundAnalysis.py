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

##Import SQl Data
sql = ("""
        select
            seller_orders.order_number as seller_order_number
            , order_items.quantity as product_quantity
            , order_items.total_usd as product_sold_amount
            , seller_orders.product_amount_usd as total_order_price
            , reimbursement_invoices.reimbursement_invoice_number as ri_number
            , seller_orders.seller_paid_at_et as seller_paid_at
            , seller_orders.seller_order_status as order_status
            , reimbursement_invoices.created_at_et as ri_created_date
			, refunds.refund_note as refund_note

            , case when
                contains(refunds.refund_note, 'shipment')
                or contains(refunds.refund_note, 'condition')
                or contains(refunds.refund_note, 'arrive')
                or contains(refunds.refund_note, 'eturn')
                or contains(refunds.refund_note, 'shipped')
                or contains(refunds.refund_note, 'eceiv')
                or contains(refunds.refund_note, 'eciev')
                or contains(refunds.refund_note, 'incorrect card')
                or contains(refunds.refund_note, 'issing card')
                or contains(refunds.refund_note, 'issing order')
                or contains(refunds.refund_note, 'stolen')
                or contains(refunds.refund_note, 'ndeliver')
                or contains(refunds.refund_note, 'postage')
                or contains(refunds.refund_note, 'reship')
                or contains(refunds.refund_note, 'damaged')
                or contains(refunds.refund_note, 'sent')
                or contains(refunds.refund_note, 'difference')
                then true else false end as package_left_ac

            , sellers.entity_name as seller_name
            , buyers.email_address as buyer_email_address
            , seller_orders.ordered_at_et::date as ordered_at_date
            , direct_orders.direct_order_number as direct_order_number



        from analytics.core.orders
        inner join analytics.core.seller_orders on orders.id = seller_orders.order_id
        inner join analytics.core.order_items on seller_orders.id = order_items.seller_order_id
        inner join analytics.core.sellers on seller_orders.seller_id = sellers.id
        inner join analytics.core.buyers on seller_orders.buyer_id = buyers.id
        inner join analytics.core.direct_orders on seller_orders.order_id = direct_orders.order_id
        left outer join hvr_tcgstore_production.tcgd.reimordersellerorderproduct on
            order_items.id = reimordersellerorderproduct.SellerOrderProductId
            and reimordersellerorderproduct.hvr_deleted = 0
        left outer join analytics.core.reimbursement_invoices on reimordersellerorderproduct.reimorderid = reimbursement_invoices.id
		left outer join analytics.core.refunds on refunds.seller_order_id = seller_orders.id
        //left outer join analytics.core.refunds on refunds.order_id = orders.id


        where
            orders.status = 'Complete'
            --and seller_orders.seller_order_status = 'Complete'
            and seller_orders.is_direct_order = true
            and seller_orders.seller_id != 249
            and sellers.is_store_your_products = false
            //and len(ri_number) = 26
            and refund_note is not null
            and reimbursement_invoices.created_at_et::date >= '2024-1-1'
            //and reimbursement_invoices.created_at_et::date >= '2023-1-1'
            //and reimbursement_invoices.created_at_et::date <= '2023-12-31'

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql)

sql_df = cursor.fetch_pandas_all()

sql_df.drop(sql_df.filter(like='Unnamed'), axis=1, inplace=True)
sql_df.dropna(subset=['SELLER_ORDER_NUMBER'], inplace=True)

##Write to sheet
dataTab = gc.open_by_key('1cAGwJPcXDZm1P1gWrERA3_6Er0C3SWJF9y5Yc4ZuLoU').worksheet('Data')
dataTab.batch_clear(['A1:O'])
gd.set_with_dataframe(dataTab, sql_df, row=1, col=1)



































